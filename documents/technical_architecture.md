# Technical Architecture: Scheduler Agent (Reactive Trigger Service)

The `agent_scheduler` is a **single-process, containerized microservice** that owns
time-based execution triggers for the `logus2k` ecosystem. It runs **FastAPI** and
**APScheduler** in the *same* process, backed by **Valkey** for restart-resilient job
persistence, and emits standard `EventEnvelope` messages onto the existing
`valkey-bus` so any consumer can react to a scheduled event.

It is a **pure trigger actor**: it contains no business logic. Its entire job is
"emit a well-formed `EventEnvelope` onto a stream at the right moment."

---

## 1. System Overview

| Concern | Choice |
| --- | --- |
| Scheduler engine | **APScheduler 3.x** (`AsyncIOScheduler`) |
| Job persistence | **`RedisJobStore`** on `valkey-bus` (redis-py client) |
| Event emission | Vendored glide publisher → `valkey-bus` streams |
| Admin interface | **FastAPI** REST API (CRUD over jobs) |
| Process model | **Single container** — FastAPI + scheduler share one event loop |
| Network | `logus2k_network` (external Docker bridge) |

### Why single-process

A persistent `RedisJobStore` is shared *state*, not a coordination channel.
APScheduler 3.x has **no leader election** — if two scheduler instances point at the
same job store, both fire every due job (duplicate emissions). Running FastAPI and the
`AsyncIOScheduler` in one process means:

* `add_job` / `remove_job` from an API route mutate the live in-memory scheduler
  immediately — no cross-process "poll the job store for changes" dance.
* Exactly one scheduler ever fires a job — no duplicates.
* The job store exists purely so the schedule survives a container restart.

If horizontal scaling is ever required, that is a migration to **APScheduler 4.x**
(which natively separates scheduler / data store / workers) — explicitly out of scope here.

---

## 2. Component Architecture

```
                 ┌──────────────────────── agent_scheduler container ───────────────────────┐
                 │                                                                           │
  HTTP (CRUD) ──▶│  FastAPI  ──▶  AsyncIOScheduler ──(on fire)──▶  emitter.emit()            │
                 │     │              │                                  │                    │
                 │     │              ▼                                  ▼                    │
                 │     │        RedisJobStore                    glide publisher              │
                 │     └──────────────┼──────────────────────────────┼─────────────────────┘
                 └────────────────────┼──────────────────────────────┼──────────────────────┘
                                      ▼                              ▼
                          valkey-bus: apscheduler.jobs     valkey-bus: stream:<target>
                          (pickled job state, persistence)  (EventEnvelope, the choreography)
```

* **FastAPI app** — owns the scheduler via lifespan (`scheduler.start()` on startup,
  `scheduler.shutdown()` on shutdown) and exposes it to routes through dependency injection.
* **`AsyncIOScheduler` + `RedisJobStore`** — persists all triggers in Valkey under a
  namespaced key (`agent_scheduler.jobs`). On restart the scheduler rehydrates from this
  store and resumes pending triggers with no user intervention.
* **`emitter.emit_scheduled_event(...)`** — the job callable. A module-level coroutine
  (stable import path → picklable by reference) that builds an `EventEnvelope` and
  publishes it via the glide publisher.
* **glide publisher (`bus_client.py`)** — a thin wrapper over `valkey-glide` exposing the
  minimal surface a producer needs: `publish` (XADD), `incr` (sid counter), `sadd`
  (active-streams registration). Uses the **same wire format** as `agent_bus`.

### Two clients, on purpose

* **redis-py** drives `RedisJobStore` (APScheduler only ships redis-py / SQLAlchemy stores).
* **valkey-glide** drives event emission, to stay byte-identical with `agent_bus`'s bus.

These are different concerns (job persistence vs. choreography) and never overlap in keyspace.

### Vendored contract

`envelope.py` is **copied verbatim** from `agent_bus` (the source of truth) so the
scheduler is a standalone service yet emits messages indistinguishable from any other
actor's. A header comment in the vendored file points back to the canonical version.
Only the additions a producer needs are layered on (e.g. a `schedule.fired` event type).

---

## 3. The Event It Writes

On every fire, the scheduler writes **one** standard `EventEnvelope` to
`stream:<resolved_target>` (one `XADD`, single field `data` = envelope JSON):

```json
{
  "header": {
    "stream_id": "nightly-report",
    "cid": "7f3a…uuid",            // fresh uuid4 per fire — each fire is a new workflow
    "sid": 1,                       // INCR sid:<cid> → first step
    "timestamp": "2026-06-28T02:00:00.041+00:00",
    "sender": "agent_scheduler",
    "event_type": "schedule.fired"  // caller-settable; defaults to schedule.fired
  },
  "payload": {
    "data": { "report": "daily_sales" },   // event_data, verbatim from JobCreate
    "context": {
      "job_id": "nightly-report",
      "trigger_type": "cron",
      "fired_at": "2026-06-28T02:00:00.041+00:00",
      "scheduled_run_time": "2026-06-28T02:00:00+00:00",  // when available
      "room": "ops-dashboard"        // only when a room hint was set
    }
  },
  "metadata": { "version": "1.0", "trace_parent": null }
}
```

* **Scheduler-generated:** `cid`, `sid`, `timestamp`, `sender`, `stream_id`, `metadata.version`.
* **Caller-controlled (fixed at registration):** `event_type`, `payload.data`, room hint.
* **Provenance** is injected into `payload.context`. Because `cid` is fresh per fire,
  consumers that need idempotency should dedup on `job_id` + `scheduled_run_time`.
* **`scheduled_run_time`** (the slot the trigger was *due*) vs **`fired_at`** (when the
  emitter actually ran) lets consumers detect late/catch-up fires. APScheduler 3.x does
  not hand the run time to the job, so a small custom executor
  (`RunTimeInjectingExecutor`) stashes it in a contextvar that `create_task` captures for
  that fire — see §6.

### Misfire policy

Behavior around downtime is **explicit**, not defaulted: `MISFIRE_GRACE_TIME` (default
30s) — a fire whose due time was missed by more than this is skipped (logged as missed)
rather than run late; `COALESCE` (default true) — multiple missed fires of one job
collapse into a single catch-up run. Both are env-tunable in `config.py`.

It emits a single kick-off event and stops — interpreting `event_data` and doing the
work belongs to the consumer.

---

## 4. Stream Routing

The target stream is always **known at job-creation time**, never invented at fire time:

* **Explicit:** `JobCreate.target_stream_id` set → publish to `stream:<target_stream_id>`.
* **Derived:** omitted → publish to `stream:<job_id>` (the caller-chosen `job_id`).

Either way the consumer can subscribe before the first fire, and every fire of a job
lands on the same stream. On first emission to a stream the publisher `SADD`s it into
`streams:active` so the bus's discovery / observers / reaper see it, exactly like a
gateway-created stream.

**"Rooms".** The scheduler stays room-agnostic: a room is just a stream id.
* *Shared-stream room* (many consumers read one stream) — works today; set
  `target_stream_id` to the room id. For broadcast semantics each consumer uses its own
  consumer group.
* *Socket.IO room* (live multi-client fan-out) — requires a **gateway enhancement in
  `agent_bus`** (`enter_room` + a room-stream observer emitting with `room=`). The
  scheduler still only writes to `stream:<room_id>`; an optional `room` hint is carried
  in `payload.context` for a future room-aware gateway. Tracked as a follow-up in agent_bus.

---

## 5. Deployment Configuration (`docker-compose.yml`)

`valkey-bus` is **owned by the `agent_bus` compose project** (it sets
`container_name: valkey-bus`). This service does **not** redeclare it — it connects over
the shared external `logus2k_network`. Base image is **glibc** (`python:3.12-slim`)
because `valkey-glide` has no musl wheels.

```yaml
services:
  agent-scheduler-app:
    build:
      context: .
      dockerfile: Dockerfile
    image: agent_scheduler:1.0
    container_name: agent-scheduler-app
    restart: unless-stopped
    env_file:
      - .env
    environment:
      VALKEY_HOST: valkey-bus          # reach the bus by service name on the network
    ports:
      - "127.0.0.1:6816:6816"          # admin API, localhost-bound (nginx fronts external access)
    networks:
      - logus2k_network
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request,sys; urllib.request.urlopen('http://127.0.0.1:6816/health'); sys.exit(0)"]
      interval: 10s
      timeout: 5s
      retries: 5

networks:
  logus2k_network:
    external: true
```

> `valkey-bus` must already be running (start the `agent_bus` compose first). The app
> retries the Valkey connection on startup so ordering across compose projects is forgiving.

---

## 6. Operational Logic & Data Flow

1. **Create** — a consumer calls `POST /jobs`. The route validates `JobCreate`, builds an
   APScheduler trigger (interval/cron/date), and `add_job`s it with `func=emit_scheduled_event`
   and JSON-serializable kwargs (`job_id`, `target_stream_id`, `event_type`, `event_data`, `room`).
   The job is persisted (pickled) in `RedisJobStore` immediately.
2. **Fire** — at the trigger time the `RunTimeInjectingExecutor` records the scheduled run
   time in a contextvar and APScheduler invokes `emit_scheduled_event` on the event loop.
   It mints `cid`/`sid`, reads the scheduled run time, builds the envelope, and the glide
   publisher `XADD`s it to `stream:<resolved_target>` (registering the stream in
   `streams:active` on first use).
3. **Consume** — any actor/gateway reading that stream receives a standard envelope and
   reacts. The scheduler is done.
4. **Restart** — on container restart the scheduler reconnects, reads `RedisJobStore`, and
   resumes all pending triggers automatically.

### Resource retention

The scheduler is a long-lived *producer* that never "disconnects", so `agent_bus`'s
client-lifecycle cleanup never reclaims scheduler-created resources. The emitter
therefore bounds its own footprint:

* **`sid:<cid>` keys** — each fire mints a fresh `cid` and `INCR sid:<cid>`; the
  key is given a TTL (`SID_TTL_S`, default 3600s, matching agent_bus). agent_bus
  consumers that continue the `cid` refresh it; otherwise it self-expires.
* **Streams** — `publish` applies an approximate `MAXLEN ~ N` (`STREAM_MAXLEN`,
  default 10000; `0` disables). A scheduler stream is a long-lived shared channel,
  so a cap (not a TTL, which would wipe history between infrequent fires) is the
  right bound. *Approximate* trimming only evicts whole radix-tree nodes, so
  `XLEN` hovers near — and can briefly exceed — `N`; a consumer more than ~N
  behind loses the oldest unprocessed events.
* **`streams:active`** — `DELETE /jobs/{id}` removes a job's *derived* stream
  (id == `job_id`, unique) from the active set; explicit/shared `target_stream_id`
  streams are left untouched. Optionally also deletes the derived stream key
  (`STREAM_DELETE_ON_JOB_DELETE`, default false).
* **Fire path** — emit errors are logged at ERROR with `job_id` +
  `scheduled_run_time` and re-raised (APScheduler records the failure). The
  `sid` TTL is set before publish, so a partial failure can't orphan a no-TTL key.

### Persistence & observability notes

* `RedisJobStore` stores **pickled** job state under `agent_scheduler.jobs` /
  `agent_scheduler.run_times`. It is Python-only and not human-readable — observability is
  via the API (`GET /jobs`, which calls `scheduler.get_jobs()`), **not** by reading raw keys.
* The pickled callable is stored **by reference** (`agent_scheduler.emitter:emit_scheduled_event`),
  so the import path must stay stable across deploys.

---

## 7. Implementation Checklist

* [ ] **Vendored contract:** copy `envelope.py` from `agent_bus`; add `schedule.fired`.
* [ ] **Bus client:** thin glide wrapper (`publish` / `incr` / `sadd`) + connection retry.
* [ ] **Emitter:** module-level `emit_scheduled_event` coroutine using a process-global publisher.
* [ ] **Executor:** `RunTimeInjectingExecutor` surfacing the per-fire `scheduled_run_time`.
* [ ] **Config:** env-driven `Settings` (Valkey conn, key prefixes, API host/port, sender id, misfire policy).
* [ ] **Data models:** Pydantic `JobCreate` / `JobUpdate` / `JobView` (interval, cron, date).
* [ ] **Scheduler:** `AsyncIOScheduler` + `RedisJobStore` + executor + misfire defaults; trigger factory (cron via `from_crontab`).
* [ ] **API:** FastAPI routes + lifespan (`start`/`shutdown`) + DI for the scheduler.
* [ ] **Health:** `/health` verifying glide connectivity and job-store reachability.
* [ ] **Container:** glibc Dockerfile + `docker-compose.yml` on external `logus2k_network`.
</content>
