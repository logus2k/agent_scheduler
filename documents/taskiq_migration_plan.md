# agent_scheduler → Taskiq migration plan

**Status:** proposal for review. No code until approved.

## 1. Why

The 07:00 cron fire was **silently dropped**. Measured root cause: a WSL2 kernel bug —
`CLOCK_MONOTONIC` runs ~5% slow on this host (microsoft/WSL #12583; the host wall clock
stays correct via NTP, but the monotonic clock the scheduler *sleeps on* is slow).
APScheduler arms **one long monotonic sleep** until the next job, so on the slow clock it
wakes minutes late, exceeds `misfire_grace_time` (30 s), and **skips** the run.

Taskiq's scheduler **re-anchors to the wall clock every minute** (sleeps to the next minute,
re-reads `datetime.now()`), so drift can't accumulate past ~1 tick. Migrating swaps the
*engine* to fix this. **The bus contract and agent_runtime are untouched.**

## 2. Invariants — what does NOT change

- **Bus contract:** a `schedule.fired` `EventEnvelope` on `stream:<target>` with a fresh
  `cid`/`sid`, `payload.context = {job_id, fired_at, trigger_type, scheduled_run_time, room}`,
  and `SADD active_streams`. agent_runtime's farm sees byte-identical events. **Zero downstream change.**
- **Admin API surface:** `POST/GET/PATCH/DELETE /jobs`, `POST /jobs/{id}/run|pause|resume`,
  `/health` — same `JobCreate`/`JobView` shapes.
- **Deployment:** single container, single process, `127.0.0.1:6816`, `logus2k_network`.
- **Persistence across restart** (jobs survive) and **per-job cron + timezone**.

## 3. Target architecture (grounded in the Taskiq docs)

All components run **embedded in the existing FastAPI process** — officially supported via
`taskiq.api.run_receiver_task` / `run_scheduler_task` run as asyncio tasks (see Taskiq's
`docs/examples/dynamics/dyn_scheduler.py`).

| Component | Choice | Notes |
|---|---|---|
| Broker | `RedisStreamBroker(url=valkey)` | supports acks → durable (README: "fine when data durability is required") |
| Result backend | `DummyResultBackend` | fire-and-forget; no result storage → no unbounded growth |
| Schedule source | `ListRedisScheduleSource(url=valkey, prefix=…)` | dynamic + persistent; `cron` + `cron_offset` (tz); `add_schedule`/`delete_schedule`/`get_schedules` |
| Scheduler | `TaskiqScheduler(broker, [source])` | per-minute, wall-clock-anchored; **only one instance** (single container ✓) |
| Worker | `run_receiver_task(broker)` | embedded asyncio task; consumes broker, runs the task |
| The task | `emit_trigger(...)` `@broker.task` | publishes the canonical envelope to the bus, reusing `emitter.py`'s existing `valkey-glide` Publisher logic |

Connections: Taskiq uses **redis-py (asyncio)** to valkey for the broker/schedule source;
the envelope publish still uses the **vendored `valkey-glide` Publisher**. Both hit
`valkey-bus`; keys namespaced. (Two client libs, one server — acceptable.)

## 4. Job model & persistence (preserves the admin API incl. pause/resume)

Keep an **authoritative job registry** as the API's source of truth, and **derive** the
Taskiq schedule source from it (idempotent reconcile):

- Registry: a persistent Redis hash `agent_scheduler.jobs` → `{job_id: JobDefinition}`
  (`job_id, trigger_type, trigger_args{cron_expression,timezone,…}, target_stream_id,
  event_type, event_data, room, paused`).
- Source entry per **non-paused** job: one `ScheduledTask`
  (`schedule_id=job_id`, `task_name="emit_trigger"`, `kwargs={emit params}`,
  `cron=cron_expression`, `cron_offset=timezone`; `interval`/`time` for interval/date jobs).
- Reconcile registry→source on every mutation and on startup.

API mapping:

| Endpoint | Action |
|---|---|
| `POST /jobs` | registry.set + (if not paused) `source.add_schedule` → 201 |
| `GET /jobs[/{id}]` | from registry (+ next_run computed from cron) |
| `PATCH /jobs/{id}` | registry.update + re-sync schedule |
| `DELETE /jobs/{id}` | registry.del + `source.delete_schedule` + `deregister_stream` |
| `POST /jobs/{id}/run` | `emit_trigger.kiq(**kwargs)` (enqueue now) → 202 |
| `POST /jobs/{id}/pause` | registry.paused=true + `source.delete_schedule` |
| `POST /jobs/{id}/resume` | registry.paused=false + `source.add_schedule` |

Trigger types: cron → `cron`+`cron_offset`; interval → `ScheduledTask.interval` (≥1 s);
date (one-shot) → `ScheduledTask.time` (source auto-deletes after fire via `post_send`).

## 5. The double-fire guard (important)

Taskiq has a known minute-boundary **double-send** issue (taskiq #296), and `RedisStreamBroker`
is at-least-once. Each emit makes a **new `cid`**, so a duplicate would deliver the news **twice**.
Mitigation: `emit_trigger` does an idempotency check **before** publishing —
`SET agent_scheduler:fired:{job_id}:{minute} NX EX 120`; if the key already exists, **skip**
(duplicate for this minute). Makes each `(job, minute)` fire **exactly once** regardless of
double-send/redelivery.

## 6. Deployment — the one decision for you

- **Recommended — embedded single process:** uvicorn runs FastAPI; its lifespan does
  `broker.startup()`, connects the bus Publisher, and starts `run_receiver_task` +
  `run_scheduler_task` as asyncio tasks; teardown cancels them + `broker.shutdown()`.
  Keeps today's single-container model exactly.
- **Alternative — separate processes:** uvicorn API + `taskiq worker` + `taskiq scheduler`
  (supervisor in one container, or 3 containers). More "idiomatic," scalable workers — but
  more moving parts and a deployment change. Overkill for one daily emit.

## 7. Dependencies

- **Add:** `taskiq`, `taskiq-redis` (1.2.3), `redis` (redis-py asyncio) — pinned.
- **Remove:** `APScheduler`.
- **Keep:** `fastapi`, `uvicorn`, `valkey-glide` (bus publish), `pydantic`, vendored
  `bus_client`/`envelope`.

## 8. Files

- **Replace:** `scheduler.py` (RedisJobStore wiring), `executor.py`
  (RunTimeInjectingExecutor), the APScheduler half of `build_trigger` in `models.py`.
- **New:** `taskiq_app.py` (broker, source, scheduler, `emit_trigger`, embedded run),
  `registry.py` (job registry + reconcile).
- **Keep/adapt:** `emitter.py` (publish logic reused by `emit_trigger`), `api.py` (rewire to
  registry+source, same shapes), `models.py` (`JobCreate`/`JobView` kept; trigger→`ScheduledTask`
  mapper), `config.py` (+ taskiq keys), `app.py`, `envelope.py`, `bus_client.py`.

## 9. Build steps

1. Deps. 2. Broker + source + scheduler + `emit_trigger`. 3. Job registry + reconcile.
4. Rewire API. 5. Lifespan embed (worker+scheduler). 6. Remove APScheduler. 7. Tests —
unit (trigger→ScheduledTask mapping, registry CRUD, idempotency guard) + integration on live
valkey (add a cron for the **next minute** → assert the envelope lands on the stream within
~70 s; `run-now` works; double-send guard holds). 8. Rebuild container; recreate `news-demo`
+ `news-morning-ai`. 9. **Empirical proof on THIS host:** schedule a job ~2 min out, confirm it
fires within ~1 min onto `stream:agent-runtime`. 10. Update docs/memory.

## 10. Risks

- **Per-minute granularity:** fires within the minute, not to-the-second (fine for 07:00 daily);
  bounded ~1 min late on the broken clock — that's the goal.
- **Double-send (#296):** mitigated by the per-`(job,minute)` idempotency guard.
- **Embedded worker/scheduler:** if the API process restarts, scheduling pauses until it's back
  (same as today). Must keep exactly **one** scheduler (single container ✓).
- **`ListRedisScheduleSource` delete-by-id** (list-backed) — the registry-reconcile path avoids
  relying on it; confirm during impl. Fallback: `RedisScheduleSource` (per-id keys).
- **Two redis client libs** (redis-py + valkey-glide) to one valkey — namespaced; acceptable.
- **Not a root-cause clock fix** — it's the robust workaround (bounds the WSL2 bug's impact).

## 11. Rollback

Keep the APScheduler code on the branch. The bus contract is unchanged, so reverting
agent_scheduler is isolated (agent_runtime untouched); jobs re-created from the registry.
