# Implementation Plan: Agent Scheduler

Goal of phase 1: a **running container** that exposes the FastAPI admin API and emits
valid `EventEnvelope` messages onto `valkey-bus` on schedule. Phases 2–3 (Client SDK
docs, Admin Web UI) build on the resulting API.

Reference services: `agent_bus` (same ecosystem) supplies the envelope contract, the
glide patterns, the Dockerfile/compose conventions, and the `logus2k_network`.

---

## Target layout

```
agent_scheduler/
  documents/
    technical_architecture.md
    interface_specification.md
    implementation_plan.md          ← this file
  src/agent_scheduler/
    __init__.py
    config.py        # env-driven Settings (Valkey, key prefixes, API host/port, sender id)
    envelope.py      # VENDORED from agent_bus + schedule.fired event type
    bus_client.py    # thin glide publisher: connect/publish/incr/sadd + retry
    emitter.py       # emit_scheduled_event coroutine + process-global publisher
    models.py        # Pydantic JobCreate / JobUpdate / JobView + trigger factory
    scheduler.py     # AsyncIOScheduler + RedisJobStore wiring
    api.py           # FastAPI app, routes, lifespan, DI
    app.py           # uvicorn entrypoint (python -m agent_scheduler.app)
  tests/
    test_envelope.py        # envelope still matches the agent_bus contract
    test_models.py          # trigger factory + validation
    test_emitter.py         # emit builds a correct envelope (fake publisher)
    test_api.py             # CRUD over a MemoryJobStore (no Valkey needed)
  requirements.txt
  Dockerfile
  docker-compose.yml
  .env.example
  .gitignore                # already present (.venv_agent_scheduler/)
```

---

## Dependencies (`requirements.txt`)

```
fastapi                 # admin API
uvicorn                 # ASGI server
APScheduler==3.11.*     # 3.x line — AsyncIOScheduler + RedisJobStore (NOT 4.x)
redis                   # RedisJobStore backend (redis-py; talks to valkey-bus)
valkey-glide==2.4.2     # event emission — pinned to match agent_bus wire client
pydantic==2.13.4        # models + vendored EventEnvelope (match agent_bus)
python-dotenv==1.2.2    # .env loading in dev

# dev/test
pytest
pytest-asyncio
httpx                   # FastAPI TestClient / async API tests
```

> Pin exact versions before first release; APScheduler stays on the **3.x** line.

---

## Phase 1 — Running container

### Step 1 · Vendored contract & config
- [ ] Copy `agent_bus/src/agent_bus/envelope.py` → `envelope.py` verbatim; add header comment
      naming it a vendored copy + canonical source path; add `EventType.SCHEDULE_FIRED = "schedule.fired"`.
- [ ] `config.py`: `Settings` (frozen dataclass, env-driven, safe defaults) — `valkey_host/port`,
      `stream_prefix="stream:"`, `active_streams_key="streams:active"`, `jobs_key`/`run_times_key`,
      `api_host="0.0.0.0"`, `api_port=6816`, `sender_id="agent_scheduler"`,
      `default_event_type="schedule.fired"`, `log_level`.
- **Done when:** `test_envelope.py` round-trips an envelope and asserts the wire shape matches agent_bus.

### Step 2 · Bus client (glide publisher)
- [ ] `bus_client.py`: `Publisher` with `connect()` (retry loop until valkey-bus reachable),
      `publish(stream, env)` (XADD via `env.to_fields()`), `incr(key)`, `sadd(key, member)`, `ping()`, `close()`.
- **Done when:** against a local Valkey, `publish` writes an entry readable by an agent_bus consumer.

### Step 3 · Emitter
- [ ] `emitter.py`: process-global `_publisher`; `connect()/close()`; module-level coroutine
      `emit_scheduled_event(job_id, target_stream_id, event_type, event_data, room=None)`:
      resolve stream (`target_stream_id` or `job_id`) → `stream:<id>`; `cid=uuid4()`;
      `sid=incr("sid:"+cid)`; build envelope via `new_event(...)` with provenance in `context`;
      `sadd(active_streams_key, resolved_id)` on first use; `publish`.
- **Done when:** `test_emitter.py` (fake publisher) asserts header/payload/context fields.

### Step 4 · Models & trigger factory
- [ ] `models.py`: `JobCreate`, `JobUpdate`, `JobView`; `job_id` regex validation;
      `build_trigger(trigger_type, trigger_args)` (interval / `CronTrigger.from_crontab` / date);
      `job_to_view(job)` mapping an APScheduler `Job` → `JobView` (incl. `resolved_stream`, `next_run_time`).
- **Done when:** `test_models.py` covers each trigger type + invalid input → error.

### Step 5 · Scheduler wiring
- [ ] `scheduler.py`: build `AsyncIOScheduler` with `RedisJobStore(jobs_key, run_times_key)`;
      expose `get_scheduler()`.
- **Done when:** scheduler starts, rehydrates persisted jobs after a restart.

### Step 6 · API
- [ ] `api.py`: FastAPI app + lifespan (`emitter.connect()` → `scheduler.start()`; reverse on shutdown);
      routes per the Interface Specification (`/jobs` CRUD, pause/resume/run, `/health`);
      `409` on duplicate `job_id`, `404` on missing, `422` on bad trigger args.
- [ ] `app.py`: uvicorn entrypoint on `api_host:api_port`.
- **Done when:** `test_api.py` exercises full CRUD against a `MemoryJobStore` (Valkey-free).

### Step 7 · Container
- [ ] `Dockerfile`: `python:3.12-slim-bookworm` (glibc — glide has no musl wheels), deps-first layer,
      `PYTHONPATH=/app/src`, `CMD ["python","-m","agent_scheduler.app"]`.
- [ ] `docker-compose.yml`: single `agent-scheduler-app` service on external `logus2k_network`,
      port `127.0.0.1:6816:6816`, `/health` healthcheck. Do **not** redeclare `valkey-bus`.
- [ ] `.env.example` mirroring config defaults.
- **Done when (phase exit):** `docker compose up` (with agent_bus's valkey-bus running) →
  `GET /health` is `200`; a created interval job emits envelopes onto `stream:<id>` that an
  agent_bus consumer reads; jobs survive `docker compose restart`.

### Verification (manual, end of phase 1)
1. Start agent_bus stack (provides `valkey-bus` + `logus2k_network`).
2. `docker compose up -d --build` here; confirm `GET /health` → `200`.
3. `POST /jobs` an interval job (`seconds: 5`, `event_data:{ping:1}`); observe the stream
   (`stream:<job_id>`) filling with valid envelopes (`sender=agent_scheduler`, `event_type=schedule.fired`).
4. `restart` the container; `GET /jobs` shows the job; emissions resume.
5. `pause` / `resume` / `DELETE` behave per spec.

---

## Phase 2 — Client SDK documentation *(after a running container)*

- [ ] Document a thin client over the REST API: create/list/get/update/delete/pause/resume/run.
- [ ] Examples per trigger type; the envelope shape consumers receive; the `resolved_stream` contract.
- [ ] Idempotency guidance (dedup on `job_id` + `scheduled_run_time`, since `cid` is per-fire).
- [ ] Reference implementation language(s) TBD with the user.

## Phase 3 — Admin Web UI *(built on the Client SDK)*

- [ ] CRUD UI over jobs (list, create with trigger builder, edit, pause/resume, delete, run-now).
- [ ] Live view of `next_run_time` / `resolved_stream` / paused state.
- [ ] Stack TBD with the user; consumes the Phase-2 SDK, not the raw API directly.

---

## Resolved decisions

- APScheduler pinned to `3.11.0` (3.x line).
- Admin API on `6816` (agent_bus gateway uses `6815`) — no clash.
- `event_type` is **fully caller-settable** per job (default `schedule.fired`); no
  allow-list. Scheduler-origin stays identifiable via `sender` + `context.job_id`.
- `scheduled_run_time` **is** emitted per fire, via `RunTimeInjectingExecutor` (a
  contextvar captured by `create_task`). Misfire policy is explicit:
  `MISFIRE_GRACE_TIME=30s`, `COALESCE=true` (both env-tunable).
</content>
