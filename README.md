# Scheduler Agent

A small, single-process **trigger service** for the `logus2k` ecosystem. It owns
time-based triggers (interval / cron / one-shot) and, when a trigger fires, emits
a standard `EventEnvelope` onto the shared `valkey-bus` so any other agent can
react. It is a *pure trigger actor*: it does no business logic itself — it just
puts a well-formed event on a stream at the right moment.

It bundles a **FastAPI** admin API, an **embedded Taskiq** engine (broker + worker
+ scheduler) backed by a Valkey-persisted job registry (restart-resilient), and a
built-in **web admin UI** — all in one container.

> The product name is **Scheduler Agent**. The code package, image, and container
> use the identifier `agent_scheduler` / `agent-scheduler-app`.

---

## Why it exists

Agents on the bus react to events. Some work needs to start *on a schedule*
("every night at 02:00", "every 30s", "once next Tuesday") rather than in
response to another agent. Scheduler Agent is the component that turns time into
bus events, without baking scheduling into every agent.

## Features

- **Three trigger types** — `interval`, `cron` (5-field crontab, with per-job
  IANA timezone), `date` (one-shot).
- **Restart-proof** — job definitions persist in Valkey (a registry hash); on
  restart the schedule is rebuilt from it.
- **Drift-resistant** — the Taskiq scheduler runs a **per-second, wall-clock
  re-anchored** loop (re-reads `datetime.now()` every tick), so a cron fires
  within ~1 s of its minute even on hosts whose monotonic clock drifts. (This
  replaced APScheduler, whose single long monotonic sleep silently dropped a
  fire on this host's WSL2 clock bug — see `documents/taskiq_migration_plan.md`.)
- **Bus-native output** — emits the exact `EventEnvelope` contract used by
  `agent_bus`, byte-for-byte.
- **Admin REST API** — full CRUD plus pause / resume / run-now / health.
- **Exactly-once per minute** — a Redis `SET NX` guard so Taskiq's at-least-once
  delivery can't double-emit within a minute.
- **Client SDKs** — Python and JavaScript (ES6).
- **Built-in web UI** — create/manage jobs, light/dark theme, per-field help, and
  a floating usage guide — served from the same container.

---

## Architecture at a glance

```
        ┌──────────────────── agent-scheduler-app (one container) ─────────────────────┐
HTTP ──▶│ FastAPI ─▶ job registry ◀── Taskiq scheduler ──(due)──▶ broker ──▶ worker     │
+ UI    │    │         (source of                │  per-second      (Redis    │  runs    │
        │    │          truth)                    │  wall-clock      stream)  ▼ emit_trigger
        │    │                                    │  loop)                 emitter.emit()  │
        └────┼────────────────┼───────────────────┼──────────────────────────┼───────────┘
             ▼                ▼                    ▼                          ▼
         (this API)   valkey-bus:            valkey-bus:               valkey-bus:
                      agent_scheduler.registry  agent_scheduler.taskiq  stream:<target>
                      (job definitions)         (Taskiq broker queue)   (EventEnvelope → consumers)
```

- **Single process** by design: FastAPI plus the **embedded** Taskiq worker +
  scheduler (`taskiq.api.run_receiver_task` / `run_scheduler_task`) share one
  event loop. Exactly one scheduler instance fires (no duplicate emissions).
- **Registry is the source of truth:** the admin API reads/writes a Redis hash;
  the Taskiq schedule source *derives* schedules from it (picked up on the
  scheduler's ~60 s refresh; `run-now` is immediate).
- **Two Valkey clients, on purpose:** `redis-py` drives the Taskiq broker + the
  job registry/dedupe; `valkey-glide` drives event emission (same wire client as
  the rest of the bus). Separate concerns, separate keyspaces.
- **Vendored contract:** `src/agent_scheduler/envelope.py` is a verbatim copy of
  `agent_bus`'s envelope so emitted events are indistinguishable from any other
  actor's.

Full detail: [documents/technical_architecture.md](documents/technical_architecture.md)
and the migration: [documents/taskiq_migration_plan.md](documents/taskiq_migration_plan.md).

---

## Quick start

**Prerequisites** (owned by the `agent_bus` compose project): the external
`logus2k_network` Docker network and a running `valkey-bus` service. Start those
first.

```bash
cp .env.example .env          # safe defaults; VALKEY_HOST is overridden in compose
docker compose up -d --build
curl http://127.0.0.1:6816/health
# {"status":"ok","valkey":"up","registry":"up","jobs":0}
```

Create a job and watch it fire:

```bash
curl -X POST http://127.0.0.1:6816/jobs -H 'Content-Type: application/json' -d '{
  "job_id": "demo", "trigger_type": "interval",
  "trigger_args": { "seconds": 5 }, "event_data": { "ping": 1 }
}'
# events land on stream:demo on valkey-bus
```

> Base image is `python:3.12-slim` (glibc). **Alpine will not work** — `valkey-glide`
> ships glibc-only wheels (no musl). The Valkey *server* container may stay on Alpine.

---

## Admin web UI

- **Direct:** `http://127.0.0.1:6816/` (UI served at the app root).
- **Public:** `https://logus2k.com/scheduler/` via the reverse proxy, gated by
  Google sign-in **restricted to the owner's email** (`/oauth2/auth-admin`).

Create jobs (with a trigger builder), pause/resume, run-now, and delete; toggle
light/dark; hover the `?` badges for per-field help; open **Help** for the usage
guide. See [documents/admin_ui.md](documents/admin_ui.md).

---

## REST API

Base: `http://agent-scheduler-app:6816` (internal) — OpenAPI at `/openapi.json`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/jobs` | List jobs |
| `GET` | `/jobs/{job_id}` | Get one job |
| `POST` | `/jobs` | Create a job |
| `PATCH` | `/jobs/{job_id}` | Update trigger/payload |
| `DELETE` | `/jobs/{job_id}` | Delete a job |
| `POST` | `/jobs/{job_id}/pause` | Pause |
| `POST` | `/jobs/{job_id}/resume` | Resume |
| `POST` | `/jobs/{job_id}/run` | Emit once now (off-schedule, un-deduped) |
| `GET` | `/health` | Liveness + Valkey/registry reachability |

Full schema, models, and error codes: [documents/interface_specification.md](documents/interface_specification.md).

### The event it emits

On each fire, one `EventEnvelope` is published to `stream:<resolved_target>`
(the `target_stream_id`, else `stream:<job_id>`):

```jsonc
{
  "header": { "stream_id": "demo", "cid": "…uuid", "sid": 1,
              "timestamp": "…", "sender": "agent_scheduler",
              "event_type": "schedule.fired" },
  "payload": { "data": { "ping": 1 },
               "context": { "job_id": "demo", "trigger_type": "interval",
                            "fired_at": "…" } },
  "metadata": { "version": "1.0", "trace_parent": null }
}
```

**Consumer tips:** dedup on `context.job_id` (`cid` is fresh per fire); identify
scheduled events by `header.sender`, not by `event_type`.

---

## Client SDKs

- **Python** — [`sdk/python/agent_scheduler_client.py`](sdk/python/agent_scheduler_client.py) (httpx).
- **JavaScript ES6** — [`sdk/javascript/agentSchedulerClient.js`](sdk/javascript/agentSchedulerClient.js) (zero-dependency, native `fetch`).

```python
from agent_scheduler_client import SchedulerClient
with SchedulerClient("http://agent-scheduler-app:6816") as s:
    s.create_cron("nightly-report", "0 2 * * *", event_data={"report": "daily_sales"})
```
```javascript
import { SchedulerClient } from "./agentSchedulerClient.js";
const s = new SchedulerClient("http://agent-scheduler-app:6816");
await s.createInterval("heartbeat", { seconds: 30 });
```

Both expose typed errors (`JobNotFoundError`, `JobConflictError`,
`ValidationError`, `ServiceUnavailableError`). See [documents/client_sdk.md](documents/client_sdk.md).

---

## Configuration

All via environment (safe defaults in `src/agent_scheduler/config.py`; see
[.env.example](.env.example)).

| Variable | Default | Purpose |
| --- | --- | --- |
| `VALKEY_HOST` / `VALKEY_PORT` | `127.0.0.1` / `6379` | Shared `valkey-bus` connection |
| `STREAM_PREFIX` | `stream:` | Stream key prefix (must match agent_bus) |
| `ACTIVE_STREAMS_KEY` | `streams:active` | Discovery set the publisher registers into |
| `REGISTRY_KEY` | `agent_scheduler.registry` | Redis hash of job definitions (source of truth) |
| `TASKIQ_QUEUE` | `agent_scheduler.taskiq` | Taskiq broker queue (its own stream) |
| `DEDUPE_TTL_S` | `120` | TTL of the per-(job,minute) idempotency guard |
| `SENDER_ID` | `agent_scheduler` | `header.sender` on emitted events |
| `DEFAULT_EVENT_TYPE` | `schedule.fired` | Default `event_type` |
| `API_HOST` / `API_PORT` | `0.0.0.0` / `6816` | Admin API bind |
| `FRONTEND_DIR` / `DOCS_DIR` | `frontend` / `documents` | Static UI + served docs |
| `LOG_LEVEL` | `INFO` | Logging |

---

## Project layout

```
agent_scheduler/
  src/agent_scheduler/   # config, envelope (vendored), bus_client, emitter,
                         # models, registry, tasks (Taskiq), api, app
  frontend/              # vanilla ES6 admin UI (served at /)
  sdk/python | sdk/javascript
  documents/             # the docs linked below
  tests/                 # pytest (Valkey-free: unit + monkeypatched API)
  Dockerfile · docker-compose.yml · requirements.txt · .env.example
```

## Development

```bash
python -m venv .venv_agent_scheduler && . .venv_agent_scheduler/bin/activate
pip install -r requirements.txt
pytest -q                       # unit + API tests, no Valkey needed (fakes)
python -m agent_scheduler.app   # run locally (needs a reachable Valkey)
```

## Documentation

| Doc | What's in it |
| --- | --- |
| [taskiq_migration_plan.md](documents/taskiq_migration_plan.md) | **Why/how APScheduler → Taskiq** (the current engine) |
| [technical_architecture.md](documents/technical_architecture.md) | Design, process model, data flow |
| [interface_specification.md](documents/interface_specification.md) | REST API, models, lifespan |
| [client_sdk.md](documents/client_sdk.md) | Python + JS SDK usage |
| [admin_ui.md](documents/admin_ui.md) | Web UI, auth, how it's served |
| [use_cases.md](documents/use_cases.md) | Worked examples (UI walkthroughs) |
| [implementation_plan.md](documents/implementation_plan.md) | Original build plan (historical) |
</content>
