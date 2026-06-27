# Scheduler Agent

A small, single-process **trigger service** for the `logus2k` ecosystem. It owns
time-based triggers (interval / cron / one-shot) and, when a trigger fires, emits
a standard `EventEnvelope` onto the shared `valkey-bus` so any other agent can
react. It is a *pure trigger actor*: it does no business logic itself — it just
puts a well-formed event on a stream at the right moment.

It bundles a **FastAPI** admin API, an **APScheduler** engine with a Valkey-backed
job store (restart-resilient), and a built-in **web admin UI** — all in one
container.

> The product name is **Scheduler Agent**. The code package, image, and container
> use the identifier `agent_scheduler` / `agent-scheduler-app`.

---

## Why it exists

Agents on the bus react to events. Some work needs to start *on a schedule*
("every night at 02:00", "every 30s", "once next Tuesday") rather than in
response to another agent. Scheduler Agent is the component that turns time into
bus events, without baking scheduling into every agent.

## Features

- **Three trigger types** — `interval`, `cron` (5-field crontab), `date` (one-shot).
- **Restart-proof** — jobs persist in Valkey (`RedisJobStore`); on restart the
  scheduler rehydrates and resumes them.
- **Bus-native output** — emits the exact `EventEnvelope` contract used by
  `agent_bus`, byte-for-byte, including a per-fire `scheduled_run_time`.
- **Admin REST API** — full CRUD plus pause / resume / run-now / health.
- **Client SDKs** — Python and JavaScript (ES6).
- **Built-in web UI** — create/manage jobs, light/dark theme, per-field help, and
  a floating usage guide — served from the same container.
- **Defined misfire policy** — explicit grace time + coalescing for downtime.

---

## Architecture at a glance

```
                ┌──────────────── agent-scheduler-app (one container) ───────────────┐
 HTTP (CRUD) ──▶│  FastAPI  ─▶  AsyncIOScheduler ──(on fire)─▶  emitter.emit()        │
   + Web UI     │     │              │                               │                 │
                │     │              ▼                               ▼                 │
                │     │        RedisJobStore                  glide publisher           │
                └─────┼──────────────┼─────────────────────────────┼──────────────────┘
                      ▼              ▼                             ▼
                  (this API)  valkey-bus: agent_scheduler.jobs   valkey-bus: stream:<target>
                              (pickled job state, persistence)   (EventEnvelope → consumers)
```

- **Single process** by design: FastAPI and `AsyncIOScheduler` share one event
  loop, so API changes take effect immediately and exactly one scheduler ever
  fires a job (no duplicate emissions). The job store is for *persistence*, not
  coordination.
- **Two Valkey clients, on purpose:** `redis-py` drives `RedisJobStore` (job
  persistence); `valkey-glide` drives event emission (same wire client as the
  rest of the bus). Separate concerns, separate keyspaces.
- **Vendored contract:** `src/agent_scheduler/envelope.py` is a verbatim copy of
  `agent_bus`'s envelope so emitted events are indistinguishable from any other
  actor's.

Full detail: [documents/technical_architecture.md](documents/technical_architecture.md).

---

## Quick start

**Prerequisites** (owned by the `agent_bus` compose project): the external
`logus2k_network` Docker network and a running `valkey-bus` service. Start those
first.

```bash
cp .env.example .env          # safe defaults; VALKEY_HOST is overridden in compose
docker compose up -d --build
curl http://127.0.0.1:6816/health
# {"status":"ok","valkey":"up","jobstore":"up","jobs":0}
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
| `POST` | `/jobs/{job_id}/run` | Emit once now (off-schedule) |
| `GET` | `/health` | Liveness + Valkey/job-store reachability |

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
                            "fired_at": "…", "scheduled_run_time": "…" } },
  "metadata": { "version": "1.0", "trace_parent": null }
}
```

**Consumer tips:** dedup on `context.job_id` + `context.scheduled_run_time`
(`cid` is fresh per fire); identify scheduled events by `header.sender`, not by
`event_type`.

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
| `JOBS_KEY` / `RUN_TIMES_KEY` | `agent_scheduler.jobs` / `.run_times` | RedisJobStore keys |
| `SENDER_ID` | `agent_scheduler` | `header.sender` on emitted events |
| `DEFAULT_EVENT_TYPE` | `schedule.fired` | Default `event_type` |
| `MISFIRE_GRACE_TIME` | `30` | Seconds late before a fire is skipped |
| `COALESCE` | `true` | Collapse missed fires into one catch-up |
| `API_HOST` / `API_PORT` | `0.0.0.0` / `6816` | Admin API bind |
| `FRONTEND_DIR` / `DOCS_DIR` | `frontend` / `documents` | Static UI + served docs |
| `LOG_LEVEL` | `INFO` | Logging |

---

## Project layout

```
agent_scheduler/
  src/agent_scheduler/   # config, envelope (vendored), bus_client, emitter,
                         # executor, models, scheduler, api, app
  frontend/              # vanilla ES6 admin UI (served at /)
  sdk/python | sdk/javascript
  documents/             # the docs linked below
  tests/                 # pytest (Valkey-free)
  Dockerfile · docker-compose.yml · requirements.txt · .env.example
```

## Development

```bash
python -m venv .venv_agent_scheduler && . .venv_agent_scheduler/bin/activate
pip install -r requirements.txt
pytest -q                       # 28 tests, no Valkey needed (MemoryJobStore + fakes)
python -m agent_scheduler.app   # run locally (needs a reachable Valkey)
```

## Documentation

| Doc | What's in it |
| --- | --- |
| [technical_architecture.md](documents/technical_architecture.md) | Design, process model, data flow, misfire policy |
| [interface_specification.md](documents/interface_specification.md) | REST API, models, lifespan, trigger factory |
| [client_sdk.md](documents/client_sdk.md) | Python + JS SDK usage |
| [admin_ui.md](documents/admin_ui.md) | Web UI, auth, how it's served |
| [use_cases.md](documents/use_cases.md) | Six worked examples (UI walkthroughs) |
| [implementation_plan.md](documents/implementation_plan.md) | Build plan & decisions |
</content>
