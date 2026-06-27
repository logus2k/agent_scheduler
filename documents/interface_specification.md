# Interface Specification: Agent Scheduler API

RESTful interface for the **single-process** `agent_scheduler` (FastAPI + APScheduler).
All mutations act on the live in-process `AsyncIOScheduler` and are persisted to the
Valkey-backed `RedisJobStore` in the same call — there is no second process to sync with.

Base URL (internal): `http://agent-scheduler-app:6816`

---

## 1. Data Models (Pydantic)

### `JobCreate` (request body — `POST /jobs`)

```jsonc
{
  "job_id": "nightly-report",          // required; ^[A-Za-z0-9._:-]+$ (used as a stream key when derived)
  "trigger_type": "cron",              // "interval" | "cron" | "date"
  "trigger_args": {                    // only the fields for the chosen trigger_type are read
    "seconds": 300,                    // interval: any of seconds/minutes/hours/days (≥1 required)
    "cron_expression": "0 2 * * *",    // cron: 5-field crontab → CronTrigger.from_crontab(...)
    "run_date": "2026-12-31T23:59:00Z" // date: ISO-8601 one-shot
  },
  "target_stream_id": null,            // optional; null → derive stream:<job_id>
  "event_type": "schedule.fired",      // optional; default "schedule.fired"
  "event_data": { "report": "daily_sales" },  // optional; becomes payload.data verbatim
  "room": null,                        // optional delivery hint → payload.context.room
  "paused": false                      // optional; create the job in a paused state
}
```

Validation rules:

* `job_id` is required, non-empty, and matches `^[A-Za-z0-9._:-]+$` (it may become part of
  a stream key). Creating a job whose `job_id` already exists → `409 Conflict`.
* `trigger_args` must contain the fields required by `trigger_type`; otherwise `422`.
* `cron_expression` is a standard 5-field crontab string.

### `JobUpdate` (request body — `PATCH /jobs/{job_id}`)

All fields optional; only the supplied ones change. Updating `trigger_type` /
`trigger_args` reschedules the job; the others rewrite the emitted envelope.

```jsonc
{
  "trigger_type": "interval",
  "trigger_args": { "minutes": 15 },
  "target_stream_id": "ops-dashboard",
  "event_type": "schedule.fired",
  "event_data": { "report": "hourly_sales" },
  "room": "ops-dashboard"
}
```

### `JobView` (response body)

```jsonc
{
  "job_id": "nightly-report",
  "trigger_type": "cron",
  "trigger": "cron[hour='2', minute='0']",   // human-readable APScheduler trigger
  "next_run_time": "2026-06-28T02:00:00+00:00", // null when paused
  "resolved_stream": "stream:nightly-report",   // exact stream events land on
  "event_type": "schedule.fired",
  "event_data": { "report": "daily_sales" },
  "room": null,
  "paused": false
}
```

`resolved_stream` is always echoed so callers relying on the derived default never
recompute it.

---

## 2. Endpoints

| Method | Endpoint | Description | Success | Errors |
| --- | --- | --- | --- | --- |
| `GET`  | `/jobs` | List all jobs (`JobView[]`). | `200` | — |
| `GET`  | `/jobs/{job_id}` | One job. | `200` | `404` |
| `POST` | `/jobs` | Create a job. | `201` (`JobView`) | `409`, `422` |
| `PATCH`| `/jobs/{job_id}` | Update trigger/payload. | `200` (`JobView`) | `404`, `422` |
| `DELETE`| `/jobs/{job_id}` | Remove a job. | `204` | `404` |
| `POST` | `/jobs/{job_id}/pause` | Pause (stop firing, keep definition). | `200` (`JobView`) | `404` |
| `POST` | `/jobs/{job_id}/resume` | Resume a paused job. | `200` (`JobView`) | `404` |
| `POST` | `/jobs/{job_id}/run` | Emit once now, off-schedule. | `202` | `404` |
| `GET`  | `/health` | Liveness + Valkey/job-store reachability. | `200` | `503` |

`GET /health` response:

```jsonc
{ "status": "ok", "valkey": "up", "jobstore": "up", "jobs": 7 }
```

---

## 3. Implementation Details

### Lifespan & dependency injection

The scheduler is created once, started in the FastAPI lifespan, and shared with routes
via a dependency.

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.redis import RedisJobStore

scheduler = AsyncIOScheduler(jobstores={
    "default": RedisJobStore(
        host=settings.valkey_host, port=settings.valkey_port,
        jobs_key="agent_scheduler.jobs", run_times_key="agent_scheduler.run_times",
    )
})

@asynccontextmanager
async def lifespan(app: FastAPI):
    await emitter.connect()   # establish the glide publisher (with retry)
    scheduler.start()         # rehydrates jobs from RedisJobStore
    yield
    scheduler.shutdown()
    await emitter.close()

app = FastAPI(lifespan=lifespan)

def get_scheduler() -> AsyncIOScheduler:
    return scheduler
```

### Trigger construction

```python
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

def build_trigger(t: str, a: dict):
    if t == "interval":
        return IntervalTrigger(**{k: a[k] for k in ("seconds","minutes","hours","days") if k in a})
    if t == "cron":
        return CronTrigger.from_crontab(a["cron_expression"])
    if t == "date":
        return DateTrigger(run_date=a["run_date"])
    raise ValueError(f"unknown trigger_type: {t}")
```

### Registering the fire callable

The job stores a **reference** to a module-level coroutine plus JSON-serializable kwargs —
never a live connection (which is not picklable). The publisher is a process global the
coroutine reaches at fire time.

```python
scheduler.add_job(
    func="agent_scheduler.emitter:emit_scheduled_event",  # stable import path
    trigger=build_trigger(body.trigger_type, body.trigger_args),
    id=body.job_id,
    replace_existing=False,
    kwargs={
        "job_id": body.job_id,
        "target_stream_id": body.target_stream_id,   # None → derive stream:<job_id>
        "event_type": body.event_type,
        "event_data": body.event_data,
        "room": body.room,
    },
)
```

---

## 4. Flow Summary

1. Consumer → `POST /jobs` on `agent-scheduler-app:6816`.
2. Route validates, builds the trigger, `add_job`s it → persisted (pickled) in
   `RedisJobStore` and live in the in-memory scheduler at once.
3. At trigger time the scheduler runs `emit_scheduled_event`, which mints `cid`/`sid`,
   builds a standard `EventEnvelope`, and `XADD`s it to `stream:<resolved_target>`
   (registering the stream in `streams:active` on first use).
4. Any consumer reading that stream reacts to a normal envelope.

---

## 5. Out of Scope (for this phase)

* **AuthN/AuthZ** — the API is localhost-bound and fronted by the corporate nginx;
  no per-request auth in this phase.
* **Horizontal scaling** — single process by design (see Technical Architecture §1).
* **Client SDK** and **Admin Web UI** — subsequent phases built on top of this API.
</content>
