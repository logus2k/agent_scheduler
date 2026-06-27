# Client SDK: Agent Scheduler

How to drive the Agent Scheduler from your own service in **Python** or
**JavaScript (ES6)**. Both reference clients are thin wrappers over the REST API
described in [interface_specification.md](interface_specification.md) — same
endpoints, same models, typed errors.

Reference implementations:
- Python — [`sdk/python/agent_scheduler_client.py`](../sdk/python/agent_scheduler_client.py)
- JavaScript ES6 — [`sdk/javascript/agentSchedulerClient.js`](../sdk/javascript/agentSchedulerClient.js)

The live OpenAPI schema is always at `GET /openapi.json` (interactive docs at
`/docs`) if you'd rather generate a client.

---

## 1. Connecting

The API listens on `:6816`, localhost-bound on the host and reachable as
`http://agent-scheduler-app:6816` from inside `logus2k_network`. No auth in this
phase (internal network, fronted by nginx).

| | Python | JavaScript ES6 |
| --- | --- | --- |
| Install | `pip install httpx` | none — native `fetch` (browser, Node 18+, Deno, Bun) |
| Import | `from agent_scheduler_client import SchedulerClient` | `import { SchedulerClient } from "./agentSchedulerClient.js"` |
| Construct | `SchedulerClient("http://agent-scheduler-app:6816")` | `new SchedulerClient("http://agent-scheduler-app:6816")` |

```python
# Python
from agent_scheduler_client import SchedulerClient

with SchedulerClient("http://agent-scheduler-app:6816") as sched:
    print(sched.health())          # {'status': 'ok', 'valkey': 'up', ...}
```

```javascript
// JavaScript ES6
import { SchedulerClient } from "./agentSchedulerClient.js";

const sched = new SchedulerClient("http://agent-scheduler-app:6816");
console.log(await sched.health()); // { status: "ok", valkey: "up", ... }
```

---

## 2. Creating jobs

Three trigger types. Each has a convenience constructor; all accept the same
optional keywords (`target_stream_id`/`targetStreamId`, `event_type`/`eventType`,
`event_data`/`eventData`, `room`, `paused`).

### Interval

```python
sched.create_interval("heartbeat", seconds=30, event_data={"ping": 1})
```
```javascript
await sched.createInterval("heartbeat", { seconds: 30 }, { eventData: { ping: 1 } });
```

### Cron (standard 5-field crontab)

```python
sched.create_cron("nightly-report", "0 2 * * *", event_data={"report": "daily_sales"})
```
```javascript
await sched.createCron("nightly-report", "0 2 * * *", { eventData: { report: "daily_sales" } });
```

### Date (one-shot)

```python
from datetime import datetime, timezone
sched.create_date("year-end", datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc))
```
```javascript
await sched.createDate("year-end", new Date("2026-12-31T23:59:00Z"));
```

### Targeting a stream / room

By default events land on `stream:<job_id>`. Point a job at an explicit stream
(e.g. a shared "room") with `target_stream_id` / `targetStreamId`:

```python
sched.create_cron("ops-digest", "*/15 * * * *",
                  target_stream_id="ops-dashboard", room="ops-dashboard")
```
```javascript
await sched.createCron("ops-digest", "*/15 * * * *",
  { targetStreamId: "ops-dashboard", room: "ops-dashboard" });
```

The create response (a `JobView`) echoes `resolved_stream` so you always know
where events will appear — subscribe there before the first fire.

---

## 3. Managing jobs

| Action | Python | JavaScript |
| --- | --- | --- |
| List | `sched.list_jobs()` | `sched.listJobs()` |
| Get | `sched.get_job(id)` | `sched.getJob(id)` |
| Update | `sched.update_job(id, event_data={...})` | `sched.updateJob(id, { eventData: {...} })` |
| Pause | `sched.pause_job(id)` | `sched.pauseJob(id)` |
| Resume | `sched.resume_job(id)` | `sched.resumeJob(id)` |
| Run now | `sched.run_job(id)` | `sched.runJob(id)` |
| Delete | `sched.delete_job(id)` | `sched.deleteJob(id)` |

**Rescheduling** requires both trigger fields together:

```python
sched.update_job("heartbeat", trigger_type="interval", trigger_args={"minutes": 5})
```
```javascript
await sched.updateJob("heartbeat", { triggerType: "interval", triggerArgs: { minutes: 5 } });
```

`run_job` / `runJob` emits one event immediately, off-schedule (useful for
testing a consumer); it does not change the trigger and works even while paused.

---

## 4. `JobView` (what reads return)

```jsonc
{
  "job_id": "nightly-report",
  "trigger_type": "cron",
  "trigger": "cron[hour='2', minute='0']",
  "next_run_time": "2026-06-28T02:00:00+00:00",  // null when paused
  "resolved_stream": "stream:nightly-report",
  "event_type": "schedule.fired",
  "event_data": { "report": "daily_sales" },
  "room": null,
  "paused": false
}
```

---

## 5. Errors

Both clients raise/reject with a typed error carrying `status` and `detail`:

| HTTP | Python exception | JS class |
| --- | --- | --- |
| 404 | `JobNotFoundError` | `JobNotFoundError` |
| 409 | `JobConflictError` (duplicate `job_id`) | `JobConflictError` |
| 422 | `ValidationError` (bad trigger args / `job_id`) | `ValidationError` |
| 503 | `ServiceUnavailableError` (Valkey/job-store down) | `ServiceUnavailableError` |
| other | `SchedulerError` | `SchedulerError` |

```python
from agent_scheduler_client import JobConflictError
try:
    sched.create_interval("heartbeat", seconds=30)
except JobConflictError:
    sched.update_job("heartbeat", trigger_args={"seconds": 30}, trigger_type="interval")
```
```javascript
import { JobConflictError } from "./agentSchedulerClient.js";
try {
  await sched.createInterval("heartbeat", { seconds: 30 });
} catch (e) {
  if (e instanceof JobConflictError) { /* update instead */ }
  else throw e;
}
```

`job_id` must match `^[A-Za-z0-9._:-]+$` (it can become a stream key) — an invalid
id is a `422`.

---

## 6. Consuming the events a job emits

The SDK *manages* jobs; it does not read the resulting events. When a job fires,
the scheduler publishes one standard `EventEnvelope` to `resolved_stream` on
`valkey-bus`. Your consumer reads that stream like any other agent_bus stream
(`XREADGROUP` via your own consumer group). Shape:

```jsonc
{
  "header": {
    "stream_id": "nightly-report",
    "cid": "…uuid",                 // fresh per fire
    "sid": 1,
    "timestamp": "2026-06-28T02:00:00.041+00:00",
    "sender": "agent_scheduler",
    "event_type": "schedule.fired"  // whatever you set on the job
  },
  "payload": {
    "data": { "report": "daily_sales" },   // your event_data, verbatim
    "context": {
      "job_id": "nightly-report",
      "trigger_type": "cron",
      "fired_at": "2026-06-28T02:00:00.041+00:00",
      "scheduled_run_time": "2026-06-28T02:00:00+00:00"
    }
  },
  "metadata": { "version": "1.0", "trace_parent": null }
}
```

Two integration notes:

- **Idempotency.** `cid` is fresh on every fire, so do **not** dedup on it.
  Use `payload.context.job_id` + `payload.context.scheduled_run_time` as the
  stable key for "this logical fire."
- **Identifying scheduled events.** Filter on `header.sender == "agent_scheduler"`
  (and/or `context.job_id`), not on `event_type` — `event_type` is whatever the
  job was configured to emit and may collide with other producers' types.

---

## 7. End-to-end example (Python)

```python
from agent_scheduler_client import SchedulerClient, JobConflictError

with SchedulerClient("http://agent-scheduler-app:6816") as sched:
    try:
        view = sched.create_cron(
            job_id="nightly-report",
            cron_expression="0 2 * * *",
            event_data={"report": "daily_sales"},
        )
    except JobConflictError:
        view = sched.get_job("nightly-report")

    print("events will appear on", view["resolved_stream"])
    sched.run_job("nightly-report")          # fire once now to smoke-test the consumer
```
</content>
