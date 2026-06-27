# Interface Specification: Agent Scheduler API

This document defines the RESTful interface for the `scheduler-admin-api`, designed to manage `agent_scheduler` tasks via Valkey.

---

## 1. Overview

The API provides a standard CRUD interface to manage persistent jobs. Since the `agent_scheduler` uses `APScheduler` with a `RedisJobStore`, all changes made through this API are immediately reflected in the Valkey state, ensuring consistency across the `logus2k-network`.

---

## 2. API Endpoints

| Method | Endpoint | Description |
| --- | --- | --- |
| **GET** | `/jobs` | List all registered scheduled jobs. |
| **GET** | `/jobs/{job_id}` | Retrieve details of a specific job. |
| **POST** | `/jobs` | Create a new interval, cron, or one-time job. |
| **DELETE** | `/jobs/{job_id}` | Remove a job from the scheduler. |
| **POST** | `/jobs/{job_id}/pause` | Pause a running job. |
| **POST** | `/jobs/{job_id}/resume` | Resume a paused job. |

---

## 3. Data Models (Pydantic)

### `JobCreate` (Request Body)

Used for creating new scheduling triggers.

```json
{
  "job_id": "string",
  "trigger_type": "interval | cron | date",
  "trigger_args": {
    "seconds": "int (for interval)",
    "cron_expression": "string (for cron, e.g., '*/5 * * * *')",
    "run_date": "iso8601 (for date)"
  },
  "event_payload": {
    "target_agent": "string",
    "event_data": "object"
  }
}

```

---

## 4. Implementation Details

### Dependency Injection

The API will use FastAPI's dependency injection to provide the `AsyncIOScheduler` instance.

```python
from fastapi import FastAPI, Depends
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Dependency to provide the scheduler instance
def get_scheduler() -> AsyncIOScheduler:
    return scheduler 

```

### Lifespan Management

To ensure the `agent_scheduler` and the Admin API remain in sync, the scheduler instance is initialized once at startup and persisted in the Valkey `RedisJobStore`.

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize Valkey-backed scheduler
    scheduler.start()
    yield
    # Shutdown: Graceful stop
    scheduler.shutdown()

```

---

## 5. Technical Architecture Integration

### Communication Flow

1. **User/Dashboard** calls `POST /jobs` on the `scheduler-admin-api`.
2. **API** updates the Valkey key `apscheduler.jobs`.
3. **`agent_scheduler` container** (which polls Valkey) detects the change in the `RedisJobStore` and registers the new job in its internal memory.
4. **Execution:** When the trigger fires, `agent_scheduler` executes the task, which publishes an `EventEnvelope` to the `valkey-bus` stream.

### Deployment Path

* **API Service:** Accessible via internal `logus2k-network` DNS at `http://scheduler-admin-api:8000`.
* **Scheduler Service:** Acts as the background worker, continuously monitoring the Valkey state persisted in `valkey-bus`.
