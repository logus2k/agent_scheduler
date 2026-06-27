# Technical Architecture: Reactive Agent Scheduler

This architecture defines the `agent_scheduler` as a dedicated, containerized microservice that manages stateful execution triggers for the system. It leverages **APScheduler** backed by **Valkey** to provide a persistent, fault-tolerant scheduling layer that functions as an autonomous component within the `logus2k-network`.

---

## 1. System Overview

The `agent_scheduler` acts as a "Trigger Actor." Instead of executing heavy logic, its primary responsibility is to emit events into the central `valkey-bus` at precisely defined intervals or timestamps.

* **Scheduler Engine:** APScheduler (Python).
* **State Backend:** Valkey (via `RedisJobStore`).
* **Communication:** `logus2k-network` (Docker bridge).
* **Interface:** FastAPI Admin API (for CRUD operations on schedules).

---

## 2. Component Architecture

### The `agent_scheduler` Container

The scheduler runs as an isolated process. It does not contain business logic; instead, it triggers predefined "Hooks" or emits events onto the Valkey Stream, which other agents then consume.

* **Job Store:** The `RedisJobStore` connects to `valkey-bus:6379`. All job metadata (triggers, next run time, task IDs) is stored in Valkey, ensuring the scheduler is stateless and restart-proof.
* **Decoupled Triggers:** The scheduler emits a structured event payload (conforming to the existing `EventEnvelope`) into the event bus, allowing other agents to react to the scheduled event.

### The Admin API (FastAPI)

A separate container provides a REST API to manage the scheduler's state.

* **Endpoints:**
* `GET /scheduler/jobs`: List all registered triggers.
* `POST /scheduler/jobs`: Create/register a new interval or cron job.
* `DELETE /scheduler/jobs/{job_id}`: Stop a specific agent task.
* `PATCH /scheduler/jobs/{job_id}`: Update trigger settings dynamically.



---

## 3. Deployment Configuration (`docker-compose.yml`)

```yaml
services:
  valkey-bus:
    image: valkey/valkey:9.1.0-alpine3.23
    networks:
      - logus2k-network
    volumes:
      - valkey_data:/data
    command: ["valkey-server", "--appendonly", "yes", "--appendfsync", "everysec"]

  scheduler-admin-api:
    image: python:3.12-alpine
    networks:
      - logus2k-network
    environment:
      - VALKEY_HOST=valkey-bus
    depends_on:
      - valkey-bus

  agent_scheduler:
    image: python:3.12-alpine
    container_name: agent_scheduler
    networks:
      - logus2k-network
    environment:
      - VALKEY_HOST=valkey-bus
    depends_on:
      - valkey-bus
    restart: unless-stopped

networks:
  logus2k-network:
    external: true

volumes:
  valkey_data:
    driver: local

```

---

## 4. Operational Logic & Data Flow

1. **Persistence:** All job configurations are persisted in Valkey. If the `agent_scheduler` container restarts, it reconnects to Valkey, reads the existing `RedisJobStore` state, and resumes pending tasks without user intervention.
2. **Concurrency:** Since jobs are stored in Valkey using atomic operations, you can scale the number of scheduler instances if necessary (though unnecessary for light workloads).
3. **Observability:** The FastAPI Admin API queries the same Valkey keys used by the `agent_scheduler`, providing real-time visibility into the "State of the Schedule."

---

## 5. Implementation Checklist

* [ ] **Data Layer:** Define the Pydantic schema for `JobConfiguration` (handling cron, interval, and date triggers).
* [ ] **API Layer:** Implement FastAPI routes to interact with the `RedisJobStore`.
* [ ] **Event Emission:** Ensure the `agent_scheduler` task function uses a standard `EventBus` client to `XADD` events into the main choreography stream.
* [ ] **Health Check:** Implement a `/health` endpoint in the Admin API to verify connectivity to the `valkey-bus`.
