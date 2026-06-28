"""Taskiq wiring — broker, the emit task, the registry-backed schedule source,
and the scheduler.

Replaces APScheduler. The Taskiq scheduler runs a **per-second, wall-clock
re-anchored** loop (it re-reads ``datetime.now()`` every tick), so the host's
slow monotonic clock can't accumulate drift the way APScheduler's single long
sleep did. The schedule source is derived from the job registry (one source of
truth); the task reuses the existing envelope/Publisher to emit onto the bus.

Embedded model: the broker, worker (``run_receiver_task``) and scheduler
(``run_scheduler_task``) all run in the FastAPI process (see api.py lifespan).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from taskiq import ScheduleSource, TaskiqEvents, TaskiqScheduler, TaskiqState
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq_redis import RedisStreamBroker

from . import emitter
from .config import settings
from .models import EMIT_TASK_NAME, to_scheduled_task
from .registry import registry

log = logging.getLogger("agent_scheduler.tasks")

# RedisStreamBroker supports acks → durable. Default result backend is the no-op
# DummyResultBackend (we don't need task results — fire-and-forget).
broker = RedisStreamBroker(url=settings.redis_url(), queue_name=settings.taskiq_queue)


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def _worker_startup(_state: TaskiqState) -> None:
    """Make the worker self-sufficient (also covers a separate `taskiq worker`)."""
    await registry.connect()
    await emitter.connect()


@broker.task(task_name=EMIT_TASK_NAME)
async def emit_trigger(
    *,
    job_id: str,
    target_stream_id: Optional[str] = None,
    event_type: Optional[str] = None,
    event_data: Optional[dict[str, Any]] = None,
    room: Optional[str] = None,
    trigger_type: Optional[str] = None,
) -> Optional[str]:
    """Publish one schedule.fired envelope onto the bus, guarded so each
    (job, minute) fires exactly once (covers Taskiq at-least-once / #296)."""
    minute = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
    dedupe_key = f"agent_scheduler:fired:{job_id}:{minute}"
    try:
        first = await registry.redis.set(
            dedupe_key, "1", nx=True, ex=settings.dedupe_ttl_s
        )
        if not first:
            log.info("dedupe: skip duplicate fire job=%s minute=%s", job_id, minute)
            return None
    except Exception as exc:  # noqa: BLE001 - fail OPEN: never drop a fire over a dedupe glitch
        log.error("dedupe check failed (emitting anyway) job=%s: %s", job_id, exc)

    return await emitter.emit_scheduled_event(
        job_id=job_id,
        target_stream_id=target_stream_id,
        event_type=event_type,
        event_data=event_data,
        room=room,
        trigger_type=trigger_type,
    )


class RegistryScheduleSource(ScheduleSource):
    """Derives the live schedule list from the job registry on each scheduler poll.

    One source of truth: add/delete/pause in the registry is reflected on the next
    poll (``update_interval``, default 60 s) — `run-now` (direct emit) covers the
    immediate case. ``schedule_id == job_id`` keeps the scheduler's per-minute
    dedup memory stable across polls.
    """

    async def startup(self) -> None:
        await registry.connect()

    async def shutdown(self) -> None:  # registry lifecycle owned by the app lifespan
        pass

    async def get_schedules(self) -> list[ScheduledTask]:
        out: list[ScheduledTask] = []
        for jd in await registry.list():
            if jd.paused:
                continue
            try:
                out.append(to_scheduled_task(jd))
            except Exception as exc:  # noqa: BLE001 - one bad job must not blank the list
                log.error("skipping unschedulable job %s: %s", jd.job_id, exc)
        return out


scheduler = TaskiqScheduler(broker, sources=[RegistryScheduleSource()])
