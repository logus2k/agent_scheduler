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
from .models import EMIT_TASK_NAME, schedule_to_scheduled_task, to_scheduled_task
from .registry import registry

log = logging.getLogger("agent_scheduler.tasks")


async def _dedupe_first_fire(dedupe_key: str, label: str) -> bool:
    """SET NX the per-(unit, minute) idempotency key. Returns True if THIS call
    won the fire (should emit), False if a duplicate already claimed the minute.
    Fails OPEN: a dedupe glitch must never drop a fire."""
    try:
        first = await registry.redis.set(
            dedupe_key, "1", nx=True, ex=settings.dedupe_ttl_s
        )
        if not first:
            log.info("dedupe: skip duplicate fire %s", label)
            return False
    except Exception as exc:  # noqa: BLE001 - fail OPEN
        log.error("dedupe check failed (emitting anyway) %s: %s", label, exc)
    return True

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
    schedule_id: Optional[str] = None,
    job_id: Optional[str] = None,
    target_stream_id: Optional[str] = None,
    event_type: Optional[str] = None,
    event_data: Optional[dict[str, Any]] = None,
    room: Optional[str] = None,
    trigger_type: Optional[str] = None,
) -> Optional[str]:
    """Fire a schedule/job. Two modes (dispatched on the kwargs the schedule
    source built):

    * **Schedule mode** (``schedule_id`` set, no welded payload): resolve the
      schedule's bindings at fire time and emit ONE envelope per binding
      (fan-out). Zero bindings → nothing emitted. Each binding is deduped
      independently per (schedule, binding, minute).
    * **Compat/welded mode** (``job_id`` set with welded payload): emit the one
      welded event, deduped per (job, minute) — the legacy contract.

    Dedup guards Taskiq at-least-once / double-send (#296)."""
    minute = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")

    # --- Schedule mode: fan out over bindings -------------------------------
    if schedule_id is not None:
        bindings = await registry.bindings_for(schedule_id)
        if not bindings:
            # An inconsequent schedule (zero bindings) fires nothing — by design.
            log.info("schedule %s fired with 0 bindings; nothing emitted", schedule_id)
            return None
        last_entry: Optional[str] = None
        for b in bindings:
            label = f"schedule={schedule_id} binding={b.binding_id} minute={minute}"
            dedupe_key = (
                f"agent_scheduler:fired:{schedule_id}:{b.binding_id}:{minute}"
            )
            if not await _dedupe_first_fire(dedupe_key, label):
                continue
            last_entry = await emitter.emit_scheduled_event(
                job_id=b.binding_id,
                target_stream_id=b.target_stream_id,
                event_type=b.event_type,
                event_data=b.event_data,
                room=b.room,
                trigger_type=trigger_type,
            )
        return last_entry

    # --- Compat/welded mode -------------------------------------------------
    if job_id is None:
        raise ValueError("emit_trigger requires either schedule_id or job_id")
    label = f"job={job_id} minute={minute}"
    dedupe_key = f"agent_scheduler:fired:{job_id}:{minute}"
    if not await _dedupe_first_fire(dedupe_key, label):
        return None
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
        schedule_ids: set[str] = set()

        # First-class schedules (§6). Each fans out to its bindings at fire time.
        for sch in await registry.list_schedules():
            schedule_ids.add(sch.schedule_id)
            if sch.paused:
                continue
            try:
                out.append(schedule_to_scheduled_task(sch))
            except Exception as exc:  # noqa: BLE001 - one bad schedule must not blank the list
                log.error("skipping unschedulable schedule %s: %s", sch.schedule_id, exc)

        # Legacy welded jobs (compat façade). Skip any whose id was already
        # promoted to a first-class schedule (avoid a duplicate ScheduledTask).
        for jd in await registry.list():
            if jd.job_id in schedule_ids:
                continue
            if jd.paused:
                continue
            try:
                out.append(to_scheduled_task(jd))
            except Exception as exc:  # noqa: BLE001 - one bad job must not blank the list
                log.error("skipping unschedulable job %s: %s", jd.job_id, exc)
        return out


scheduler = TaskiqScheduler(broker, sources=[RegistryScheduleSource()])
