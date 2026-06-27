"""Emitter — the job callable APScheduler invokes on every fire.

``emit_scheduled_event`` is a *module-level* coroutine on purpose: RedisJobStore
pickles the job by a textual reference (``agent_scheduler.emitter:emit_scheduled_event``),
so the import path must be stable and the function must not close over any
unpicklable state (a live connection). The publisher is therefore a process
global, established once at startup via ``connect()``.

Each fire is a fresh workflow: a new ``cid`` (uuid4) and ``sid`` (INCR sid:<cid>),
``sender=agent_scheduler``, scheduling provenance in ``payload.context``.
"""

from __future__ import annotations

import contextvars
import logging
import uuid
from typing import Any, Optional

from .bus_client import Publisher
from .config import settings
from .envelope import new_event, now_iso

log = logging.getLogger("agent_scheduler.emitter")

# Process-global publisher, set in connect(); read by the (picklable) job callable.
_publisher: Optional[Publisher] = None

# Per-fire scheduled run time, set by RunTimeInjectingExecutor just before the job
# task is created (create_task captures the context, so the running coroutine sees
# the value for *its* fire). APScheduler 3.x does not pass the run time to the job,
# so we smuggle it through this contextvar instead of mutating the job.
scheduled_run_time_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "scheduled_run_time", default=None
)


async def connect() -> Publisher:
    """Establish the shared publisher (idempotent)."""
    global _publisher
    if _publisher is None:
        _publisher = await Publisher.create(settings)
    return _publisher


async def close() -> None:
    global _publisher
    if _publisher is not None:
        await _publisher.close()
        _publisher = None


def is_connected() -> bool:
    return _publisher is not None


async def ping() -> bool:
    return _publisher is not None and await _publisher.ping()


def resolve_stream_id(job_id: str, target_stream_id: Optional[str]) -> str:
    """Target id known at creation time: explicit, else derived from job_id."""
    return target_stream_id or job_id


async def emit_scheduled_event(
    *,
    job_id: str,
    target_stream_id: Optional[str] = None,
    event_type: Optional[str] = None,
    event_data: Optional[dict[str, Any]] = None,
    room: Optional[str] = None,
    trigger_type: Optional[str] = None,
    scheduled_run_time: Optional[str] = None,
) -> str:
    """Build and publish one standard EventEnvelope. Returns the stream entry id."""
    if _publisher is None:
        raise RuntimeError("publisher not connected; call connect() at startup")

    stream_id = resolve_stream_id(job_id, target_stream_id)
    stream_key = settings.stream_key(stream_id)

    cid = str(uuid.uuid4())
    sid = await _publisher.incr(f"sid:{cid}")

    # Prefer an explicit value (rare); otherwise the executor-injected one for this fire.
    if scheduled_run_time is None:
        scheduled_run_time = scheduled_run_time_var.get()

    context: dict[str, Any] = {
        "job_id": job_id,
        "fired_at": now_iso(),
    }
    if trigger_type is not None:
        context["trigger_type"] = trigger_type
    if scheduled_run_time is not None:
        context["scheduled_run_time"] = scheduled_run_time
    if room is not None:
        context["room"] = room

    env = new_event(
        stream_id=stream_id,
        cid=cid,
        sid=sid,
        sender=settings.sender_id,
        event_type=event_type or settings.default_event_type,
        data=event_data or {},
        context=context,
    )

    # Register the stream so agent_bus discovery/observers/reaper see it.
    await _publisher.sadd(settings.active_streams_key, stream_id)
    entry_id = await _publisher.publish(stream_key, env)
    log.info(
        "fired job=%s -> %s entry=%s cid=%s", job_id, stream_key, entry_id, cid
    )
    return entry_id
