"""Bus ingress — the ``schedule.requested`` consumer (the "Scheduler-bridge").

An agent publishes a ``schedule.requested`` event onto the control stream; this
loop turns it into a first-class Schedule + Binding in the registry, from which
the Taskiq schedule source picks it up on its next poll and fires it like any
other schedule. Registration is an idempotent upsert (by ``schedule_id`` /
``binding_id``), so at-least-once redelivery and crash-recovery replays are safe.

A well-formed envelope whose ``payload.data`` is not a valid ``ScheduleRequest``
(bad trigger, malformed id) is dead-lettered — loud, inspectable, and does not
wedge the group. An entry of any other ``event_type`` is acked and ignored.
"""

from __future__ import annotations

import asyncio
import logging

from pydantic import ValidationError

from .bus_consumer import Consumer, Delivery
from .config import Settings, settings as default_settings
from .envelope import EventEnvelope
from .models import (
    ScheduleRequest,
    schedule_request_to_schedule_and_binding,
    validate_trigger,
)
from .registry import registry

log = logging.getLogger("agent_scheduler.ingress")


async def handle_request(
    env: EventEnvelope,
    *,
    consumer: Consumer,
    stream: str,
    entry_id: str,
    settings: Settings = default_settings,
) -> None:
    """Process one delivered envelope: register a schedule, ignore, or dead-letter.

    Never raises for expected conditions (wrong type, invalid request) — those are
    handled here so the caller can ack unconditionally."""
    if env.header.event_type != settings.request_event_type:
        # Not ours — someone else's traffic on the stream. Ack + ignore.
        log.debug(
            "ingress: ignoring event_type=%s (cid=%s)",
            env.header.event_type,
            env.header.cid,
        )
        return

    data = env.payload.data or {}
    try:
        req = ScheduleRequest.model_validate(data)
        validate_trigger(req.trigger_type, req.trigger_args)
    except (ValidationError, ValueError) as exc:
        log.error(
            "ingress: rejecting invalid schedule.requested (cid=%s): %s",
            env.header.cid,
            exc,
        )
        await consumer.dead_letter(
            stream, entry_id, env.to_json(), f"invalid schedule.requested: {exc}"
        )
        return

    if not req.replace and await registry.schedule_exists(req.schedule_id):
        log.info(
            "ingress: schedule %s already exists and replace=false; leaving it",
            req.schedule_id,
        )
        return

    sch, binding = schedule_request_to_schedule_and_binding(req)
    await registry.put_schedule(sch)
    await registry.put_binding(binding)
    log.info(
        "ingress: registered schedule=%s binding=%s via bus (sender=%s cid=%s)",
        sch.schedule_id,
        binding.binding_id,
        env.header.sender,
        env.header.cid,
    )


async def _process(consumer: Consumer, d: Delivery, group: str) -> None:
    """Handle one delivery and ack it unconditionally (at-least-once; the handler
    is idempotent, so a redelivery just upserts the same schedule)."""
    try:
        await handle_request(
            d.envelope, consumer=consumer, stream=d.stream, entry_id=d.entry_id
        )
    except Exception:  # noqa: BLE001 - a handler crash must not wedge the group
        log.exception("ingress: handler crashed on entry %s (acking anyway)", d.entry_id)
    finally:
        await consumer.ack(d.stream, group, [d.entry_id])


async def run_ingress_loop(
    consumer: Consumer,
    *,
    stream: str,
    group: str,
    consumer_name: str,
    block_ms: int,
    count: int,
    reclaim_min_idle_ms: int,
    stop: asyncio.Event,
) -> None:
    """Reclaim any crash-pending requests once, then tail the control stream until
    ``stop`` is set (or the task is cancelled at shutdown)."""
    log.info(
        "ingress loop starting on %s (group=%s consumer=%s)",
        stream,
        group,
        consumer_name,
    )
    # Startup crash-recovery: re-handle entries this consumer group left pending.
    try:
        _cursor, pending = await consumer.reclaim(
            stream, group, consumer_name, reclaim_min_idle_ms
        )
        for d in pending:
            await _process(consumer, d, group)
        if pending:
            log.info("ingress: reclaimed %d pending request(s) at startup", len(pending))
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        log.exception("ingress: startup reclaim failed (continuing)")

    while not stop.is_set():
        try:
            deliveries = await consumer.read_group(
                [stream], group, consumer_name, count=count, block_ms=block_ms
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - transient read error; back off and retry
            log.exception("ingress: read failed; backing off 1s")
            await asyncio.sleep(1)
            continue
        for d in deliveries:
            await _process(consumer, d, group)

    log.info("ingress loop stopped")
