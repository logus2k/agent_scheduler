"""Ingress handler — maps schedule.requested to a Schedule + Binding, Valkey-free.

We drive ``handle_request`` directly with a fake consumer (records dead-letters)
and a monkeypatched registry (in-memory), mirroring test_api's approach.
"""

import pytest

from agent_scheduler import ingress
from agent_scheduler.config import settings
from agent_scheduler.envelope import new_event
from agent_scheduler.models import Binding, Schedule


class FakeConsumer:
    def __init__(self):
        self.dead_letters = []

    async def dead_letter(self, stream, entry_id, fields, error):
        self.dead_letters.append((stream, entry_id, fields, error))


@pytest.fixture
def reg(monkeypatch):
    """In-memory stand-in for the process-global registry used by ingress."""
    schedules: dict[str, Schedule] = {}
    bindings: dict[str, Binding] = {}

    async def put_schedule(sch):
        schedules[sch.schedule_id] = sch

    async def put_binding(b):
        bindings[b.binding_id] = b

    async def schedule_exists(sid):
        return sid in schedules

    from agent_scheduler.registry import registry as real

    monkeypatch.setattr(real, "put_schedule", put_schedule)
    monkeypatch.setattr(real, "put_binding", put_binding)
    monkeypatch.setattr(real, "schedule_exists", schedule_exists)
    return schedules, bindings


def _request_env(data: dict, event_type: str = "schedule.requested"):
    return new_event(
        stream_id="schedule-requests",
        cid="cid-1",
        sid=1,
        sender="test-agent",
        event_type=event_type,
        data=data,
    )


@pytest.mark.asyncio
async def test_valid_request_registers_schedule_and_binding(reg):
    schedules, bindings = reg
    consumer = FakeConsumer()
    env = _request_env(
        {
            "schedule_id": "cv.release.sense",
            "trigger_type": "cron",
            "trigger_args": {"cron_expression": "0 3 * * *", "timezone": "Europe/Lisbon"},
            "event_type": "sense.tick",
            "event_data": {"source": "release", "scope": "cv"},
            "target_stream_id": "cv",
        }
    )

    await ingress.handle_request(env, consumer=consumer, stream="stream:schedule-requests", entry_id="1-0")

    assert "cv.release.sense" in schedules
    sch = schedules["cv.release.sense"]
    assert sch.trigger_type == "cron"
    assert sch.trigger_args.cron_expression == "0 3 * * *"
    # One derived binding <schedule_id>:b0 carrying the emit action.
    assert "cv.release.sense:b0" in bindings
    b = bindings["cv.release.sense:b0"]
    assert b.schedule_id == "cv.release.sense"
    assert b.event_type == "sense.tick"
    assert b.event_data == {"source": "release", "scope": "cv"}
    assert b.target_stream_id == "cv"
    assert not consumer.dead_letters


@pytest.mark.asyncio
async def test_wrong_event_type_is_ignored(reg):
    schedules, bindings = reg
    consumer = FakeConsumer()
    env = _request_env(
        {
            "schedule_id": "x",
            "trigger_type": "cron",
            "trigger_args": {"cron_expression": "0 3 * * *"},
            "event_type": "sense.tick",
        },
        event_type="maturity.updated",  # not ours
    )

    await ingress.handle_request(env, consumer=consumer, stream="s", entry_id="1-0")

    assert not schedules and not bindings
    assert not consumer.dead_letters


@pytest.mark.asyncio
async def test_invalid_trigger_is_dead_lettered(reg):
    schedules, bindings = reg
    consumer = FakeConsumer()
    env = _request_env(
        {
            "schedule_id": "bad",
            "trigger_type": "cron",
            "trigger_args": {},  # cron with no cron_expression -> invalid
            "event_type": "sense.tick",
        }
    )

    await ingress.handle_request(env, consumer=consumer, stream="s", entry_id="9-0")

    assert not schedules and not bindings
    assert len(consumer.dead_letters) == 1
    assert consumer.dead_letters[0][1] == "9-0"


@pytest.mark.asyncio
async def test_malformed_payload_is_dead_lettered(reg):
    schedules, bindings = reg
    consumer = FakeConsumer()
    # Missing required fields (trigger_type / trigger_args) -> ValidationError.
    env = _request_env({"schedule_id": "nope"})

    await ingress.handle_request(env, consumer=consumer, stream="s", entry_id="7-0")

    assert not schedules and not bindings
    assert len(consumer.dead_letters) == 1


@pytest.mark.asyncio
async def test_replace_false_leaves_existing(reg):
    schedules, bindings = reg
    consumer = FakeConsumer()
    schedules["dup"] = Schedule(
        schedule_id="dup", trigger_type="cron", trigger_args={"cron_expression": "0 1 * * *"}
    )
    env = _request_env(
        {
            "schedule_id": "dup",
            "trigger_type": "cron",
            "trigger_args": {"cron_expression": "0 9 * * *"},  # different
            "event_type": "sense.tick",
            "replace": False,
        }
    )

    await ingress.handle_request(env, consumer=consumer, stream="s", entry_id="3-0")

    # Untouched: still the original cron, no binding written.
    assert schedules["dup"].trigger_args.cron_expression == "0 1 * * *"
    assert not bindings
    assert not consumer.dead_letters
