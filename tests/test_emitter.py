"""emit_scheduled_event builds a correct envelope (with a fake publisher)."""

import pytest

from agent_scheduler import emitter
from agent_scheduler.config import settings
from agent_scheduler.envelope import EventEnvelope


class FakePublisher:
    def __init__(self):
        self.published: list[tuple[str, EventEnvelope]] = []
        self.sadds: list[tuple[str, str]] = []
        self._counter = 0

    async def incr(self, key: str) -> int:
        self._counter += 1
        return self._counter

    async def sadd(self, key: str, member: str) -> None:
        self.sadds.append((key, member))

    async def publish(self, stream: str, env: EventEnvelope) -> str:
        self.published.append((stream, env))
        return "1-0"

    async def ping(self) -> bool:
        return True


@pytest.fixture
def fake_publisher(monkeypatch):
    fake = FakePublisher()
    monkeypatch.setattr(emitter, "_publisher", fake)
    return fake


async def test_emit_derived_stream(fake_publisher):
    await emitter.emit_scheduled_event(
        job_id="nightly-report",
        target_stream_id=None,
        event_type="schedule.fired",
        event_data={"report": "daily_sales"},
        trigger_type="cron",
    )
    stream, env = fake_publisher.published[0]
    assert stream == "stream:nightly-report"
    assert env.header.sender == settings.sender_id
    assert env.header.event_type == "schedule.fired"
    assert env.header.stream_id == "nightly-report"
    assert env.header.sid == 1
    assert env.payload.data == {"report": "daily_sales"}
    assert env.payload.context["job_id"] == "nightly-report"
    assert env.payload.context["trigger_type"] == "cron"
    assert "fired_at" in env.payload.context
    # Stream registered for discovery.
    assert (settings.active_streams_key, "nightly-report") in fake_publisher.sadds


async def test_emit_explicit_target_and_room(fake_publisher):
    await emitter.emit_scheduled_event(
        job_id="j1",
        target_stream_id="ops-dashboard",
        event_data={},
        room="ops-dashboard",
    )
    stream, env = fake_publisher.published[0]
    assert stream == "stream:ops-dashboard"
    assert env.header.stream_id == "ops-dashboard"
    assert env.payload.context["room"] == "ops-dashboard"


async def test_emit_without_publisher_raises(monkeypatch):
    monkeypatch.setattr(emitter, "_publisher", None)
    with pytest.raises(RuntimeError):
        await emitter.emit_scheduled_event(job_id="j", event_data={})
