"""First-class Schedule + Binding model, mapping, migration, and fan-out tests.

Valkey-free: the model/mapping tests are pure; the fan-out test drives
``tasks.emit_trigger`` against an in-memory fake registry + a recording emitter.
"""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from agent_scheduler.models import (
    Binding,
    JobCreate,
    Schedule,
    TriggerArgs,
    job_to_schedule_and_binding,
    schedule_to_scheduled_task,
    schedule_to_view,
)


# --- Schedule model / mapping ------------------------------------------------

def _sch(**kw) -> Schedule:
    base = dict(
        schedule_id="s1",
        trigger_type="cron",
        trigger_args=TriggerArgs(cron_expression="0 7 * * *"),
    )
    base.update(kw)
    return Schedule(**base)


def test_schedule_is_payload_free():
    # A pure schedule has NO target/event fields — cron/tz/type only.
    sch = _sch()
    for forbidden in ("target_stream_id", "event_type", "event_data", "room"):
        assert not hasattr(sch, forbidden)


def test_bad_schedule_id_rejected():
    with pytest.raises(ValidationError):
        Schedule(
            schedule_id="bad id",
            trigger_type="cron",
            trigger_args=TriggerArgs(cron_expression="0 7 * * *"),
        )


def test_schedule_to_scheduled_task_carries_only_schedule_id():
    sch = _sch(
        trigger_args=TriggerArgs(cron_expression="0 7 * * *", timezone="Europe/Lisbon")
    )
    st = schedule_to_scheduled_task(sch)
    assert st.task_name == "emit_trigger"
    assert st.schedule_id == "s1"          # stable == schedule_id
    assert st.cron == "0 7 * * *"
    assert st.cron_offset == "Europe/Lisbon"
    assert st.interval is None and st.time is None
    # Fan-out happens at fire time: kwargs carry the schedule_id, NOT a payload.
    assert st.kwargs["schedule_id"] == "s1"
    assert "event_data" not in st.kwargs
    assert "target_stream_id" not in st.kwargs


def test_schedule_view_reports_binding_count():
    v = schedule_to_view(_sch(), n_bindings=3)
    assert v.schedule_id == "s1" and v.n_bindings == 3
    assert v.next_run_time is not None and v.next_run_time.hour == 7


def test_schedule_view_next_run_none_when_paused():
    v = schedule_to_view(_sch(paused=True), n_bindings=0)
    assert v.paused is True and v.next_run_time is None


# --- cron / timezone stays DST-correct --------------------------------------

def test_cron_dst_correct_lisbon():
    """0 7 * * * in Europe/Lisbon must be 07:00 LOCAL year-round: in July (WEST,
    UTC+1) that's 06:00 UTC; in January (WET, UTC+0) that's 07:00 UTC. The next
    run must be at local-07:00 whichever side of DST 'now' is."""
    from zoneinfo import ZoneInfo
    from croniter import croniter

    tz = ZoneInfo("Europe/Lisbon")
    # A summer instant (DST active).
    summer = datetime(2026, 7, 1, 0, 0, tzinfo=tz)
    nxt_summer = croniter("0 7 * * *", summer).get_next(datetime)
    assert nxt_summer.hour == 7 and nxt_summer.tzinfo is not None
    assert nxt_summer.astimezone(timezone.utc).hour == 6  # UTC+1 in summer
    # A winter instant (DST inactive).
    winter = datetime(2026, 1, 1, 0, 0, tzinfo=tz)
    nxt_winter = croniter("0 7 * * *", winter).get_next(datetime)
    assert nxt_winter.hour == 7
    assert nxt_winter.astimezone(timezone.utc).hour == 7  # UTC+0 in winter


# --- migration: welded job -> Schedule + Binding -----------------------------

def test_migration_maps_job_faithfully():
    job = JobCreate(
        job_id="news",
        trigger_type="cron",
        trigger_args=TriggerArgs(cron_expression="0 7 * * *", timezone="Europe/Lisbon"),
        target_stream_id="agent-runtime",
        event_type="schedule.fired",
        event_data={"agent": "news_curator"},
        room="ops",
        paused=True,
    )
    sch, binding = job_to_schedule_and_binding(job)

    # Schedule keeps id + trigger + paused, and NOTHING else.
    assert sch.schedule_id == "news"
    assert sch.trigger_type == "cron"
    assert sch.trigger_args.cron_expression == "0 7 * * *"
    assert sch.trigger_args.timezone == "Europe/Lisbon"
    assert sch.paused is True

    # Binding carries the full welded payload+destination faithfully.
    assert binding.schedule_id == "news"
    assert binding.target_stream_id == "agent-runtime"
    assert binding.event_type == "schedule.fired"
    assert binding.event_data == {"agent": "news_curator"}
    assert binding.room == "ops"


def test_migration_is_idempotent_on_binding_id():
    job = JobCreate(
        job_id="j",
        trigger_type="cron",
        trigger_args=TriggerArgs(cron_expression="0 7 * * *"),
    )
    _, b1 = job_to_schedule_and_binding(job)
    _, b2 = job_to_schedule_and_binding(job)
    assert b1.binding_id == b2.binding_id  # deterministic → re-run overwrites, no dupes


# --- emit_trigger fan-out (schedule mode) -----------------------------------

class _FakeRegistry:
    """Minimal registry stand-in for tasks.emit_trigger: bindings_for + a redis
    whose SET NX always wins (so nothing is deduped away)."""

    def __init__(self, bindings):
        self._bindings = bindings

        class _Redis:
            async def set(self, *a, **k):
                return True  # NX always wins → first fire

        self.redis = _Redis()

    async def bindings_for(self, schedule_id):
        return [b for b in self._bindings if b.schedule_id == schedule_id]


@pytest.fixture
def emit_env(monkeypatch):
    from agent_scheduler import emitter, tasks

    emitted: list[dict] = []

    async def _emit(**kw):
        emitted.append(kw)
        return f"entry-{len(emitted)}"

    monkeypatch.setattr(emitter, "emit_scheduled_event", _emit)

    def _set_bindings(bindings):
        monkeypatch.setattr(tasks, "registry", _FakeRegistry(bindings))

    return tasks, emitted, _set_bindings


async def test_zero_bindings_emits_nothing(emit_env):
    tasks, emitted, set_bindings = emit_env
    set_bindings([])  # inconsequent schedule
    result = await tasks.emit_trigger(schedule_id="s1", trigger_type="cron")
    assert result is None
    assert emitted == []


async def test_n_bindings_emit_n_distinct_events(emit_env):
    tasks, emitted, set_bindings = emit_env
    bindings = [
        Binding(binding_id="b1", schedule_id="s1", target_stream_id="stream-a",
                event_data={"n": 1}),
        Binding(binding_id="b2", schedule_id="s1", target_stream_id="stream-b",
                event_data={"n": 2}),
        Binding(binding_id="b3", schedule_id="s1", target_stream_id="stream-c",
                event_data={"n": 3}),
    ]
    set_bindings(bindings)
    await tasks.emit_trigger(schedule_id="s1", trigger_type="cron")

    assert len(emitted) == 3
    assert [e["target_stream_id"] for e in emitted] == ["stream-a", "stream-b", "stream-c"]
    assert [e["event_data"] for e in emitted] == [{"n": 1}, {"n": 2}, {"n": 3}]
    # each fan-out event is tagged by its own binding id
    assert {e["job_id"] for e in emitted} == {"b1", "b2", "b3"}


async def test_fan_out_ignores_other_schedules_bindings(emit_env):
    tasks, emitted, set_bindings = emit_env
    set_bindings([
        Binding(binding_id="b1", schedule_id="s1", target_stream_id="a"),
        Binding(binding_id="b2", schedule_id="OTHER", target_stream_id="b"),
    ])
    await tasks.emit_trigger(schedule_id="s1", trigger_type="cron")
    assert len(emitted) == 1 and emitted[0]["job_id"] == "b1"


async def test_dedupe_skips_second_binding_fire(monkeypatch):
    """A binding that already claimed its (schedule, binding, minute) slot is
    skipped; a fresh one still fires."""
    from agent_scheduler import emitter, tasks

    emitted: list[dict] = []

    async def _emit(**kw):
        emitted.append(kw)
        return "e"

    monkeypatch.setattr(emitter, "emit_scheduled_event", _emit)

    class _Redis:
        def __init__(self):
            self.claimed = set()

        async def set(self, key, *a, **k):
            if key in self.claimed:
                return None  # already claimed → dedupe
            self.claimed.add(key)
            return True

    class _Reg:
        redis = _Redis()

        async def bindings_for(self, sid):
            return [
                Binding(binding_id="b1", schedule_id="s1", target_stream_id="a"),
                Binding(binding_id="b2", schedule_id="s1", target_stream_id="b"),
            ]

    reg = _Reg()
    monkeypatch.setattr(tasks, "registry", reg)

    await tasks.emit_trigger(schedule_id="s1", trigger_type="cron")
    assert len(emitted) == 2  # first fire: both bindings emit
    # Same minute again: both already claimed → nothing new.
    await tasks.emit_trigger(schedule_id="s1", trigger_type="cron")
    assert len(emitted) == 2


async def test_welded_compat_mode_still_single_event(monkeypatch):
    from agent_scheduler import emitter, tasks

    emitted: list[dict] = []

    async def _emit(**kw):
        emitted.append(kw)
        return "e"

    monkeypatch.setattr(emitter, "emit_scheduled_event", _emit)

    class _Redis:
        async def set(self, *a, **k):
            return True

    class _Reg:
        redis = _Redis()

    monkeypatch.setattr(tasks, "registry", _Reg())

    await tasks.emit_trigger(
        job_id="legacy",
        target_stream_id="s",
        event_data={"x": 1},
        trigger_type="cron",
    )
    assert len(emitted) == 1 and emitted[0]["job_id"] == "legacy"


async def test_emit_trigger_requires_an_id(monkeypatch):
    from agent_scheduler import tasks

    class _Redis:
        async def set(self, *a, **k):
            return True

    class _Reg:
        redis = _Redis()

    monkeypatch.setattr(tasks, "registry", _Reg())
    with pytest.raises(ValueError):
        await tasks.emit_trigger()
