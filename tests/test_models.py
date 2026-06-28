"""Trigger validation + JobDefinition -> Taskiq ScheduledTask mapping (no Valkey)."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from agent_scheduler.models import (
    JobCreate,
    TriggerArgs,
    next_run_time,
    to_scheduled_task,
    validate_trigger,
)


def _job(**kw) -> JobCreate:
    base = dict(job_id="j", trigger_type="cron",
                trigger_args=TriggerArgs(cron_expression="0 7 * * *"))
    base.update(kw)
    return JobCreate(**base)


# --- validate_trigger -------------------------------------------------------

def test_cron_ok():
    validate_trigger("cron", TriggerArgs(cron_expression="0 2 * * *"))


def test_cron_missing_expression():
    with pytest.raises(ValueError):
        validate_trigger("cron", TriggerArgs())


def test_cron_invalid_expression():
    with pytest.raises(ValueError):
        validate_trigger("cron", TriggerArgs(cron_expression="not a cron"))


def test_interval_ok():
    validate_trigger("interval", TriggerArgs(seconds=300))


def test_interval_needs_a_field():
    with pytest.raises(ValueError):
        validate_trigger("interval", TriggerArgs())


def test_date_needs_run_date():
    with pytest.raises(ValueError):
        validate_trigger("date", TriggerArgs())


def test_unknown_trigger_type():
    with pytest.raises(ValueError):
        validate_trigger("weekly", TriggerArgs())


def test_bad_timezone_rejected():
    with pytest.raises(ValidationError):
        TriggerArgs(cron_expression="0 7 * * *", timezone="Mars/Olympus")


def test_bad_job_id_rejected():
    with pytest.raises(ValidationError):
        JobCreate(job_id="bad id", trigger_type="cron",
                  trigger_args=TriggerArgs(cron_expression="0 7 * * *"))


# --- to_scheduled_task ------------------------------------------------------

def test_cron_to_scheduled_task():
    jd = _job(trigger_args=TriggerArgs(cron_expression="0 7 * * *", timezone="Europe/Lisbon"),
              target_stream_id="agent-runtime", event_data={"agent": "news"})
    st = to_scheduled_task(jd)
    assert st.task_name == "emit_trigger"
    assert st.schedule_id == "j"          # stable: == job_id
    assert st.cron == "0 7 * * *"
    assert st.cron_offset == "Europe/Lisbon"
    assert st.interval is None and st.time is None
    assert st.kwargs["job_id"] == "j"
    assert st.kwargs["target_stream_id"] == "agent-runtime"
    assert st.kwargs["event_data"] == {"agent": "news"}
    assert st.kwargs["trigger_type"] == "cron"


def test_interval_to_scheduled_task():
    jd = _job(trigger_type="interval", trigger_args=TriggerArgs(minutes=5, seconds=30))
    st = to_scheduled_task(jd)
    assert st.interval == 330 and st.cron is None


def test_date_to_scheduled_task():
    when = datetime(2030, 1, 1, 7, 0, tzinfo=timezone.utc)
    jd = _job(trigger_type="date", trigger_args=TriggerArgs(run_date=when))
    st = to_scheduled_task(jd)
    assert st.time == when and st.cron is None


# --- next_run_time ----------------------------------------------------------

def test_next_run_cron_future_in_tz():
    jd = _job(trigger_args=TriggerArgs(cron_expression="0 7 * * *", timezone="Europe/Lisbon"))
    nxt = next_run_time(jd)
    assert nxt is not None and nxt.hour == 7


def test_next_run_none_when_paused():
    jd = _job(paused=True)
    assert next_run_time(jd) is None
