"""Trigger factory + JobCreate validation."""

import pytest
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pydantic import ValidationError

from agent_scheduler.models import JobCreate, TriggerArgs, build_trigger


def test_interval_trigger():
    t = build_trigger("interval", TriggerArgs(seconds=300))
    assert isinstance(t, IntervalTrigger)


def test_interval_requires_a_field():
    with pytest.raises(ValueError):
        build_trigger("interval", TriggerArgs())


def test_cron_trigger_from_crontab():
    t = build_trigger("cron", TriggerArgs(cron_expression="0 2 * * *"))
    assert isinstance(t, CronTrigger)


def test_cron_requires_expression():
    with pytest.raises(ValueError):
        build_trigger("cron", TriggerArgs())


def test_date_trigger():
    t = build_trigger("date", TriggerArgs(run_date="2026-12-31T23:59:00Z"))
    assert isinstance(t, DateTrigger)


def test_unknown_trigger_type():
    with pytest.raises(ValueError):
        build_trigger("nope", TriggerArgs(seconds=1))


def test_job_id_charset_validation():
    JobCreate(job_id="ok.id-1:x", trigger_type="interval", trigger_args=TriggerArgs(seconds=1))
    with pytest.raises(ValidationError):
        JobCreate(job_id="bad id!", trigger_type="interval", trigger_args=TriggerArgs(seconds=1))
