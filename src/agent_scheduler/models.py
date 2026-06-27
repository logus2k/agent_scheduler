"""Pydantic models + the APScheduler trigger factory.

The API contract: JobCreate (request) / JobUpdate (PATCH) / JobView (response),
plus pure helpers to turn a request into an APScheduler trigger and an
APScheduler Job back into a JobView.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Literal, Optional

from apscheduler.job import Job
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pydantic import BaseModel, Field, field_validator

from .config import settings
from .emitter import resolve_stream_id

# job_id may become part of a stream key, so constrain its charset.
JOB_ID_RE = re.compile(r"^[A-Za-z0-9._:-]+$")

TriggerType = Literal["interval", "cron", "date"]
_INTERVAL_FIELDS = ("seconds", "minutes", "hours", "days", "weeks")


class TriggerArgs(BaseModel):
    # interval
    weeks: Optional[int] = None
    days: Optional[int] = None
    hours: Optional[int] = None
    minutes: Optional[int] = None
    seconds: Optional[int] = None
    # cron
    cron_expression: Optional[str] = None
    # date
    run_date: Optional[datetime] = None


class JobCreate(BaseModel):
    job_id: str
    trigger_type: TriggerType
    trigger_args: TriggerArgs
    target_stream_id: Optional[str] = None
    event_type: str = Field(default_factory=lambda: settings.default_event_type)
    event_data: dict[str, Any] = Field(default_factory=dict)
    room: Optional[str] = None
    paused: bool = False

    @field_validator("job_id")
    @classmethod
    def _valid_job_id(cls, v: str) -> str:
        if not JOB_ID_RE.match(v):
            raise ValueError(
                "job_id must match ^[A-Za-z0-9._:-]+$ (it can become a stream key)"
            )
        return v


class JobUpdate(BaseModel):
    trigger_type: Optional[TriggerType] = None
    trigger_args: Optional[TriggerArgs] = None
    target_stream_id: Optional[str] = None
    event_type: Optional[str] = None
    event_data: Optional[dict[str, Any]] = None
    room: Optional[str] = None


class JobView(BaseModel):
    job_id: str
    trigger_type: str
    trigger: str
    next_run_time: Optional[datetime]
    resolved_stream: str
    event_type: str
    event_data: dict[str, Any]
    room: Optional[str]
    paused: bool


def build_trigger(trigger_type: str, args: TriggerArgs):
    """Translate a (trigger_type, args) pair into an APScheduler trigger."""
    if trigger_type == "interval":
        kwargs = {f: getattr(args, f) for f in _INTERVAL_FIELDS if getattr(args, f)}
        if not kwargs:
            raise ValueError(
                "interval trigger needs at least one of "
                f"{_INTERVAL_FIELDS} to be > 0"
            )
        return IntervalTrigger(**kwargs)
    if trigger_type == "cron":
        if not args.cron_expression:
            raise ValueError("cron trigger needs 'cron_expression'")
        return CronTrigger.from_crontab(args.cron_expression)
    if trigger_type == "date":
        if not args.run_date:
            raise ValueError("date trigger needs 'run_date'")
        return DateTrigger(run_date=args.run_date)
    raise ValueError(f"unknown trigger_type: {trigger_type}")


def _trigger_type_of(job: Job) -> str:
    name = type(job.trigger).__name__
    return {
        "IntervalTrigger": "interval",
        "CronTrigger": "cron",
        "DateTrigger": "date",
    }.get(name, name)


def job_to_view(job: Job) -> JobView:
    """Map an APScheduler Job (with our kwargs) to the API's JobView."""
    kw = job.kwargs or {}
    job_id = kw.get("job_id", job.id)
    return JobView(
        job_id=job_id,
        trigger_type=_trigger_type_of(job),
        trigger=str(job.trigger),
        next_run_time=job.next_run_time,  # None when paused
        resolved_stream=settings.stream_key(
            resolve_stream_id(job_id, kw.get("target_stream_id"))
        ),
        event_type=kw.get("event_type") or settings.default_event_type,
        event_data=kw.get("event_data") or {},
        room=kw.get("room"),
        paused=job.next_run_time is None,
    )
