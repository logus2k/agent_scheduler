"""Pydantic models + the Taskiq schedule mapping.

The API contract is unchanged: ``JobCreate`` (request) / ``JobUpdate`` (PATCH) /
``JobView`` (response). A stored job is just a ``JobCreate`` (the registry persists
it verbatim). Helpers map a job to a Taskiq ``ScheduledTask`` (the schedule the
scheduler reads) and to a ``JobView`` (with a computed next-run for the admin UI).
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional
from zoneinfo import ZoneInfo

from croniter import croniter
from pydantic import BaseModel, Field, field_validator
from taskiq.scheduler.scheduled_task import ScheduledTask

from .config import settings
from .emitter import resolve_stream_id

# job_id may become part of a stream key, so constrain its charset.
JOB_ID_RE = re.compile(r"^[A-Za-z0-9._:-]+$")

TriggerType = Literal["interval", "cron", "date"]
_INTERVAL_FIELDS = ("seconds", "minutes", "hours", "days", "weeks")
_INTERVAL_SECONDS = {"seconds": 1, "minutes": 60, "hours": 3600, "days": 86400, "weeks": 604800}

# The single Taskiq task every schedule points at (defined in tasks.py).
EMIT_TASK_NAME = "emit_trigger"


class TriggerArgs(BaseModel):
    # interval
    weeks: Optional[int] = None
    days: Optional[int] = None
    hours: Optional[int] = None
    minutes: Optional[int] = None
    seconds: Optional[int] = None
    # cron
    cron_expression: Optional[str] = None
    # IANA timezone name (e.g. "Europe/Lisbon") the cron is interpreted in.
    # Omit to use UTC. DST-correct when set.
    timezone: Optional[str] = None
    # date
    run_date: Optional[datetime] = None

    @field_validator("timezone")
    @classmethod
    def _valid_timezone(cls, v: Optional[str]) -> Optional[str]:
        if v:
            try:
                ZoneInfo(v)
            except Exception as exc:  # noqa: BLE001
                raise ValueError(f"unknown IANA timezone {v!r}: {exc}") from exc
        return v


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


# A stored job definition is exactly a JobCreate (persisted verbatim in the registry).
JobDefinition = JobCreate


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


# --- mapping: JobDefinition -> Taskiq ScheduledTask --------------------------

def _interval_seconds(args: TriggerArgs) -> int:
    return sum(_INTERVAL_SECONDS[f] * (getattr(args, f) or 0) for f in _INTERVAL_FIELDS)


def validate_trigger(trigger_type: str, args: TriggerArgs) -> None:
    """Reject a malformed trigger at create/update time (mirrors the old contract)."""
    if trigger_type == "cron":
        if not args.cron_expression:
            raise ValueError("cron trigger needs 'cron_expression'")
        try:
            croniter(args.cron_expression)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"invalid cron_expression: {exc}") from exc
    elif trigger_type == "interval":
        if _interval_seconds(args) < 1:
            raise ValueError(
                f"interval trigger needs at least one of {_INTERVAL_FIELDS} (>= 1s total)"
            )
    elif trigger_type == "date":
        if not args.run_date:
            raise ValueError("date trigger needs 'run_date'")
    else:
        raise ValueError(f"unknown trigger_type: {trigger_type}")


def to_scheduled_task(jd: JobDefinition) -> ScheduledTask:
    """Map a job definition to the Taskiq ScheduledTask the scheduler evaluates."""
    common: dict[str, Any] = {
        "task_name": EMIT_TASK_NAME,
        "labels": {},
        "args": [],
        "schedule_id": jd.job_id,
        "kwargs": {
            "job_id": jd.job_id,
            "target_stream_id": jd.target_stream_id,
            "event_type": jd.event_type,
            "event_data": jd.event_data,
            "room": jd.room,
            "trigger_type": jd.trigger_type,
        },
    }
    ta = jd.trigger_args
    if jd.trigger_type == "cron":
        return ScheduledTask(**common, cron=ta.cron_expression, cron_offset=ta.timezone)
    if jd.trigger_type == "interval":
        return ScheduledTask(**common, interval=_interval_seconds(ta))
    if jd.trigger_type == "date":
        return ScheduledTask(**common, time=ta.run_date)
    raise ValueError(f"unknown trigger_type: {jd.trigger_type}")


# --- mapping: JobDefinition -> JobView ---------------------------------------

def _trigger_str(jd: JobDefinition) -> str:
    ta = jd.trigger_args
    if jd.trigger_type == "cron":
        tz = f" {ta.timezone}" if ta.timezone else " UTC"
        return f"cron[{ta.cron_expression}]{tz}"
    if jd.trigger_type == "interval":
        return f"interval[{_interval_seconds(ta)}s]"
    if jd.trigger_type == "date":
        return f"date[{ta.run_date.isoformat() if ta.run_date else '?'}]"
    return jd.trigger_type


def next_run_time(jd: JobDefinition) -> Optional[datetime]:
    """Best-effort next fire time for the admin view (None when paused)."""
    if jd.paused:
        return None
    ta = jd.trigger_args
    if jd.trigger_type == "cron" and ta.cron_expression:
        tz = ZoneInfo(ta.timezone) if ta.timezone else timezone.utc
        now = datetime.now(tz)
        try:
            return croniter(ta.cron_expression, now).get_next(datetime)
        except Exception:  # noqa: BLE001
            return None
    if jd.trigger_type == "interval":
        secs = _interval_seconds(ta)
        return datetime.now(timezone.utc) + timedelta(seconds=secs) if secs else None
    if jd.trigger_type == "date":
        return ta.run_date
    return None


def job_def_to_view(jd: JobDefinition) -> JobView:
    return JobView(
        job_id=jd.job_id,
        trigger_type=jd.trigger_type,
        trigger=_trigger_str(jd),
        next_run_time=next_run_time(jd),
        resolved_stream=settings.stream_key(
            resolve_stream_id(jd.job_id, jd.target_stream_id)
        ),
        event_type=jd.event_type or settings.default_event_type,
        event_data=jd.event_data or {},
        room=jd.room,
        paused=jd.paused,
    )
