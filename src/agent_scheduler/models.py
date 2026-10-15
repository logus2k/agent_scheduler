"""Pydantic models + the Taskiq schedule mapping.

Two shapes coexist during the §6 migration:

* **First-class Schedule + Bindings** (the target model). A ``Schedule`` is
  cron/timezone/type **only** — it knows nothing about what it fires. A
  ``Binding`` (schedule_id → action) carries the payload+destination
  (target_stream_id / event_type / event_data / room). One schedule fans out
  to many bindings; a schedule with zero bindings is inconsequent.
* **Welded Job** (``JobCreate`` / ``JobUpdate`` / ``JobView``) — the legacy
  contract kept as a **compat façade**. A stored job is just a ``JobCreate``.
  ``job_to_schedule_and_binding`` maps one welded job → one Schedule + one
  Binding for the migration.

Helpers map a schedule to the Taskiq ``ScheduledTask`` the scheduler evaluates
and to a ``*View`` (with a computed next-run for the admin UI).
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

# job_id / schedule_id / binding_id may become part of a stream key or a Redis
# hash field, so constrain their charset.
JOB_ID_RE = re.compile(r"^[A-Za-z0-9._:-]+$")
ID_RE = JOB_ID_RE

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
    # Raw trigger fields (cron_expression/timezone, interval parts, run_date) so
    # an admin UI can pre-fill an edit form; `trigger` is the human-readable form.
    trigger_args: dict[str, Any]
    next_run_time: Optional[datetime]
    target_stream_id: Optional[str]
    resolved_stream: str
    event_type: str
    event_data: dict[str, Any]
    room: Optional[str]
    paused: bool


# --- First-class Schedule + Bindings (the §6 target model) -------------------

class Schedule(BaseModel):
    """A pure schedule: cron/timezone/type ONLY. It knows nothing about what it
    fires. Consequential only once one or more Bindings point at it."""

    schedule_id: str
    trigger_type: TriggerType
    trigger_args: TriggerArgs
    paused: bool = False

    @field_validator("schedule_id")
    @classmethod
    def _valid_schedule_id(cls, v: str) -> str:
        if not ID_RE.match(v):
            raise ValueError(
                "schedule_id must match ^[A-Za-z0-9._:-]+$ (it becomes a hash field / stream key)"
            )
        return v


class ScheduleCreate(BaseModel):
    schedule_id: str
    trigger_type: TriggerType
    trigger_args: TriggerArgs
    paused: bool = False

    @field_validator("schedule_id")
    @classmethod
    def _valid_schedule_id(cls, v: str) -> str:
        if not ID_RE.match(v):
            raise ValueError(
                "schedule_id must match ^[A-Za-z0-9._:-]+$ (it becomes a hash field / stream key)"
            )
        return v


class ScheduleUpdate(BaseModel):
    trigger_type: Optional[TriggerType] = None
    trigger_args: Optional[TriggerArgs] = None


class ScheduleView(BaseModel):
    schedule_id: str
    trigger_type: str
    trigger: str
    trigger_args: dict[str, Any]
    next_run_time: Optional[datetime]
    paused: bool
    n_bindings: int


class Binding(BaseModel):
    """A (schedule → action) link. Carries the payload+destination that a fire
    of ``schedule_id`` should emit. Many bindings may share one schedule."""

    binding_id: str
    schedule_id: str
    target_stream_id: Optional[str] = None
    event_type: str = Field(default_factory=lambda: settings.default_event_type)
    event_data: dict[str, Any] = Field(default_factory=dict)
    room: Optional[str] = None

    @field_validator("binding_id", "schedule_id")
    @classmethod
    def _valid_id(cls, v: str) -> str:
        if not ID_RE.match(v):
            raise ValueError("binding_id/schedule_id must match ^[A-Za-z0-9._:-]+$")
        return v


class BindingCreate(BaseModel):
    binding_id: str
    target_stream_id: Optional[str] = None
    event_type: str = Field(default_factory=lambda: settings.default_event_type)
    event_data: dict[str, Any] = Field(default_factory=dict)
    room: Optional[str] = None

    @field_validator("binding_id")
    @classmethod
    def _valid_id(cls, v: str) -> str:
        if not ID_RE.match(v):
            raise ValueError("binding_id must match ^[A-Za-z0-9._:-]+$")
        return v


class BindingUpdate(BaseModel):
    target_stream_id: Optional[str] = None
    event_type: Optional[str] = None
    event_data: Optional[dict[str, Any]] = None
    room: Optional[str] = None


class BindingView(BaseModel):
    binding_id: str
    schedule_id: str
    target_stream_id: Optional[str]
    resolved_stream: str
    event_type: str
    event_data: dict[str, Any]
    room: Optional[str]


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


def _scheduled_task(
    schedule_key: str, trigger_type: str, ta: TriggerArgs, kwargs: dict[str, Any]
) -> ScheduledTask:
    """Build the Taskiq ScheduledTask for a (trigger_type, trigger_args), pointing
    the emit task at ``schedule_key`` and carrying ``kwargs``.

    ``schedule_key == schedule_id`` (or ``job_id`` on the compat path) keeps the
    scheduler's per-minute dedup memory stable across polls."""
    common: dict[str, Any] = {
        "task_name": EMIT_TASK_NAME,
        "labels": {},
        "args": [],
        "schedule_id": schedule_key,
        "kwargs": kwargs,
    }
    if trigger_type == "cron":
        return ScheduledTask(**common, cron=ta.cron_expression, cron_offset=ta.timezone)
    if trigger_type == "interval":
        return ScheduledTask(**common, interval=_interval_seconds(ta))
    if trigger_type == "date":
        return ScheduledTask(**common, time=ta.run_date)
    raise ValueError(f"unknown trigger_type: {trigger_type}")


def to_scheduled_task(jd: JobDefinition) -> ScheduledTask:
    """Compat path: map a welded job definition to a Taskiq ScheduledTask that
    carries the job's welded payload+destination in kwargs (single event)."""
    return _scheduled_task(
        jd.job_id,
        jd.trigger_type,
        jd.trigger_args,
        {
            "job_id": jd.job_id,
            "target_stream_id": jd.target_stream_id,
            "event_type": jd.event_type,
            "event_data": jd.event_data,
            "room": jd.room,
            "trigger_type": jd.trigger_type,
        },
    )


def schedule_to_scheduled_task(sch: Schedule) -> ScheduledTask:
    """Target path: map a first-class Schedule to a Taskiq ScheduledTask. It
    carries ONLY the schedule_id — the emit task resolves the schedule's bindings
    at fire time and fans out one event per binding."""
    return _scheduled_task(
        sch.schedule_id,
        sch.trigger_type,
        sch.trigger_args,
        {"schedule_id": sch.schedule_id, "trigger_type": sch.trigger_type},
    )


# --- trigger presentation helpers (schedule-only) ----------------------------

def _trigger_str_for(trigger_type: str, ta: TriggerArgs) -> str:
    if trigger_type == "cron":
        tz = f" {ta.timezone}" if ta.timezone else " UTC"
        return f"cron[{ta.cron_expression}]{tz}"
    if trigger_type == "interval":
        return f"interval[{_interval_seconds(ta)}s]"
    if trigger_type == "date":
        return f"date[{ta.run_date.isoformat() if ta.run_date else '?'}]"
    return trigger_type


def _next_run_time_for(
    trigger_type: str, ta: TriggerArgs, paused: bool
) -> Optional[datetime]:
    """Best-effort next fire time for the admin view (None when paused)."""
    if paused:
        return None
    if trigger_type == "cron" and ta.cron_expression:
        tz = ZoneInfo(ta.timezone) if ta.timezone else timezone.utc
        now = datetime.now(tz)
        try:
            return croniter(ta.cron_expression, now).get_next(datetime)
        except Exception:  # noqa: BLE001
            return None
    if trigger_type == "interval":
        secs = _interval_seconds(ta)
        return datetime.now(timezone.utc) + timedelta(seconds=secs) if secs else None
    if trigger_type == "date":
        return ta.run_date
    return None


def next_run_time(jd: JobDefinition) -> Optional[datetime]:
    """Compat: next fire time for a welded job (used by JobView)."""
    return _next_run_time_for(jd.trigger_type, jd.trigger_args, jd.paused)


# --- mapping: JobDefinition -> JobView ---------------------------------------

def job_def_to_view(jd: JobDefinition) -> JobView:
    return JobView(
        job_id=jd.job_id,
        trigger_type=jd.trigger_type,
        trigger=_trigger_str_for(jd.trigger_type, jd.trigger_args),
        trigger_args=jd.trigger_args.model_dump(exclude_none=True),
        next_run_time=next_run_time(jd),
        target_stream_id=jd.target_stream_id,
        resolved_stream=settings.stream_key(
            resolve_stream_id(jd.job_id, jd.target_stream_id)
        ),
        event_type=jd.event_type or settings.default_event_type,
        event_data=jd.event_data or {},
        room=jd.room,
        paused=jd.paused,
    )


# --- mapping: Schedule / Binding -> views ------------------------------------

def schedule_to_view(sch: Schedule, n_bindings: int) -> ScheduleView:
    return ScheduleView(
        schedule_id=sch.schedule_id,
        trigger_type=sch.trigger_type,
        trigger=_trigger_str_for(sch.trigger_type, sch.trigger_args),
        trigger_args=sch.trigger_args.model_dump(exclude_none=True),
        next_run_time=_next_run_time_for(sch.trigger_type, sch.trigger_args, sch.paused),
        paused=sch.paused,
        n_bindings=n_bindings,
    )


def binding_to_view(b: Binding) -> BindingView:
    # A binding resolves its stream from its own target; if unset, the schedule_id
    # is the derived stream (mirrors the welded-job fallback via resolve_stream_id).
    return BindingView(
        binding_id=b.binding_id,
        schedule_id=b.schedule_id,
        target_stream_id=b.target_stream_id,
        resolved_stream=settings.stream_key(
            resolve_stream_id(b.schedule_id, b.target_stream_id)
        ),
        event_type=b.event_type or settings.default_event_type,
        event_data=b.event_data or {},
        room=b.room,
    )


# --- migration: one welded job -> one Schedule + one Binding ------------------

class ScheduleRequest(BaseModel):
    """The ``schedule.requested`` bus contract (``payload.data``).

    An agent asks the scheduler to register recurring (or one-shot) work that
    emits ``event_type`` with ``event_data`` onto ``target_stream_id`` whenever
    the trigger fires. Maps to exactly one :class:`Schedule` + one
    :class:`Binding`. Re-sending the same ``schedule_id`` upserts (idempotent),
    unless ``replace`` is false and the schedule already exists.
    """

    schedule_id: str
    trigger_type: TriggerType
    trigger_args: TriggerArgs
    # What each fire emits:
    event_type: str = Field(default_factory=lambda: settings.default_event_type)
    event_data: dict[str, Any] = Field(default_factory=dict)
    # Where to emit; None derives the stream from the (single) binding id.
    target_stream_id: Optional[str] = None
    room: Optional[str] = None
    # Binding id; defaults to a deterministic ``<schedule_id>:b0`` so a re-send upserts.
    binding_id: Optional[str] = None
    paused: bool = False
    # Upsert if the schedule already exists (False = leave an existing one untouched).
    replace: bool = True

    @field_validator("schedule_id", "binding_id")
    @classmethod
    def _valid_id(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not ID_RE.match(v):
            raise ValueError(
                "schedule_id/binding_id must match ^[A-Za-z0-9._:-]+$ "
                "(they become a hash field / stream key)"
            )
        return v


def schedule_request_to_schedule_and_binding(
    req: ScheduleRequest,
) -> tuple[Schedule, Binding]:
    """Map a bus ``ScheduleRequest`` to its Schedule + single Binding.

    The binding id is derived deterministically (``<schedule_id>:b0``) when the
    request omits one, so re-sending the same request upserts the same pair."""
    schedule = Schedule(
        schedule_id=req.schedule_id,
        trigger_type=req.trigger_type,
        trigger_args=req.trigger_args,
        paused=req.paused,
    )
    binding = Binding(
        binding_id=req.binding_id or f"{req.schedule_id}:b0",
        schedule_id=req.schedule_id,
        target_stream_id=req.target_stream_id,
        event_type=req.event_type or settings.default_event_type,
        event_data=req.event_data or {},
        room=req.room,
    )
    return schedule, binding


def job_to_schedule_and_binding(jd: JobDefinition) -> tuple[Schedule, Binding]:
    """Faithfully split a welded job into its pure Schedule and a single Binding.

    The schedule keeps the job's id (stable Taskiq schedule_id, so per-minute
    dedup memory carries over). The binding id is derived deterministically so
    re-running the migration is idempotent (same job → same binding_id)."""
    schedule = Schedule(
        schedule_id=jd.job_id,
        trigger_type=jd.trigger_type,
        trigger_args=jd.trigger_args,
        paused=jd.paused,
    )
    binding = Binding(
        binding_id=f"{jd.job_id}:b0",
        schedule_id=jd.job_id,
        target_stream_id=jd.target_stream_id,
        event_type=jd.event_type or settings.default_event_type,
        event_data=jd.event_data or {},
        room=jd.room,
    )
    return schedule, binding
