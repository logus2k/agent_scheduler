"""Agent Scheduler — Python client SDK.

A thin, dependency-light wrapper over the scheduler's REST API. Returns plain
dicts shaped like the API's ``JobView`` and raises typed errors so callers can
branch on failure mode rather than parsing status codes.

Requires: httpx (``pip install httpx``).

Example
-------
    from agent_scheduler_client import SchedulerClient

    with SchedulerClient("http://agent-scheduler-app:6816") as sched:
        sched.create_cron(
            job_id="nightly-report",
            cron_expression="0 2 * * *",
            event_data={"report": "daily_sales"},
        )
        for job in sched.list_jobs():
            print(job["job_id"], "->", job["resolved_stream"], job["next_run_time"])
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import httpx

__all__ = [
    "SchedulerClient",
    "SchedulerError",
    "JobNotFoundError",
    "JobConflictError",
    "ValidationError",
    "ServiceUnavailableError",
]


# --- errors -----------------------------------------------------------------

class SchedulerError(Exception):
    """Base error. ``status`` is the HTTP code; ``detail`` the server message."""

    def __init__(self, status: int, detail: Any):
        self.status = status
        self.detail = detail
        super().__init__(f"[{status}] {detail}")


class JobNotFoundError(SchedulerError):
    """404 — no job with that id."""


class JobConflictError(SchedulerError):
    """409 — a job with that id already exists."""


class ValidationError(SchedulerError):
    """422 — invalid request body / trigger args."""


class ServiceUnavailableError(SchedulerError):
    """503 — scheduler is degraded (Valkey or job store unreachable)."""


_ERROR_BY_STATUS = {
    404: JobNotFoundError,
    409: JobConflictError,
    422: ValidationError,
    503: ServiceUnavailableError,
}


# --- client -----------------------------------------------------------------

class SchedulerClient:
    """Synchronous client for the Agent Scheduler admin API."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
        client: Optional[httpx.Client] = None,
    ):
        self._owns_client = client is None
        self._http = client or httpx.Client(
            base_url=base_url.rstrip("/"), timeout=timeout
        )

    # context manager sugar
    def __enter__(self) -> "SchedulerClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    # --- low-level ----------------------------------------------------------

    def _request(self, method: str, path: str, **kwargs) -> Any:
        resp = self._http.request(method, path, **kwargs)
        if resp.is_success:
            if resp.status_code == 204 or not resp.content:
                return None
            return resp.json()
        # Error: unwrap FastAPI's {"detail": ...}
        try:
            detail = resp.json().get("detail", resp.text)
        except Exception:  # noqa: BLE001
            detail = resp.text
        raise _ERROR_BY_STATUS.get(resp.status_code, SchedulerError)(
            resp.status_code, detail
        )

    # --- ops ----------------------------------------------------------------

    def health(self) -> dict:
        """Liveness + Valkey/job-store reachability. Raises on 503."""
        return self._request("GET", "/health")

    def list_jobs(self) -> list[dict]:
        return self._request("GET", "/jobs")

    def get_job(self, job_id: str) -> dict:
        return self._request("GET", f"/jobs/{job_id}")

    def create_job(
        self,
        *,
        job_id: str,
        trigger_type: str,
        trigger_args: dict,
        target_stream_id: Optional[str] = None,
        event_type: str = "schedule.fired",
        event_data: Optional[dict] = None,
        room: Optional[str] = None,
        paused: bool = False,
    ) -> dict:
        """Create a job. ``trigger_type`` ∈ {interval, cron, date}; ``trigger_args``
        carries the fields for that type (see create_interval/cron/date helpers)."""
        body = {
            "job_id": job_id,
            "trigger_type": trigger_type,
            "trigger_args": trigger_args,
            "target_stream_id": target_stream_id,
            "event_type": event_type,
            "event_data": event_data or {},
            "room": room,
            "paused": paused,
        }
        return self._request("POST", "/jobs", json=body)

    # convenience constructors --------------------------------------------------

    def create_interval(
        self,
        job_id: str,
        *,
        seconds: int = 0,
        minutes: int = 0,
        hours: int = 0,
        days: int = 0,
        weeks: int = 0,
        **kwargs,
    ) -> dict:
        args = {
            k: v
            for k, v in dict(
                seconds=seconds, minutes=minutes, hours=hours, days=days, weeks=weeks
            ).items()
            if v
        }
        return self.create_job(
            job_id=job_id, trigger_type="interval", trigger_args=args, **kwargs
        )

    def create_cron(self, job_id: str, cron_expression: str, **kwargs) -> dict:
        return self.create_job(
            job_id=job_id,
            trigger_type="cron",
            trigger_args={"cron_expression": cron_expression},
            **kwargs,
        )

    def create_date(self, job_id: str, run_date: "str | datetime", **kwargs) -> dict:
        if isinstance(run_date, datetime):
            run_date = run_date.isoformat()
        return self.create_job(
            job_id=job_id,
            trigger_type="date",
            trigger_args={"run_date": run_date},
            **kwargs,
        )

    def update_job(
        self,
        job_id: str,
        *,
        trigger_type: Optional[str] = None,
        trigger_args: Optional[dict] = None,
        target_stream_id: Optional[str] = None,
        event_type: Optional[str] = None,
        event_data: Optional[dict] = None,
        room: Optional[str] = None,
    ) -> dict:
        """Partial update. Supply trigger_type + trigger_args together to reschedule."""
        body = {
            k: v
            for k, v in dict(
                trigger_type=trigger_type,
                trigger_args=trigger_args,
                target_stream_id=target_stream_id,
                event_type=event_type,
                event_data=event_data,
                room=room,
            ).items()
            if v is not None
        }
        return self._request("PATCH", f"/jobs/{job_id}", json=body)

    def delete_job(self, job_id: str) -> None:
        self._request("DELETE", f"/jobs/{job_id}")

    def pause_job(self, job_id: str) -> dict:
        return self._request("POST", f"/jobs/{job_id}/pause")

    def resume_job(self, job_id: str) -> dict:
        return self._request("POST", f"/jobs/{job_id}/resume")

    def run_job(self, job_id: str) -> dict:
        """Emit once now, off-schedule. Returns {status, job_id, entry_id}."""
        return self._request("POST", f"/jobs/{job_id}/run")
