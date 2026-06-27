"""FastAPI admin API — CRUD over the in-process AsyncIOScheduler.

Single process: every route mutates the live scheduler, and the change is
persisted to RedisJobStore in the same call. No second process to sync with.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from apscheduler.jobstores.base import ConflictingIdError, JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, HTTPException, Response

from . import emitter
from .config import settings
from .models import JobCreate, JobUpdate, JobView, build_trigger, job_to_view
from .scheduler import build_scheduler

log = logging.getLogger("agent_scheduler.api")

EMITTER_REF = "agent_scheduler.emitter:emit_scheduled_event"


def create_app(
    scheduler: AsyncIOScheduler | None = None,
    *,
    connect_emitter: bool = True,
) -> FastAPI:
    """Build the app. Tests inject a MemoryJobStore scheduler and
    ``connect_emitter=False`` to stay Valkey-free."""
    sched = scheduler or build_scheduler(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if connect_emitter:
            await emitter.connect()  # glide publisher (retry until valkey-bus is up)
        sched.start()                # rehydrates persisted jobs
        log.info("agent_scheduler up — API on %s:%s", settings.api_host, settings.api_port)
        try:
            yield
        finally:
            sched.shutdown()
            if connect_emitter:
                await emitter.close()
            log.info("agent_scheduler shut down cleanly")

    app = FastAPI(title="Agent Scheduler", version="1.0.0", lifespan=lifespan)

    def get_scheduler() -> AsyncIOScheduler:
        return sched

    def _job_or_404(sched: AsyncIOScheduler, job_id: str):
        job = sched.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"job not found: {job_id}")
        return job

    # --- CRUD ---------------------------------------------------------------

    @app.post("/jobs", response_model=JobView, status_code=201)
    def create_job(body: JobCreate, sched: AsyncIOScheduler = Depends(get_scheduler)):
        try:
            trigger = build_trigger(body.trigger_type, body.trigger_args)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        try:
            job = sched.add_job(
                func=EMITTER_REF,
                trigger=trigger,
                id=body.job_id,
                replace_existing=False,
                kwargs={
                    "job_id": body.job_id,
                    "target_stream_id": body.target_stream_id,
                    "event_type": body.event_type,
                    "event_data": body.event_data,
                    "room": body.room,
                    "trigger_type": body.trigger_type,
                },
            )
        except ConflictingIdError:
            raise HTTPException(status_code=409, detail=f"job already exists: {body.job_id}")
        if body.paused:
            sched.pause_job(body.job_id)
            job = sched.get_job(body.job_id)
        return job_to_view(job)

    @app.get("/jobs", response_model=list[JobView])
    def list_jobs(sched: AsyncIOScheduler = Depends(get_scheduler)):
        return [job_to_view(j) for j in sched.get_jobs()]

    @app.get("/jobs/{job_id}", response_model=JobView)
    def get_job(job_id: str, sched: AsyncIOScheduler = Depends(get_scheduler)):
        return job_to_view(_job_or_404(sched, job_id))

    @app.patch("/jobs/{job_id}", response_model=JobView)
    def update_job(
        job_id: str, body: JobUpdate, sched: AsyncIOScheduler = Depends(get_scheduler)
    ):
        job = _job_or_404(sched, job_id)

        # Payload changes: merge into the stored kwargs.
        kw = dict(job.kwargs or {})
        for field in ("target_stream_id", "event_type", "event_data", "room"):
            val = getattr(body, field)
            if val is not None:
                kw[field] = val
        if kw != (job.kwargs or {}):
            sched.modify_job(job_id, kwargs=kw)

        # Trigger changes: reschedule (both fields required together).
        if body.trigger_type is not None or body.trigger_args is not None:
            if body.trigger_type is None or body.trigger_args is None:
                raise HTTPException(
                    status_code=422,
                    detail="trigger_type and trigger_args must be supplied together",
                )
            try:
                trigger = build_trigger(body.trigger_type, body.trigger_args)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc))
            sched.reschedule_job(job_id, trigger=trigger)
            sched.modify_job(job_id, kwargs={**kw, "trigger_type": body.trigger_type})

        return job_to_view(sched.get_job(job_id))

    @app.delete("/jobs/{job_id}", status_code=204)
    def delete_job(job_id: str, sched: AsyncIOScheduler = Depends(get_scheduler)):
        try:
            sched.remove_job(job_id)
        except JobLookupError:
            raise HTTPException(status_code=404, detail=f"job not found: {job_id}")
        return Response(status_code=204)

    # --- lifecycle ----------------------------------------------------------

    @app.post("/jobs/{job_id}/pause", response_model=JobView)
    def pause_job(job_id: str, sched: AsyncIOScheduler = Depends(get_scheduler)):
        try:
            sched.pause_job(job_id)
        except JobLookupError:
            raise HTTPException(status_code=404, detail=f"job not found: {job_id}")
        return job_to_view(sched.get_job(job_id))

    @app.post("/jobs/{job_id}/resume", response_model=JobView)
    def resume_job(job_id: str, sched: AsyncIOScheduler = Depends(get_scheduler)):
        try:
            sched.resume_job(job_id)
        except JobLookupError:
            raise HTTPException(status_code=404, detail=f"job not found: {job_id}")
        return job_to_view(sched.get_job(job_id))

    @app.post("/jobs/{job_id}/run", status_code=202)
    async def run_job(job_id: str, sched: AsyncIOScheduler = Depends(get_scheduler)):
        """Emit once now, off-schedule (does not affect the trigger)."""
        job = _job_or_404(sched, job_id)
        entry_id = await emitter.emit_scheduled_event(**(job.kwargs or {}))
        return {"status": "fired", "job_id": job_id, "entry_id": entry_id}

    # --- ops ---------------------------------------------------------------

    @app.get("/health")
    async def health(sched: AsyncIOScheduler = Depends(get_scheduler)):
        valkey_up = (not connect_emitter) or await emitter.ping()
        try:
            jobs = sched.get_jobs()
            jobstore_up = True
        except Exception:  # noqa: BLE001
            jobs = []
            jobstore_up = False
        ok = valkey_up and jobstore_up
        body = {
            "status": "ok" if ok else "degraded",
            "valkey": "up" if valkey_up else "down",
            "jobstore": "up" if jobstore_up else "down",
            "jobs": len(jobs),
        }
        if not ok:
            raise HTTPException(status_code=503, detail=body)
        return body

    return app


# Module-level app for uvicorn (production: Valkey-backed jobstore + glide publisher).
app = create_app()
