"""FastAPI admin API — CRUD over the job registry, scheduled by embedded Taskiq.

Single process: the FastAPI lifespan connects the registry + bus publisher and runs
the Taskiq worker and scheduler as embedded asyncio tasks. The admin routes mutate
the registry (the source of truth); the schedule source derives schedules from it.
The bus contract (the schedule.fired envelope) is unchanged.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from taskiq.api import run_receiver_task, run_scheduler_task

from . import emitter
from .config import settings
from .models import (
    JobCreate,
    JobDefinition,
    JobUpdate,
    JobView,
    job_def_to_view,
    validate_trigger,
)
from .registry import registry
from .tasks import broker, scheduler

log = logging.getLogger("agent_scheduler.api")


def _emit_kwargs(jd: JobDefinition) -> dict:
    return {
        "job_id": jd.job_id,
        "target_stream_id": jd.target_stream_id,
        "event_type": jd.event_type,
        "event_data": jd.event_data,
        "room": jd.room,
        "trigger_type": jd.trigger_type,
    }


def create_app(*, embed: bool = True, connect_emitter: bool = True) -> FastAPI:
    """Build the app. ``embed`` runs the Taskiq worker+scheduler in-process;
    ``connect_emitter`` connects the glide bus Publisher."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await registry.connect()
        if connect_emitter:
            await emitter.connect()  # glide publisher (retry until valkey-bus is up)

        bg: list[asyncio.Task] = []
        if embed:
            await broker.startup()
            bg = [
                asyncio.create_task(run_receiver_task(broker), name="taskiq-worker"),
                asyncio.create_task(run_scheduler_task(scheduler), name="taskiq-scheduler"),
            ]
        log.info("agent_scheduler up — API on %s:%s", settings.api_host, settings.api_port)
        try:
            yield
        finally:
            for t in bg:
                t.cancel()
            for t in bg:
                try:
                    await t
                except asyncio.CancelledError:
                    pass
            if embed:
                await broker.shutdown()
            if connect_emitter:
                await emitter.close()
            await registry.close()
            log.info("agent_scheduler shut down cleanly")

    app = FastAPI(title="Agent Scheduler", version="2.0.0", lifespan=lifespan)

    async def _job_or_404(job_id: str) -> JobDefinition:
        jd = await registry.get(job_id)
        if jd is None:
            raise HTTPException(status_code=404, detail=f"job not found: {job_id}")
        return jd

    # --- CRUD ---------------------------------------------------------------

    @app.post("/jobs", response_model=JobView, status_code=201)
    async def create_job(body: JobCreate):
        try:
            validate_trigger(body.trigger_type, body.trigger_args)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        if await registry.exists(body.job_id):
            raise HTTPException(status_code=409, detail=f"job already exists: {body.job_id}")
        await registry.put(body)
        return job_def_to_view(body)

    @app.get("/jobs", response_model=list[JobView])
    async def list_jobs():
        return [job_def_to_view(jd) for jd in await registry.list()]

    @app.get("/jobs/{job_id}", response_model=JobView)
    async def get_job(job_id: str):
        return job_def_to_view(await _job_or_404(job_id))

    @app.patch("/jobs/{job_id}", response_model=JobView)
    async def update_job(job_id: str, body: JobUpdate):
        jd = await _job_or_404(job_id)
        data = jd.model_dump()

        for field in ("target_stream_id", "event_type", "event_data", "room"):
            val = getattr(body, field)
            if val is not None:
                data[field] = val

        if body.trigger_type is not None or body.trigger_args is not None:
            if body.trigger_type is None or body.trigger_args is None:
                raise HTTPException(
                    status_code=422,
                    detail="trigger_type and trigger_args must be supplied together",
                )
            try:
                validate_trigger(body.trigger_type, body.trigger_args)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc))
            data["trigger_type"] = body.trigger_type
            data["trigger_args"] = body.trigger_args.model_dump()

        updated = JobDefinition.model_validate(data)
        await registry.put(updated)
        return job_def_to_view(updated)

    @app.delete("/jobs/{job_id}", status_code=204)
    async def delete_job(job_id: str):
        jd = await _job_or_404(job_id)
        await registry.delete(job_id)
        # Only a derived per-job stream (id == job_id) is unique to this job and
        # safe to drop from the active set; shared explicit targets are left alone.
        if jd.target_stream_id is None:
            await emitter.deregister_stream(job_id)
        return Response(status_code=204)

    # --- lifecycle ----------------------------------------------------------

    @app.post("/jobs/{job_id}/pause", response_model=JobView)
    async def pause_job(job_id: str):
        jd = await _job_or_404(job_id)
        jd.paused = True
        await registry.put(jd)
        return job_def_to_view(jd)

    @app.post("/jobs/{job_id}/resume", response_model=JobView)
    async def resume_job(job_id: str):
        jd = await _job_or_404(job_id)
        jd.paused = False
        await registry.put(jd)
        return job_def_to_view(jd)

    @app.post("/jobs/{job_id}/run", status_code=202)
    async def run_job(job_id: str):
        """Emit once now, off-schedule and un-deduped (does not affect the trigger)."""
        jd = await _job_or_404(job_id)
        entry_id = await emitter.emit_scheduled_event(**_emit_kwargs(jd))
        return {"status": "fired", "job_id": job_id, "entry_id": entry_id}

    # --- ops ---------------------------------------------------------------

    @app.get("/health")
    async def health():
        registry_up = await registry.ping()
        valkey_up = (not connect_emitter) or await emitter.ping()
        try:
            n_jobs = len(await registry.list())
        except Exception:  # noqa: BLE001
            n_jobs = -1
            registry_up = False
        ok = registry_up and valkey_up
        body = {
            "status": "ok" if ok else "degraded",
            "valkey": "up" if valkey_up else "down",
            "registry": "up" if registry_up else "down",
            "jobs": n_jobs,
        }
        if not ok:
            raise HTTPException(status_code=503, detail=body)
        return body

    # Markdown docs (the Help dialog fetches /docs/use_cases.md). Mounted BEFORE
    # the greedy root UI mount below.
    if os.path.isdir(settings.docs_dir):
        app.mount("/docs", StaticFiles(directory=settings.docs_dir), name="docs")
        log.info("docs mounted at /docs (from %s)", settings.docs_dir)
    else:
        log.warning("docs dir %r not found; /docs disabled", settings.docs_dir)

    # Admin web UI (static, same-origin). MUST be the last mount ("/" is greedy).
    if os.path.isdir(settings.frontend_dir):
        app.mount("/", StaticFiles(directory=settings.frontend_dir, html=True), name="ui")
        log.info("admin UI mounted at / (from %s)", settings.frontend_dir)
    else:
        log.warning("frontend dir %r not found; UI disabled", settings.frontend_dir)

    return app


# Module-level app for uvicorn (production: embedded Taskiq + glide publisher).
app = create_app()
