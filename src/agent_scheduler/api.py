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
    Binding,
    BindingCreate,
    BindingUpdate,
    BindingView,
    JobCreate,
    JobDefinition,
    JobUpdate,
    JobView,
    Schedule,
    ScheduleCreate,
    ScheduleUpdate,
    ScheduleView,
    binding_to_view,
    job_def_to_view,
    job_to_schedule_and_binding,
    schedule_to_view,
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

    # --- Schedules (first-class, §6) ----------------------------------------

    async def _schedule_or_404(schedule_id: str) -> Schedule:
        sch = await registry.get_schedule(schedule_id)
        if sch is None:
            raise HTTPException(
                status_code=404, detail=f"schedule not found: {schedule_id}"
            )
        return sch

    async def _schedule_view(sch: Schedule) -> ScheduleView:
        n = len(await registry.bindings_for(sch.schedule_id))
        return schedule_to_view(sch, n)

    @app.post("/schedules", response_model=ScheduleView, status_code=201)
    async def create_schedule(body: ScheduleCreate):
        try:
            validate_trigger(body.trigger_type, body.trigger_args)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        if await registry.schedule_exists(body.schedule_id):
            raise HTTPException(
                status_code=409, detail=f"schedule already exists: {body.schedule_id}"
            )
        sch = Schedule(**body.model_dump())
        await registry.put_schedule(sch)
        return await _schedule_view(sch)

    @app.get("/schedules", response_model=list[ScheduleView])
    async def list_schedules():
        return [await _schedule_view(s) for s in await registry.list_schedules()]

    @app.get("/schedules/{schedule_id}", response_model=ScheduleView)
    async def get_schedule(schedule_id: str):
        return await _schedule_view(await _schedule_or_404(schedule_id))

    @app.patch("/schedules/{schedule_id}", response_model=ScheduleView)
    async def update_schedule(schedule_id: str, body: ScheduleUpdate):
        sch = await _schedule_or_404(schedule_id)
        data = sch.model_dump()
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
        updated = Schedule.model_validate(data)
        await registry.put_schedule(updated)
        return await _schedule_view(updated)

    @app.delete("/schedules/{schedule_id}", status_code=204)
    async def delete_schedule(schedule_id: str):
        await _schedule_or_404(schedule_id)
        # Cascade: drop the schedule's bindings too (they are meaningless orphaned).
        for b in await registry.bindings_for(schedule_id):
            await registry.delete_binding(b.binding_id)
        await registry.delete_schedule(schedule_id)
        return Response(status_code=204)

    @app.post("/schedules/{schedule_id}/pause", response_model=ScheduleView)
    async def pause_schedule(schedule_id: str):
        sch = await _schedule_or_404(schedule_id)
        sch.paused = True
        await registry.put_schedule(sch)
        return await _schedule_view(sch)

    @app.post("/schedules/{schedule_id}/resume", response_model=ScheduleView)
    async def resume_schedule(schedule_id: str):
        sch = await _schedule_or_404(schedule_id)
        sch.paused = False
        await registry.put_schedule(sch)
        return await _schedule_view(sch)

    @app.post("/schedules/{schedule_id}/run", status_code=202)
    async def run_schedule(schedule_id: str):
        """Fan out once now, off-schedule and un-deduped: emit one event per
        binding (nothing if the schedule has zero bindings)."""
        sch = await _schedule_or_404(schedule_id)
        bindings = await registry.bindings_for(schedule_id)
        entries = []
        for b in bindings:
            entry_id = await emitter.emit_scheduled_event(
                job_id=b.binding_id,
                target_stream_id=b.target_stream_id,
                event_type=b.event_type,
                event_data=b.event_data,
                room=b.room,
                trigger_type=sch.trigger_type,
            )
            entries.append({"binding_id": b.binding_id, "entry_id": entry_id})
        return {"status": "fired", "schedule_id": schedule_id, "emitted": entries}

    # --- Bindings (schedule -> action) --------------------------------------

    async def _binding_or_404(schedule_id: str, binding_id: str) -> Binding:
        b = await registry.get_binding(binding_id)
        if b is None or b.schedule_id != schedule_id:
            raise HTTPException(
                status_code=404,
                detail=f"binding not found: {binding_id} on schedule {schedule_id}",
            )
        return b

    @app.post(
        "/schedules/{schedule_id}/bindings",
        response_model=BindingView,
        status_code=201,
    )
    async def create_binding(schedule_id: str, body: BindingCreate):
        await _schedule_or_404(schedule_id)
        if await registry.binding_exists(body.binding_id):
            raise HTTPException(
                status_code=409, detail=f"binding already exists: {body.binding_id}"
            )
        b = Binding(schedule_id=schedule_id, **body.model_dump())
        await registry.put_binding(b)
        return binding_to_view(b)

    @app.get("/schedules/{schedule_id}/bindings", response_model=list[BindingView])
    async def list_bindings(schedule_id: str):
        await _schedule_or_404(schedule_id)
        return [binding_to_view(b) for b in await registry.bindings_for(schedule_id)]

    @app.get(
        "/schedules/{schedule_id}/bindings/{binding_id}", response_model=BindingView
    )
    async def get_binding(schedule_id: str, binding_id: str):
        return binding_to_view(await _binding_or_404(schedule_id, binding_id))

    @app.patch(
        "/schedules/{schedule_id}/bindings/{binding_id}", response_model=BindingView
    )
    async def update_binding(schedule_id: str, binding_id: str, body: BindingUpdate):
        b = await _binding_or_404(schedule_id, binding_id)
        data = b.model_dump()
        for field in ("target_stream_id", "event_type", "event_data", "room"):
            val = getattr(body, field)
            if val is not None:
                data[field] = val
        updated = Binding.model_validate(data)
        await registry.put_binding(updated)
        return binding_to_view(updated)

    @app.delete(
        "/schedules/{schedule_id}/bindings/{binding_id}", status_code=204
    )
    async def delete_binding(schedule_id: str, binding_id: str):
        await _binding_or_404(schedule_id, binding_id)
        await registry.delete_binding(binding_id)
        return Response(status_code=204)

    # --- Migration: welded jobs -> Schedule + Binding -----------------------

    @app.post("/migrate/jobs-to-schedules", status_code=200)
    async def migrate_jobs_to_schedules(delete_jobs: bool = False):
        """Idempotent migration: each welded job -> one Schedule + one Binding.

        Loud on anything it can't map (re-raises). Leaves the source jobs in
        place by default (reversible) unless ``delete_jobs=true``."""
        migrated = []
        for jd in await registry.list():
            sch, binding = job_to_schedule_and_binding(jd)
            await registry.put_schedule(sch)
            await registry.put_binding(binding)
            if delete_jobs:
                await registry.delete(jd.job_id)
            migrated.append(
                {
                    "job_id": jd.job_id,
                    "schedule_id": sch.schedule_id,
                    "binding_id": binding.binding_id,
                }
            )
        return {"migrated": migrated, "count": len(migrated), "jobs_deleted": delete_jobs}

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
