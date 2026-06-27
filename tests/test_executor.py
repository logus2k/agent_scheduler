"""RunTimeInjectingExecutor surfaces the per-fire scheduled run time."""

import asyncio
from datetime import datetime

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from agent_scheduler import emitter
from agent_scheduler.executor import RunTimeInjectingExecutor

_captured: list = []


async def _recorder():
    # The job reads what the executor injected for *this* fire.
    _captured.append(emitter.scheduled_run_time_var.get())


async def test_executor_injects_scheduled_run_time():
    _captured.clear()
    sched = AsyncIOScheduler(
        jobstores={"default": MemoryJobStore()},
        executors={"default": RunTimeInjectingExecutor()},
        job_defaults={"misfire_grace_time": 30, "coalesce": True},
    )
    sched.start()
    sched.add_job(_recorder, "interval", seconds=1, id="r")
    await asyncio.sleep(1.3)
    sched.shutdown(wait=False)

    assert _captured, "job never fired"
    assert _captured[0] is not None
    # It must be a valid ISO-8601 timestamp.
    datetime.fromisoformat(_captured[0])


async def test_no_executor_means_no_run_time():
    # Without the injecting executor, the contextvar stays at its default.
    assert emitter.scheduled_run_time_var.get() is None
