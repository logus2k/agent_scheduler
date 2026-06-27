"""Custom executor that surfaces each fire's scheduled run time.

APScheduler 3.x calls ``job.func(*args, **kwargs)`` without telling the function
*when* it was scheduled to run. We need that inside the emitted envelope (for
consumer idempotency and late-fire detection), so this executor stashes the
run time in a contextvar immediately before the job task is created. Because
``loop.create_task`` snapshots the current context, the running coroutine reads
the value for its own fire even though we reset the var right after.

With ``coalesce=True`` (our default) APScheduler hands a single run time per
submit, so taking the last element is exact; with coalesce off, it is the most
recent of the collapsed window.
"""

from __future__ import annotations

from apscheduler.executors.asyncio import AsyncIOExecutor

from .emitter import scheduled_run_time_var


class RunTimeInjectingExecutor(AsyncIOExecutor):
    def _do_submit_job(self, job, run_times):
        token = scheduled_run_time_var.set(
            run_times[-1].isoformat() if run_times else None
        )
        try:
            return super()._do_submit_job(job, run_times)
        finally:
            # Safe to reset now: the task created above already captured the value.
            scheduled_run_time_var.reset(token)
