"""Scheduler wiring — AsyncIOScheduler backed by a Valkey RedisJobStore.

The job store exists for restart-resilience only (not coordination): on startup
the scheduler rehydrates persisted jobs and resumes their triggers. The store
talks to the same valkey-bus as the publisher, but via redis-py (APScheduler
only ships redis-py / SQLAlchemy stores) and under a namespaced key, so it never
collides with the choreography keyspace.
"""

from __future__ import annotations

import logging

from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .config import Settings, settings as default_settings
from .executor import RunTimeInjectingExecutor

log = logging.getLogger("agent_scheduler.scheduler")


def build_scheduler(settings: Settings = default_settings) -> AsyncIOScheduler:
    jobstore = RedisJobStore(
        host=settings.valkey_host,
        port=settings.valkey_port,
        jobs_key=settings.jobs_key,
        run_times_key=settings.run_times_key,
    )
    scheduler = AsyncIOScheduler(
        jobstores={"default": jobstore},
        executors={"default": RunTimeInjectingExecutor()},
        job_defaults={
            "misfire_grace_time": settings.misfire_grace_time,
            "coalesce": settings.coalesce,
        },
    )
    log.info(
        "scheduler built (jobstore=%s on %s:%s, misfire_grace=%ss, coalesce=%s)",
        settings.jobs_key,
        settings.valkey_host,
        settings.valkey_port,
        settings.misfire_grace_time,
        settings.coalesce,
    )
    return scheduler
