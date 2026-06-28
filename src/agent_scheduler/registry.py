"""Job registry — the persistent source of truth for scheduled jobs.

A single Redis hash (``registry_key``) maps ``job_id -> JobDefinition JSON``,
on the same valkey-bus as everything else (via redis-py, which taskiq-redis
already depends on). The admin API reads/writes here; the Taskiq schedule source
(``RegistryScheduleSource``) derives schedules from it. Persistent across restarts.

The same redis-py client also backs the per-(job,minute) idempotency guard.
"""

from __future__ import annotations

import logging

import redis.asyncio as aioredis

from .config import Settings, settings as default_settings
from .models import JobDefinition

log = logging.getLogger("agent_scheduler.registry")


class JobRegistry:
    def __init__(self, settings: Settings = default_settings):
        self._settings = settings
        self._key = settings.registry_key
        self._redis: aioredis.Redis | None = None

    async def connect(self) -> None:
        if self._redis is None:
            self._redis = aioredis.Redis.from_url(
                self._settings.redis_url(), decode_responses=True
            )
            await self._redis.ping()
            log.info("registry connected (%s)", self._settings.redis_url())

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    @property
    def redis(self) -> aioredis.Redis:
        if self._redis is None:
            raise RuntimeError("registry not connected; call connect() first")
        return self._redis

    async def ping(self) -> bool:
        return self._redis is not None and bool(await self._redis.ping())

    # --- CRUD ---------------------------------------------------------------

    async def exists(self, job_id: str) -> bool:
        return bool(await self.redis.hexists(self._key, job_id))

    async def get(self, job_id: str) -> JobDefinition | None:
        raw = await self.redis.hget(self._key, job_id)
        return JobDefinition.model_validate_json(raw) if raw else None

    async def list(self) -> list[JobDefinition]:
        rows = await self.redis.hgetall(self._key)
        return [JobDefinition.model_validate_json(v) for v in rows.values()]

    async def put(self, job: JobDefinition) -> None:
        """Insert or replace a job definition (the API enforces create vs update)."""
        await self.redis.hset(self._key, job.job_id, job.model_dump_json())
        log.info("registry put: %s (paused=%s)", job.job_id, job.paused)

    async def delete(self, job_id: str) -> bool:
        removed = await self.redis.hdel(self._key, job_id)
        if removed:
            log.info("registry delete: %s", job_id)
        return bool(removed)


# Process-global registry (shared by the API, the schedule source, and the task).
registry = JobRegistry()
