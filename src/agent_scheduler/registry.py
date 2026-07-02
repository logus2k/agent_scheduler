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
from .models import Binding, JobDefinition, Schedule

log = logging.getLogger("agent_scheduler.registry")


class JobRegistry:
    def __init__(self, settings: Settings = default_settings):
        self._settings = settings
        self._key = settings.registry_key
        self._schedules_key = settings.schedules_key
        self._bindings_key = settings.bindings_key
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

    # --- Schedules (first-class, §6) ----------------------------------------

    async def schedule_exists(self, schedule_id: str) -> bool:
        return bool(await self.redis.hexists(self._schedules_key, schedule_id))

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        raw = await self.redis.hget(self._schedules_key, schedule_id)
        return Schedule.model_validate_json(raw) if raw else None

    async def list_schedules(self) -> list[Schedule]:
        rows = await self.redis.hgetall(self._schedules_key)
        return [Schedule.model_validate_json(v) for v in rows.values()]

    async def put_schedule(self, sch: Schedule) -> None:
        await self.redis.hset(self._schedules_key, sch.schedule_id, sch.model_dump_json())
        log.info("registry put schedule: %s (paused=%s)", sch.schedule_id, sch.paused)

    async def delete_schedule(self, schedule_id: str) -> bool:
        removed = await self.redis.hdel(self._schedules_key, schedule_id)
        if removed:
            log.info("registry delete schedule: %s", schedule_id)
        return bool(removed)

    # --- Bindings (indexed by schedule_id at read time) ---------------------

    async def binding_exists(self, binding_id: str) -> bool:
        return bool(await self.redis.hexists(self._bindings_key, binding_id))

    async def get_binding(self, binding_id: str) -> Binding | None:
        raw = await self.redis.hget(self._bindings_key, binding_id)
        return Binding.model_validate_json(raw) if raw else None

    async def list_bindings(self) -> list[Binding]:
        rows = await self.redis.hgetall(self._bindings_key)
        return [Binding.model_validate_json(v) for v in rows.values()]

    async def bindings_for(self, schedule_id: str) -> list[Binding]:
        """All bindings attached to a schedule (the fan-out set). Empty is valid
        (an inconsequent schedule fires nothing)."""
        return [b for b in await self.list_bindings() if b.schedule_id == schedule_id]

    async def put_binding(self, binding: Binding) -> None:
        await self.redis.hset(
            self._bindings_key, binding.binding_id, binding.model_dump_json()
        )
        log.info(
            "registry put binding: %s -> schedule %s",
            binding.binding_id,
            binding.schedule_id,
        )

    async def delete_binding(self, binding_id: str) -> bool:
        removed = await self.redis.hdel(self._bindings_key, binding_id)
        if removed:
            log.info("registry delete binding: %s", binding_id)
        return bool(removed)


# Process-global registry (shared by the API, the schedule source, and the task).
registry = JobRegistry()
