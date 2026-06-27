"""Environment-driven configuration.

A single immutable ``Settings`` instance, populated from the environment
(with a ``.env`` loaded in dev). Every knob has a safe default so the app
runs with an empty environment. Keep ALL tunables here — no magic numbers
scattered across the codebase. Mirrors agent_bus's config conventions.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load .env if present (no-op in containers that inject real env vars).
load_dotenv()


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _str(name: str, default: str) -> str:
    raw = os.getenv(name)
    return default if raw is None or raw == "" else raw


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    # --- Valkey connection (shared valkey-bus) ---
    valkey_host: str = _str("VALKEY_HOST", "127.0.0.1")
    valkey_port: int = _int("VALKEY_PORT", 6379)

    # --- Stream / discovery conventions (must match agent_bus) ---
    stream_prefix: str = _str("STREAM_PREFIX", "stream:")
    active_streams_key: str = _str("ACTIVE_STREAMS_KEY", "streams:active")

    # --- RedisJobStore keys (namespaced; do not collide with agent_bus keys) ---
    jobs_key: str = _str("JOBS_KEY", "agent_scheduler.jobs")
    run_times_key: str = _str("RUN_TIMES_KEY", "agent_scheduler.run_times")

    # --- Emission identity ---
    sender_id: str = _str("SENDER_ID", "agent_scheduler")
    default_event_type: str = _str("DEFAULT_EVENT_TYPE", "schedule.fired")

    # --- Misfire policy (defines behavior around downtime) ---
    # A fire whose scheduled time was missed by more than this many seconds is
    # skipped (logged as missed) rather than run late. Raise it to allow catch-up.
    misfire_grace_time: int = _int("MISFIRE_GRACE_TIME", 30)
    # Collapse multiple missed fires of one job into a single catch-up run.
    coalesce: bool = _bool("COALESCE", True)

    # --- Publisher connection retry (startup resilience across compose projects) ---
    connect_retries: int = _int("CONNECT_RETRIES", 30)
    connect_retry_delay_s: int = _int("CONNECT_RETRY_DELAY_S", 2)

    # --- Admin API ---
    api_host: str = _str("API_HOST", "0.0.0.0")
    api_port: int = _int("API_PORT", 6816)

    # --- Logging ---
    log_level: str = _str("LOG_LEVEL", "INFO")

    def stream_key(self, stream_id: str) -> str:
        """The dedicated stream key for a target: ``stream:<stream_id>``."""
        return f"{self.stream_prefix}{stream_id}"


settings = Settings()
