# Agent Scheduler — single-process app container (FastAPI + APScheduler).
#
# IMPORTANT: base image MUST be glibc (Debian slim), NOT alpine/MUSL.
# valkey-glide ships a Rust core with no MUSL wheels, so an Alpine base
# would fail to install/run the client.
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first for layer caching.
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Application source (package lives under src/agent_scheduler).
COPY src/ ./src/
ENV PYTHONPATH=/app/src

# Single-process entrypoint: uvicorn serving the admin API, which owns the scheduler.
CMD ["python", "-m", "agent_scheduler.app"]
