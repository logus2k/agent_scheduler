"""Entrypoint — run the admin API (which owns the scheduler) under uvicorn.

The FastAPI lifespan connects the glide publisher, starts the AsyncIOScheduler
(rehydrating persisted jobs), and tears both down cleanly on shutdown.
"""

from __future__ import annotations

import logging

import uvicorn

from .api import app
from .config import settings


def main() -> None:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
