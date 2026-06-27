"""agent_scheduler — a reactive, single-process trigger actor.

FastAPI + APScheduler (3.x) in one process; emits standard agent_bus
``EventEnvelope`` messages onto ``valkey-bus`` on schedule.
"""

__version__ = "1.0.0"
