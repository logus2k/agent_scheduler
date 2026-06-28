"""agent_scheduler — a reactive, single-process trigger actor.

FastAPI + embedded Taskiq (broker + worker + scheduler) in one process; emits
standard agent_bus ``EventEnvelope`` messages onto ``valkey-bus`` on schedule.
Taskiq's per-second, wall-clock-anchored loop replaces APScheduler (whose single
long monotonic sleep drifted on this host's WSL2 clock bug).
"""

__version__ = "2.0.0"
