"""DEPRECATED — removed in the Taskiq migration.

The APScheduler RunTimeInjectingExecutor (which smuggled scheduled_run_time
through a contextvar) is no longer needed: Taskiq fires the task directly and
the emitter timestamps `fired_at` itself. Kept as a tombstone; delete once
committed.
"""
