# Proposal: Resource Retention & Emit Reliability Fixes

**Status:** ✅ APPLIED (2026-06-27). Fixes 1–3 implemented in full plus Fix 4's
ERROR logging; the optional Fix 4 retry was skipped and
`STREAM_DELETE_ON_JOB_DELETE` defaults to `false` (owner decisions). Verified
live (sid TTL set; ~612 adds → `XLEN` bounded ~11; derived-job delete SREMs the
active set, explicit-target does not) and by 34 passing tests. See
`technical_architecture.md` § Resource retention. One correction to the note
below: `agent_bus`'s `EventBus.publish` does **not** TTL the stream — stream TTLs
come from `StreamCleaner`, driven by client lifecycle (which never fires for the
scheduler), so the original leak was slightly worse than stated.

---

**Status (original):** proposal for the implementation agent to evaluate and apply.
**Scope:** `agent_scheduler` only. No wire/contract changes — the emitted
`EventEnvelope` shape is unchanged; these fixes only bound the keys/streams the
emitter creates and harden the fire path.

## Why

The scheduler is a long-lived **producer** that never "disconnects", so the
cleanup paths `agent_bus` relies on (per-client stream deletion on disconnect +
idle TTL) never fire for scheduler-created resources. A code review confirmed
there is **no** retention/TTL/trim anywhere in `src/` (only the `DELETE /jobs`
endpoint exists, and it removes the APScheduler job only). Left as-is, every
fire leaks state into Valkey without bound.

## Findings (verified in code)

| # | Issue | Location | Severity |
|---|---|---|---|
| 1 | `sid:<cid>` key created per fire, never expires | `emitter.py:84`, `bus_client.py:94` | High |
| 2 | Scheduler stream grows one entry per fire, no `MAXLEN`/trim | `bus_client.py:87`, `emitter.py:113` | High |
| 3 | `streams:active` entry never removed (stale after job delete) | `emitter.py:112`, `api.py` delete route | Low |
| 4 | Emit failure is log-and-drop; orphan `sid` key on partial failure | `emitter.py:77-113` | Medium |

> Mitigation nuance for #1/#2: if an `agent_bus` actor reacts on the same
> `cid`/stream, agent_bus's `WorkflowRegistry.next_sid` TTLs the `sid` key and
> `EventBus.publish` TTLs the stream. So the leak is worst for `schedule.fired`
> events consumed by **non-agent_bus** services or **no** consumer. The fixes
> below make the scheduler self-sufficient regardless of who consumes.

---

## Fix 1 — TTL the `sid:<cid>` key (High)

**Root cause.** `emitter.py:84` does `sid = await _publisher.incr(f"sid:{cid}")`
with a fresh `cid = uuid4()` per fire; `bus_client.py:94` is a bare `INCR`.

**Do NOT** drop the `INCR` and hard-code `sid=1`: the counter must be reserved
so a downstream consumer's `next_sid(cid)` returns `2`, not a colliding `1`.

**Fix.** Set an expiry on the key right after `INCR`, mirroring agent_bus
(`WorkflowRegistry.next_sid` expires the key every step). agent_bus actors that
react will *refresh* this TTL, so it lives for active workflows and self-cleans
otherwise.

```python
# bus_client.py — add to Publisher
async def expire(self, key: str, seconds: int) -> None:
    await self._client.expire(key, seconds)
```

```python
# emitter.py — after minting sid (line ~84)
sid = await _publisher.incr(f"sid:{cid}")
await _publisher.expire(f"sid:{cid}", settings.sid_ttl_s)
```

**Config.** Add `sid_ttl_s` (below). Default **3600** to match agent_bus's
`stream_ttl_s` so cross-service behavior is uniform.

---

## Fix 2 — Cap scheduler streams with `MAXLEN ~` (High)

**Root cause.** `bus_client.py:87-92` `XADD`s with no trim; nothing else trims.

**Decision — MAXLEN vs TTL.** A scheduler stream is a **long-lived shared
channel**, not an ephemeral per-client stream, so **approximate `MAXLEN`** is
the right tool (TTL would delete the whole stream — and its history — between
infrequent fires). `exact=False` (`~`) is the efficient radix-tree trim.

**Fix.** Thread a configurable cap into `publish` (glide API confirmed:
`TrimByMaxLen(exact, threshold, limit)` + `StreamAddOptions(..., trim=...)`):

```python
# bus_client.py
from glide import StreamAddOptions, TrimByMaxLen

async def publish(self, stream: str, env: EventEnvelope) -> str:
    trim = None
    if self._settings.stream_maxlen > 0:
        trim = TrimByMaxLen(exact=False, threshold=self._settings.stream_maxlen)
    entry_id = await self._client.xadd(
        stream, env.to_fields(), StreamAddOptions(make_stream=True, trim=trim)
    )
    return _s(entry_id)
```

**Trade-off to note in the docstring.** `MAXLEN ~ N` means a consumer that falls
more than ~N entries behind loses the oldest unprocessed events. For kick-off
trigger events that's acceptable; pick `N` comfortably above the worst expected
consumer lag. `STREAM_MAXLEN=0` disables trimming (full retention; rely on
external trimming) for callers who want it.

**Config.** Add `stream_maxlen` (below). Suggested default **10000**.

---

## Fix 3 — Clean up `streams:active` on job delete (Low)

**Root cause.** `emitter.py:112` `SADD`s the stream id on every fire (idempotent,
so bounded by distinct streams). The `DELETE /jobs/{job_id}` route removes the
APScheduler job but never `SREM`s the stream id, leaving a stale entry the
agent_bus reaper/observers keep polling.

**Fix (safe subset).** On delete, read the job's `target_stream_id` first:

- **Derived stream** (`target_stream_id` is `None` → stream id == `job_id`):
  `SREM streams:active <job_id>`. Safe because `job_id` is unique, so no other
  job derives that stream.
- **Explicit `target_stream_id`** (a shared "room", possibly used by other jobs
  or external producers): **leave it** — removing a shared stream from the active
  set could blind consumers of other producers.

```python
# bus_client.py — add
async def srem(self, key: str, member: str) -> None:
    await self._client.srem(key, [member])
```

```python
# api.py delete route — before/after remove_job(job_id)
job = sched.get_job(job_id)            # 404 if missing (existing behavior)
target = job.kwargs.get("target_stream_id")
sched.remove_job(job_id)
if target is None:                     # only the derived per-job stream
    await publisher.srem(settings.active_streams_key, job_id)
    # Optional, behind STREAM_DELETE_ON_JOB_DELETE (default false): also drop the
    # stream itself — only do this if losing its history is acceptable.
```

**Owner decision.** Whether to also delete the stream key on job-delete
(`STREAM_DELETE_ON_JOB_DELETE`, default **false**). `SREM` alone is safe and
sufficient to keep the active set clean; the orphaned stream is already bounded
by Fix 2.

> Note: the delete route is currently sync (`def delete_job`). Calling the async
> `srem` needs an `async def` route (FastAPI supports both) or a small run-in-loop
> helper. Implementation agent to wire this cleanly.

---

## Fix 4 — Harden the fire path (Medium)

**Root cause.** `emitter.py` lets any `incr`/`sadd`/`publish` error propagate out
of the job callable. APScheduler logs the job error but: (a) there's no retry, so
a one-shot `date` job's failed emit is **lost**; (b) a `publish` failure *after*
the `incr` leaves an orphan `sid:<cid>`.

**Fixes.**
1. **Orphan key** — largely resolved by Fix 1 (the `sid` key now carries a TTL),
   so a failed fire's key self-expires. No extra work required.
2. **Visibility** — wrap the emit body and log at `ERROR` with `job_id` +
   `scheduled_run_time` before re-raising, so missed fires are unmistakable:
   ```python
   try:
       ...  # incr/expire/build/sadd/publish
   except Exception:
       log.error("emit FAILED job=%s scheduled_run_time=%s", job_id, scheduled_run_time, exc_info=True)
       raise
   ```
3. **Optional transient retry** — a small bounded retry (e.g. 2 attempts, short
   backoff) around `publish` for transient glide errors. Keep it minimal; do
   **not** retry indefinitely inside the event loop. Mark as optional — evaluate
   whether it's worth it given recurring jobs self-cover on the next fire.

**Owner decision.** Whether to add the retry (point 3) and whether a one-shot
`date` job that fails to emit should remain in the store for manual `run`-now vs.
be dropped per APScheduler defaults.

---

## New configuration (consolidated)

Add to `config.py` `Settings` (+ `.env.example`), all env-tunable with safe
defaults:

```python
# --- Resource retention ---
# TTL (seconds) on the per-fire sid:<cid> key. Match agent_bus stream_ttl_s.
sid_ttl_s: int = _int("SID_TTL_S", 3600)
# Approximate MAXLEN cap per scheduler stream (XADD MAXLEN ~ N). 0 = unbounded.
stream_maxlen: int = _int("STREAM_MAXLEN", 10000)
# Optional: also delete the derived stream key when its job is deleted.
stream_delete_on_job_delete: bool = _bool("STREAM_DELETE_ON_JOB_DELETE", False)
```

---

## Suggested order

1. **Fix 1** (sid TTL) — also resolves Fix 4's orphan-key concern.
2. **Fix 2** (stream `MAXLEN`) — the main memory bound.
3. **Fix 3** (`streams:active` cleanup) — low risk, optional stream delete.
4. **Fix 4** (logging; optional retry).

Fixes 1–2 are the load-bearing ones; 3–4 are hygiene/robustness.

## Tests to add / update

- `test_emitter.py`: extend the fake publisher to record calls; assert that a
  fire calls `expire("sid:<cid>", sid_ttl_s)` and that `publish` is invoked with
  a `MAXLEN`-trim option when `stream_maxlen > 0` (and none when `0`).
- `test_emitter.py`: assert that when `publish` raises, the error propagates
  (logged) and no second envelope is emitted.
- `test_api.py`: deleting a **derived**-stream job calls `srem(active_key, job_id)`;
  deleting an **explicit-target** job does **not** `srem`.
- Keep the existing 28 tests green; none of these changes the envelope contract,
  so `test_envelope.py` must be unaffected.

## Acceptance criteria

- A high-frequency interval job (e.g. `seconds: 1`) run for a few minutes leaves:
  - `XLEN stream:<job>` bounded at ≈ `STREAM_MAXLEN` (not growing without limit).
  - no growing population of `sid:*` keys (each carries a TTL and expires).
- Deleting a derived-stream job removes its `streams:active` membership.
- A simulated `publish` failure is logged at `ERROR` with `job_id` +
  `scheduled_run_time`; the orphan `sid` key expires within `SID_TTL_S`.
- `GET /health`, CRUD, pause/resume/run, and the emitted envelope shape are
  unchanged.

## Out of scope (do not bundle here)

- The Socket.IO **room fan-out** (needs an `agent_bus` gateway enhancement —
  `enter_room` + a room-stream observer); already tracked as an agent_bus
  follow-up.
- Migrating off the **vendored `envelope.py`** to a shared contract package —
  separate refactor.
- APScheduler 4.x / horizontal scaling — explicitly out of scope per the
  technical architecture.
