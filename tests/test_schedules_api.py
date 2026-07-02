"""Schedule + Binding CRUD API and the jobs->schedules migration endpoint.

Valkey-free: the shared ``registry`` object is monkeypatched with in-memory
stores for jobs, schedules, and bindings; the emitter is a recorder. Taskiq is
not embedded (``embed=False``).
"""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch):
    from agent_scheduler import emitter
    from agent_scheduler.models import Binding, JobDefinition, Schedule
    from agent_scheduler.registry import registry as reg

    jobs: dict[str, JobDefinition] = {}
    schedules: dict[str, Schedule] = {}
    bindings: dict[str, Binding] = {}

    async def _noop(*a, **k):
        return None

    async def _ping():
        return True

    # jobs
    async def _exists(jid):
        return jid in jobs

    async def _get(jid):
        return jobs.get(jid)

    async def _list():
        return list(jobs.values())

    async def _put(jd):
        jobs[jd.job_id] = jd

    async def _delete(jid):
        return jobs.pop(jid, None) is not None

    # schedules
    async def _sched_exists(sid):
        return sid in schedules

    async def _get_schedule(sid):
        return schedules.get(sid)

    async def _list_schedules():
        return list(schedules.values())

    async def _put_schedule(sch):
        schedules[sch.schedule_id] = sch

    async def _delete_schedule(sid):
        return schedules.pop(sid, None) is not None

    # bindings
    async def _binding_exists(bid):
        return bid in bindings

    async def _get_binding(bid):
        return bindings.get(bid)

    async def _list_bindings():
        return list(bindings.values())

    async def _bindings_for(sid):
        return [b for b in bindings.values() if b.schedule_id == sid]

    async def _put_binding(b):
        bindings[b.binding_id] = b

    async def _delete_binding(bid):
        return bindings.pop(bid, None) is not None

    for name, fn in {
        "connect": _noop, "close": _noop, "ping": _ping, "exists": _exists,
        "get": _get, "list": _list, "put": _put, "delete": _delete,
        "schedule_exists": _sched_exists, "get_schedule": _get_schedule,
        "list_schedules": _list_schedules, "put_schedule": _put_schedule,
        "delete_schedule": _delete_schedule, "binding_exists": _binding_exists,
        "get_binding": _get_binding, "list_bindings": _list_bindings,
        "bindings_for": _bindings_for, "put_binding": _put_binding,
        "delete_binding": _delete_binding,
    }.items():
        monkeypatch.setattr(reg, name, fn)

    emitted: list[dict] = []

    async def _emit(**kw):
        emitted.append(kw)
        return f"entry-{len(emitted)}"

    monkeypatch.setattr(emitter, "connect", _noop)
    monkeypatch.setattr(emitter, "close", _noop)
    monkeypatch.setattr(emitter, "ping", _ping)
    monkeypatch.setattr(emitter, "emit_scheduled_event", _emit)
    monkeypatch.setattr(emitter, "deregister_stream", _noop)

    from agent_scheduler.api import create_app

    with TestClient(create_app(embed=False, connect_emitter=True)) as c:
        c.emitted = emitted            # type: ignore[attr-defined]
        c.jobs = jobs                  # type: ignore[attr-defined]
        c.schedules = schedules        # type: ignore[attr-defined]
        c.bindings = bindings          # type: ignore[attr-defined]
        yield c


_SCHED = {
    "schedule_id": "s1",
    "trigger_type": "cron",
    "trigger_args": {"cron_expression": "0 7 * * *", "timezone": "Europe/Lisbon"},
}


# --- Schedule CRUD ----------------------------------------------------------

def test_create_schedule_is_payload_free(client):
    r = client.post("/schedules", json=_SCHED)
    assert r.status_code == 201, r.text
    v = r.json()
    assert v["schedule_id"] == "s1"
    assert v["n_bindings"] == 0
    assert v["next_run_time"] is not None
    # the view exposes cron/tz only — no target/event fields
    assert "event_data" not in v and "target_stream_id" not in v


def test_create_schedule_rejects_extra_payload_fields(client):
    # Pydantic ignores unknown fields by default; the stored schedule must not
    # carry them. Send a welded-looking body and confirm it round-trips clean.
    r = client.post("/schedules", json={**_SCHED, "event_data": {"x": 1},
                                        "target_stream_id": "z"})
    assert r.status_code == 201
    assert not hasattr(client.schedules["s1"], "event_data")


def test_duplicate_schedule_is_409(client):
    client.post("/schedules", json=_SCHED)
    assert client.post("/schedules", json=_SCHED).status_code == 409


def test_invalid_cron_schedule_is_422(client):
    bad = {**_SCHED, "trigger_args": {"cron_expression": "not a cron"}}
    assert client.post("/schedules", json=bad).status_code == 422


def test_get_and_list_schedules(client):
    client.post("/schedules", json=_SCHED)
    client.post("/schedules", json={**_SCHED, "schedule_id": "s2"})
    assert client.get("/schedules/s1").json()["schedule_id"] == "s1"
    assert {s["schedule_id"] for s in client.get("/schedules").json()} == {"s1", "s2"}


def test_schedule_404(client):
    assert client.get("/schedules/nope").status_code == 404


def test_patch_schedule_trigger(client):
    client.post("/schedules", json=_SCHED)
    r = client.patch("/schedules/s1", json={
        "trigger_type": "cron",
        "trigger_args": {"cron_expression": "30 6 * * *"},
    })
    assert r.status_code == 200
    assert r.json()["trigger_args"]["cron_expression"] == "30 6 * * *"


def test_patch_schedule_partial_trigger_is_422(client):
    client.post("/schedules", json=_SCHED)
    r = client.patch("/schedules/s1", json={"trigger_type": "cron"})
    assert r.status_code == 422


def test_pause_resume_schedule(client):
    client.post("/schedules", json=_SCHED)
    assert client.post("/schedules/s1/pause").json()["paused"] is True
    assert client.get("/schedules/s1").json()["next_run_time"] is None
    assert client.post("/schedules/s1/resume").json()["paused"] is False
    assert client.get("/schedules/s1").json()["next_run_time"] is not None


def test_delete_schedule_cascades_bindings(client):
    client.post("/schedules", json=_SCHED)
    client.post("/schedules/s1/bindings", json={"binding_id": "b1"})
    assert client.delete("/schedules/s1").status_code == 204
    assert client.get("/schedules/s1").status_code == 404
    assert client.bindings == {}  # binding cascaded away


# --- Binding CRUD -----------------------------------------------------------

def _mk_schedule(client):
    client.post("/schedules", json=_SCHED)


def test_create_and_count_bindings(client):
    _mk_schedule(client)
    r = client.post("/schedules/s1/bindings", json={
        "binding_id": "b1", "target_stream_id": "agent-runtime",
        "event_data": {"agent": "news"}, "room": "ops",
    })
    assert r.status_code == 201, r.text
    v = r.json()
    assert v["binding_id"] == "b1" and v["schedule_id"] == "s1"
    assert v["event_data"] == {"agent": "news"}
    # n_bindings on the schedule view reflects the new binding
    assert client.get("/schedules/s1").json()["n_bindings"] == 1


def test_binding_requires_existing_schedule(client):
    assert client.post("/schedules/missing/bindings",
                       json={"binding_id": "b1"}).status_code == 404


def test_duplicate_binding_is_409(client):
    _mk_schedule(client)
    client.post("/schedules/s1/bindings", json={"binding_id": "b1"})
    assert client.post("/schedules/s1/bindings",
                       json={"binding_id": "b1"}).status_code == 409


def test_list_bindings_scoped_to_schedule(client):
    _mk_schedule(client)
    client.post("/schedules", json={**_SCHED, "schedule_id": "s2"})
    client.post("/schedules/s1/bindings", json={"binding_id": "b1"})
    client.post("/schedules/s2/bindings", json={"binding_id": "b2"})
    ids = {b["binding_id"] for b in client.get("/schedules/s1/bindings").json()}
    assert ids == {"b1"}


def test_get_binding_wrong_schedule_is_404(client):
    _mk_schedule(client)
    client.post("/schedules", json={**_SCHED, "schedule_id": "s2"})
    client.post("/schedules/s1/bindings", json={"binding_id": "b1"})
    # b1 belongs to s1, not s2
    assert client.get("/schedules/s2/bindings/b1").status_code == 404


def test_patch_binding(client):
    _mk_schedule(client)
    client.post("/schedules/s1/bindings", json={"binding_id": "b1",
                                                "event_data": {"a": 1}})
    r = client.patch("/schedules/s1/bindings/b1",
                     json={"event_data": {"a": 2}, "room": "r"})
    assert r.status_code == 200
    assert r.json()["event_data"] == {"a": 2} and r.json()["room"] == "r"


def test_delete_binding(client):
    _mk_schedule(client)
    client.post("/schedules/s1/bindings", json={"binding_id": "b1"})
    assert client.delete("/schedules/s1/bindings/b1").status_code == 204
    assert client.get("/schedules/s1").json()["n_bindings"] == 0


# --- run-now fan-out --------------------------------------------------------

def test_run_schedule_fans_out_over_bindings(client):
    _mk_schedule(client)
    client.post("/schedules/s1/bindings", json={"binding_id": "b1",
                                                "target_stream_id": "a"})
    client.post("/schedules/s1/bindings", json={"binding_id": "b2",
                                                "target_stream_id": "b"})
    r = client.post("/schedules/s1/run")
    assert r.status_code == 202
    assert len(r.json()["emitted"]) == 2
    assert {e["job_id"] for e in client.emitted} == {"b1", "b2"}


def test_run_schedule_zero_bindings_emits_nothing(client):
    _mk_schedule(client)
    r = client.post("/schedules/s1/run")
    assert r.status_code == 202 and r.json()["emitted"] == []
    assert client.emitted == []


# --- migration endpoint -----------------------------------------------------

def test_migrate_jobs_to_schedules(client):
    # Seed a welded job via the compat /jobs façade.
    client.post("/jobs", json={
        "job_id": "news", "trigger_type": "cron",
        "trigger_args": {"cron_expression": "0 7 * * *", "timezone": "Europe/Lisbon"},
        "target_stream_id": "agent-runtime", "event_data": {"agent": "news"},
    })
    r = client.post("/migrate/jobs-to-schedules")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    m = body["migrated"][0]
    assert m["job_id"] == "news" and m["schedule_id"] == "news"

    # The migrated schedule + binding are now queryable and faithful.
    sv = client.get("/schedules/news").json()
    assert sv["n_bindings"] == 1
    bv = client.get("/schedules/news/bindings").json()[0]
    assert bv["target_stream_id"] == "agent-runtime"
    assert bv["event_data"] == {"agent": "news"}

    # Reversible: source job still present by default.
    assert client.get("/jobs/news").status_code == 200


def test_migrate_is_idempotent(client):
    client.post("/jobs", json={
        "job_id": "j", "trigger_type": "cron",
        "trigger_args": {"cron_expression": "0 7 * * *"},
    })
    client.post("/migrate/jobs-to-schedules")
    client.post("/migrate/jobs-to-schedules")
    # exactly one schedule + one binding, not duplicated
    assert len(client.schedules) == 1
    assert len(client.bindings) == 1


def test_migrate_delete_jobs_flag(client):
    client.post("/jobs", json={
        "job_id": "j", "trigger_type": "cron",
        "trigger_args": {"cron_expression": "0 7 * * *"},
    })
    r = client.post("/migrate/jobs-to-schedules?delete_jobs=true")
    assert r.json()["jobs_deleted"] is True
    assert client.get("/jobs/j").status_code == 404
    assert client.get("/schedules/j").status_code == 200
