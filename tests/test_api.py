"""CRUD over the API, Valkey-free.

The API uses the shared ``registry`` object and the ``emitter`` module; we
monkeypatch both with in-memory fakes (the same registry instance is imported by
api.py, so patching its methods is enough). The Taskiq worker/scheduler are not
started (``embed=False``).
"""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch):
    from agent_scheduler import emitter
    from agent_scheduler.models import JobDefinition
    from agent_scheduler.registry import registry as reg

    store: dict[str, JobDefinition] = {}

    async def _noop(*a, **k):
        return None

    async def _ping():
        return True

    async def _exists(jid):
        return jid in store

    async def _get(jid):
        return store.get(jid)

    async def _list():
        return list(store.values())

    async def _put(jd):
        store[jd.job_id] = jd

    async def _delete(jid):
        return store.pop(jid, None) is not None

    for name, fn in {
        "connect": _noop, "close": _noop, "ping": _ping, "exists": _exists,
        "get": _get, "list": _list, "put": _put, "delete": _delete,
    }.items():
        monkeypatch.setattr(reg, name, fn)

    emitted: list[dict] = []

    async def _emit(**kw):
        emitted.append(kw)
        return "entry-1"

    monkeypatch.setattr(emitter, "connect", _noop)
    monkeypatch.setattr(emitter, "close", _noop)
    monkeypatch.setattr(emitter, "ping", _ping)
    monkeypatch.setattr(emitter, "emit_scheduled_event", _emit)
    monkeypatch.setattr(emitter, "deregister_stream", _noop)

    from agent_scheduler.api import create_app

    with TestClient(create_app(embed=False, connect_emitter=True)) as c:
        c.emitted = emitted  # type: ignore[attr-defined]
        yield c


_CRON = {"job_id": "j1", "trigger_type": "cron",
         "trigger_args": {"cron_expression": "0 7 * * *", "timezone": "Europe/Lisbon"},
         "target_stream_id": "agent-runtime", "event_data": {"agent": "news"}}


def test_create_and_get(client):
    r = client.post("/jobs", json=_CRON)
    assert r.status_code == 201
    v = r.json()
    assert v["job_id"] == "j1" and v["paused"] is False
    assert v["next_run_time"] is not None
    assert client.get("/jobs/j1").json()["event_data"] == {"agent": "news"}


def test_duplicate_is_409(client):
    client.post("/jobs", json=_CRON)
    assert client.post("/jobs", json=_CRON).status_code == 409


def test_invalid_cron_is_422(client):
    bad = {**_CRON, "trigger_args": {"cron_expression": "not a cron"}}
    assert client.post("/jobs", json=bad).status_code == 422


def test_unknown_is_404(client):
    assert client.get("/jobs/nope").status_code == 404


def test_list(client):
    client.post("/jobs", json=_CRON)
    client.post("/jobs", json={**_CRON, "job_id": "j2"})
    assert {j["job_id"] for j in client.get("/jobs").json()} == {"j1", "j2"}


def test_patch_event_data(client):
    client.post("/jobs", json=_CRON)
    r = client.patch("/jobs/j1", json={"event_data": {"agent": "other"}})
    assert r.status_code == 200 and r.json()["event_data"] == {"agent": "other"}


def test_pause_resume(client):
    client.post("/jobs", json=_CRON)
    assert client.post("/jobs/j1/pause").json()["paused"] is True
    assert client.get("/jobs/j1").json()["next_run_time"] is None
    assert client.post("/jobs/j1/resume").json()["paused"] is False
    assert client.get("/jobs/j1").json()["next_run_time"] is not None


def test_run_now_emits(client):
    client.post("/jobs", json=_CRON)
    r = client.post("/jobs/j1/run")
    assert r.status_code == 202 and r.json()["status"] == "fired"
    assert client.emitted and client.emitted[-1]["job_id"] == "j1"


def test_delete(client):
    client.post("/jobs", json=_CRON)
    assert client.delete("/jobs/j1").status_code == 204
    assert client.get("/jobs/j1").status_code == 404


def test_health(client):
    assert client.get("/health").json()["status"] == "ok"
