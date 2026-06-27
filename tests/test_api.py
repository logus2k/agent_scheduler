"""CRUD over the API against a Valkey-free MemoryJobStore scheduler."""

import pytest
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi.testclient import TestClient

from agent_scheduler import emitter
from agent_scheduler.api import create_app
from agent_scheduler.envelope import EventEnvelope


class FakePublisher:
    def __init__(self):
        self.published: list[tuple[str, EventEnvelope]] = []
        self._counter = 0

    async def incr(self, key: str) -> int:
        self._counter += 1
        return self._counter

    async def sadd(self, key: str, member: str) -> None:
        pass

    async def publish(self, stream: str, env: EventEnvelope) -> str:
        self.published.append((stream, env))
        return "1-0"

    async def ping(self) -> bool:
        return True


@pytest.fixture
def client():
    scheduler = AsyncIOScheduler(jobstores={"default": MemoryJobStore()})
    app = create_app(scheduler, connect_emitter=False)
    with TestClient(app) as c:
        yield c


def _interval_body(job_id="j1", **over):
    body = {
        "job_id": job_id,
        "trigger_type": "interval",
        "trigger_args": {"seconds": 300},
        "event_data": {"ping": 1},
    }
    body.update(over)
    return body


def test_create_list_get(client):
    r = client.post("/jobs", json=_interval_body())
    assert r.status_code == 201
    view = r.json()
    assert view["job_id"] == "j1"
    assert view["resolved_stream"] == "stream:j1"
    assert view["event_type"] == "schedule.fired"
    assert view["paused"] is False

    assert len(client.get("/jobs").json()) == 1
    assert client.get("/jobs/j1").json()["job_id"] == "j1"


def test_duplicate_conflict(client):
    assert client.post("/jobs", json=_interval_body()).status_code == 201
    assert client.post("/jobs", json=_interval_body()).status_code == 409


def test_explicit_target_stream(client):
    r = client.post("/jobs", json=_interval_body(job_id="j2", target_stream_id="room-x"))
    assert r.json()["resolved_stream"] == "stream:room-x"


def test_invalid_trigger_args(client):
    body = _interval_body(job_id="bad")
    body["trigger_args"] = {}
    assert client.post("/jobs", json=body).status_code == 422


def test_invalid_job_id(client):
    assert client.post("/jobs", json=_interval_body(job_id="bad id!")).status_code == 422


def test_pause_resume(client):
    client.post("/jobs", json=_interval_body())
    assert client.post("/jobs/j1/pause").json()["paused"] is True
    assert client.post("/jobs/j1/resume").json()["paused"] is False


def test_create_paused(client):
    r = client.post("/jobs", json=_interval_body(paused=True))
    assert r.json()["paused"] is True


def test_update_payload_and_trigger(client):
    client.post("/jobs", json=_interval_body())
    r = client.patch("/jobs/j1", json={"event_data": {"ping": 2}})
    assert r.json()["event_data"] == {"ping": 2}
    r = client.patch(
        "/jobs/j1",
        json={"trigger_type": "interval", "trigger_args": {"minutes": 15}},
    )
    assert r.status_code == 200


def test_update_partial_trigger_422(client):
    client.post("/jobs", json=_interval_body())
    assert client.patch("/jobs/j1", json={"trigger_type": "interval"}).status_code == 422


def test_delete(client):
    client.post("/jobs", json=_interval_body())
    assert client.delete("/jobs/j1").status_code == 204
    assert client.get("/jobs/j1").status_code == 404


def test_missing_job_404(client):
    assert client.get("/jobs/nope").status_code == 404
    assert client.post("/jobs/nope/pause").status_code == 404
    assert client.delete("/jobs/nope").status_code == 404


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_run_now(client, monkeypatch):
    fake = FakePublisher()
    monkeypatch.setattr(emitter, "_publisher", fake)
    client.post("/jobs", json=_interval_body())
    r = client.post("/jobs/j1/run")
    assert r.status_code == 202
    assert fake.published
    stream, env = fake.published[0]
    assert stream == "stream:j1"
    assert env.payload.data == {"ping": 1}
