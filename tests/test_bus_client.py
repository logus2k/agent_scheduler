"""Publisher applies an approximate MAXLEN trim per STREAM_MAXLEN (Fix 2)."""

from dataclasses import replace

from agent_scheduler.bus_client import Publisher
from agent_scheduler.config import settings
from agent_scheduler.envelope import EventType, new_event


class FakeGlide:
    def __init__(self):
        self.xadds = []

    async def xadd(self, stream, values, options):
        self.xadds.append((stream, values, options))
        return b"1-0"


def _env():
    return new_event(
        stream_id="s", cid="c", sid=1, sender="agent_scheduler",
        event_type=EventType.SCHEDULE_FIRED, data={"k": "v"},
    )


async def test_publish_trims_when_maxlen_set():
    client = FakeGlide()
    pub = Publisher(client, replace(settings, stream_maxlen=5))
    await pub.publish("stream:x", _env())
    opts = client.xadds[0][2]
    assert opts.trim is not None
    assert opts.trim.threshold == 5
    assert opts.trim.exact is False  # approximate (~) trim


async def test_publish_no_trim_when_zero():
    client = FakeGlide()
    pub = Publisher(client, replace(settings, stream_maxlen=0))
    await pub.publish("stream:x", _env())
    assert client.xadds[0][2].trim is None
