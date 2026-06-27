"""The vendored envelope must stay byte-compatible with agent_bus's contract."""

from agent_scheduler.envelope import (
    SCHEMA_VERSION,
    WIRE_FIELD,
    EventEnvelope,
    EventType,
    new_event,
)


def test_wire_field_and_version():
    assert WIRE_FIELD == "data"
    assert SCHEMA_VERSION == "1.0"
    assert EventType.SCHEDULE_FIRED == "schedule.fired"


def test_round_trip_and_to_fields():
    env = new_event(
        stream_id="s1",
        cid="c1",
        sid=1,
        sender="agent_scheduler",
        event_type=EventType.SCHEDULE_FIRED,
        data={"k": "v"},
        context={"job_id": "j1"},
    )
    fields = env.to_fields()
    assert len(fields) == 1
    field, value = fields[0]
    assert field == WIRE_FIELD

    # Simulate glide's bytes-keyed read result and parse it back.
    parsed = EventEnvelope.from_fields([[field.encode(), value.encode()]])
    assert parsed.header.stream_id == "s1"
    assert parsed.header.event_type == "schedule.fired"
    assert parsed.payload.data == {"k": "v"}
    assert parsed.payload.context == {"job_id": "j1"}
    assert parsed.metadata.version == "1.0"


def test_from_fields_missing_field_raises():
    import pytest

    with pytest.raises(ValueError):
        EventEnvelope.from_fields([[b"other", b"{}"]])
