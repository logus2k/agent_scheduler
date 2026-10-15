"""Consumer — the glide (direct-Valkey) *read* facade for bus ingress.

The scheduler is normally a pure producer (see :mod:`bus_client`). The one place
it consumes is the ``schedule.requested`` control stream, where agents ask it to
register recurring work. This module is the deliberately-thin read counterpart to
``Publisher``: a consumer-group tail with ack, crash-recovery (XAUTOCLAIM) and
poison routing — mirroring the semantics of agent_bus's own ``BusClient`` so a
scheduled request is handled exactly like any other bus message.

Kept separate from ``Publisher`` so that module stays true to its "producer only"
contract. Same wire client (valkey-glide) and vendored ``EventEnvelope`` as the
rest of the service, so requests are indistinguishable on the wire.

Semantics (preserved from agent_bus):
  * **At-least-once** — a redelivered entry (after reclaim) is expected; the
    ingress handler is idempotent (upsert by schedule_id), so it dedupes itself.
  * **Poison routing** — an unparseable entry goes to the DLQ *and* is acked, so
    one bad message can't wedge the group.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from typing import Iterable, NamedTuple, Optional

from glide import (
    GlideClient,
    GlideClientConfiguration,
    NodeAddress,
    RequestError,
    StreamAddOptions,
    StreamGroupOptions,
    StreamReadGroupOptions,
)

from .config import Settings, settings as default_settings
from .envelope import WIRE_FIELD, EventEnvelope

log = logging.getLogger("agent_scheduler.bus_consumer")


def _s(value) -> str:
    """Decode glide's bytes results to str (pass through str/None)."""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return value


def make_consumer(name: str) -> str:
    """A consumer id encoding host+pid so multiple instances of the service share
    one group and load-balance (matches agent_bus's BaseActor convention)."""
    return f"{name}-{socket.gethostname()}-{os.getpid()}"


class Delivery(NamedTuple):
    """One successfully-parsed stream entry. ``.entry_id`` acks it; ``.envelope``
    carries the message (``.header.cid`` + ``.sid`` = idempotency key)."""

    stream: str
    entry_id: str
    envelope: EventEnvelope


class Consumer:
    """Async glide read facade scoped to consumer-group ingress."""

    def __init__(self, client: GlideClient, settings: Settings):
        self._client = client
        self._settings = settings

    @classmethod
    async def create(cls, settings: Settings = default_settings) -> "Consumer":
        """Connect with the same bounded retry loop as ``Publisher`` so startup is
        forgiving when valkey-bus (owned by agent_bus) comes up late."""
        config = GlideClientConfiguration(
            [NodeAddress(settings.valkey_host, settings.valkey_port)]
        )
        last_exc: Exception | None = None
        for attempt in range(1, settings.connect_retries + 1):
            try:
                client = await GlideClient.create(config)
                log.info(
                    "Consumer connected to %s:%s",
                    settings.valkey_host,
                    settings.valkey_port,
                )
                return cls(client, settings)
            except Exception as exc:  # noqa: BLE001 - retry any connection failure
                last_exc = exc
                log.warning(
                    "valkey-bus not reachable (attempt %d/%d): %s",
                    attempt,
                    settings.connect_retries,
                    exc,
                )
                await asyncio.sleep(settings.connect_retry_delay_s)
        raise RuntimeError(
            f"could not connect to valkey-bus at "
            f"{settings.valkey_host}:{settings.valkey_port}"
        ) from last_exc

    async def close(self) -> None:
        await self._client.close()

    async def ping(self) -> bool:
        try:
            await self._client.ping()
            return True
        except Exception:  # noqa: BLE001
            return False

    async def ensure_group(self, stream: str, group: str, start: str = "0") -> None:
        """XGROUP CREATE (with MKSTREAM), idempotent (BUSYGROUP-safe).
        ``start="0"`` delivers any backlog once; ``"$"`` only new messages."""
        try:
            await self._client.xgroup_create(
                stream, group, start, StreamGroupOptions(make_stream=True)
            )
        except RequestError as exc:
            if "BUSYGROUP" in str(exc):
                return
            raise

    async def read_group(
        self,
        streams: Iterable[str],
        group: str,
        consumer: str,
        count: int = 32,
        block_ms: Optional[int] = None,
    ) -> list[Delivery]:
        """XREADGROUP new (``>``) messages. Poison/trimmed entries are DLQ'd (as
        needed) + acked here and never returned."""
        keys_and_ids = {s: ">" for s in streams}
        if not keys_and_ids:
            return []
        options = StreamReadGroupOptions(count=count, block_ms=block_ms)
        result = await self._client.xreadgroup(keys_and_ids, group, consumer, options)
        if not result:
            return []
        deliveries: list[Delivery] = []
        for stream_b, entries in result.items():
            stream = _s(stream_b)
            if not entries:
                continue
            for entry_id_b, fields in entries.items():
                entry_id = _s(entry_id_b)
                if fields is None:  # trimmed/deleted from the stream
                    await self.ack(stream, group, [entry_id])
                    continue
                try:
                    env = EventEnvelope.from_fields(fields)
                except Exception as exc:  # noqa: BLE001 - poison
                    await self.dead_letter(stream, entry_id, fields, str(exc))
                    await self.ack(stream, group, [entry_id])
                    continue
                deliveries.append(Delivery(stream, entry_id, env))
        return deliveries

    async def ack(self, stream: str, group: str, entry_ids: list[str]) -> int:
        if not entry_ids:
            return 0
        return await self._client.xack(stream, group, entry_ids)

    async def reclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_ms: int,
        start: str = "0-0",
        count: int = 32,
    ) -> tuple[str, list[Delivery]]:
        """XAUTOCLAIM idle pending entries (crash recovery). Returns the next
        cursor and the reclaimed deliveries (poison → DLQ + ack)."""
        res = await self._client.xautoclaim(
            stream, group, consumer, min_idle_ms, start, count
        )
        next_cursor = _s(res[0])
        claimed = res[1] if len(res) > 1 and res[1] else {}
        deliveries: list[Delivery] = []
        for entry_id_b, fields in claimed.items():
            entry_id = _s(entry_id_b)
            if fields is None:
                await self.ack(stream, group, [entry_id])
                continue
            try:
                env = EventEnvelope.from_fields(fields)
            except Exception as exc:  # noqa: BLE001
                await self.dead_letter(stream, entry_id, fields, str(exc))
                await self.ack(stream, group, [entry_id])
                continue
            deliveries.append(Delivery(stream, entry_id, env))
        return next_cursor, deliveries

    async def dead_letter(self, stream: str, entry_id: str, fields, error: str) -> None:
        """Route an unprocessable entry (raw payload + error) to the DLQ.

        ``fields`` may be glide's raw ``[[field, value], ...]`` or a plain string
        (a logically-invalid but well-formed envelope the handler rejected)."""
        raw = ""
        try:
            if isinstance(fields, str):
                raw = fields
            else:
                for pair in fields:
                    if _s(pair[0]) == WIRE_FIELD:
                        raw = _s(pair[1])
                        break
        except Exception:  # noqa: BLE001
            raw = repr(fields)
        await self._client.xadd(
            self._settings.dlq_stream,
            [
                ("source_stream", stream),
                ("source_id", entry_id),
                ("error", error),
                ("raw", raw),
            ],
            StreamAddOptions(make_stream=True),
        )
