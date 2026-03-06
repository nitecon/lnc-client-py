"""Lance Producer — batched message production with ack handling.

Collects records until batch_size OR linger_ms is reached, then sends
an Ingest frame and waits for server ACK. Supports optional LZ4 compression.

Example::

    producer = await Producer.connect("10.0.10.11:1992", ProducerConfig())
    await producer.send(topic="my-events", data=b'{"price": 6942.25}')
    await producer.send_async(topic="my-events", data=b'fire and forget')
    await producer.flush()
    await producer.close()
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import warnings

from lnc_client.config import ClientConfig, ProducerConfig
from lnc_client.connection import LwpConnection
from lnc_client.errors import ConnectionError, LanceError
from lnc_client.protocol import (
    ControlCommand,
    build_ingest_frame,
)
from lnc_client.tlv import RecordType, TlvRecord, encode_records

log = logging.getLogger("lnc_client.producer")


class Producer:
    """Batched Lance producer with ack tracking."""

    def __init__(self, conn: LwpConnection, config: ProducerConfig) -> None:
        self._conn = conn
        self._config = config
        self._batch_id: int = 0
        self._pending_acks: dict[int, asyncio.Future] = {}
        self._ack_reader_task: asyncio.Task | None = None
        self._closed = False
        self._host: str = ""
        self._port: int = 1992
        self._topic_cache: dict[str, int] = {}

    @classmethod
    async def connect(
        cls,
        address: str,
        config: ProducerConfig | None = None,
    ) -> Producer:
        """Connect to a Lance server and create a Producer.

        Args:
            address: "host:port" string.
            config: Producer configuration.
        """
        cfg = config or ProducerConfig()
        host, _, port_str = address.rpartition(":")
        port = int(port_str) if port_str else 1992
        if not host:
            host = address

        conn = LwpConnection(
            host,
            port,
            keepalive_interval_s=cfg.keepalive_interval_s,
            connect_timeout_s=cfg.connect_timeout_s,
            ssl_context=cfg.ssl_context,
        )
        await conn.connect()

        prod = cls(conn, cfg)
        prod._host = host
        prod._port = port
        prod._ack_reader_task = asyncio.create_task(prod._ack_reader_loop())
        return prod

    async def close(self) -> None:
        """Close the producer and its connection."""
        self._closed = True
        if self._ack_reader_task:
            self._ack_reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ack_reader_task
        # Fail any remaining pending acks
        for fut in self._pending_acks.values():
            if not fut.done():
                fut.set_exception(ConnectionError("Producer closed"))
        self._pending_acks.clear()
        await self._conn.close()

    async def send(
        self,
        topic: str | int | None = None,
        data: bytes = b"",
        *,
        topic_id: int | None = None,
        record_type: int = RecordType.RAW_DATA,
    ) -> int:
        """Send data and wait for server ACK. Returns batch_id.

        Args:
            topic: Topic name (str) or numeric topic ID (int).
            data: Payload bytes.
            topic_id: Deprecated — use ``topic`` instead.
            record_type: TLV record type.
        """
        resolved = await self._resolve_topic_arg(topic, topic_id)
        batch_id = await self._send_internal(resolved, data, record_type=record_type)
        await self._wait_ack(batch_id)
        return batch_id

    async def send_async(
        self,
        topic: str | int | None = None,
        data: bytes = b"",
        *,
        topic_id: int | None = None,
        record_type: int = RecordType.RAW_DATA,
    ) -> int:
        """Send data without waiting for ACK (pipelined). Returns batch_id.

        Args:
            topic: Topic name (str) or numeric topic ID (int).
            data: Payload bytes.
            topic_id: Deprecated — use ``topic`` instead.
            record_type: TLV record type.
        """
        resolved = await self._resolve_topic_arg(topic, topic_id)
        return await self._send_internal(resolved, data, record_type=record_type)

    async def send_batch(
        self,
        topic: str | int | None = None,
        records: list[TlvRecord] | None = None,
        *,
        topic_id: int | None = None,
    ) -> int:
        """Send multiple TLV records as a single batch. Waits for ACK.

        Args:
            topic: Topic name (str) or numeric topic ID (int).
            records: List of TLV records.
            topic_id: Deprecated — use ``topic`` instead.
        """
        if records is None:
            records = []
        if self._closed:
            raise ConnectionError("Producer is closed")

        resolved = await self._resolve_topic_arg(topic, topic_id)
        payload = encode_records(records)

        compressed = False
        if self._config.compression:
            import lz4.block

            compressed_payload = lz4.block.compress(payload, store_size=False)
            if len(compressed_payload) < len(payload):
                payload = compressed_payload
                compressed = True

        self._batch_id += 1
        batch_id = self._batch_id

        frame = build_ingest_frame(
            payload=payload,
            batch_id=batch_id,
            record_count=len(records),
            topic_id=resolved,
            compressed=compressed,
        )

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending_acks[batch_id] = fut

        await self._conn.send_frame(frame)
        await self._wait_ack(batch_id)
        return batch_id

    async def flush(self, timeout: float = 30.0) -> None:
        """Wait for all pending ACKs to be resolved.

        Ensures all previously sent records have been acknowledged by the server.
        """
        if not self._pending_acks:
            return

        pending = list(self._pending_acks.values())
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError as e:
            raise LanceError(f"Flush timed out with {len(self._pending_acks)} pending ACKs") from e

    # ----- internal -----

    async def _resolve_topic_arg(
        self,
        topic: str | int | None,
        topic_id: int | None,
    ) -> int:
        """Resolve the topic argument to a numeric ID."""
        if topic is None and topic_id is not None:
            warnings.warn(
                "topic_id= is deprecated, use topic= instead",
                DeprecationWarning,
                stacklevel=3,
            )
            topic = topic_id
        if topic is None:
            raise ValueError("topic is required")
        if isinstance(topic, int):
            return topic
        return await self._resolve_topic(topic)

    async def _resolve_topic(self, name: str) -> int:
        """Resolve a topic name to its numeric ID, using cache when possible."""
        cached = self._topic_cache.get(name)
        if cached is not None:
            return cached

        from lnc_client.client import LanceClient

        mgmt_cfg = ClientConfig(
            host=self._host,
            port=self._port,
            connect_timeout_s=self._config.connect_timeout_s,
            ssl_context=self._config.ssl_context,
        )
        mgmt = LanceClient(mgmt_cfg)
        try:
            await mgmt.connect()
            info = await mgmt.ensure_topic_default(name)
            self._topic_cache[name] = info.id
            return info.id
        finally:
            await mgmt.close()

    async def _send_internal(
        self,
        topic_id: int,
        data: bytes,
        *,
        record_type: int = RecordType.RAW_DATA,
    ) -> int:
        """Internal send — takes a resolved numeric topic_id."""
        if self._closed:
            raise ConnectionError("Producer is closed")

        # Encode as TLV
        record = TlvRecord(record_type=record_type, value=data)
        payload = record.encode()

        # Optional LZ4 compression
        compressed = False
        if self._config.compression:
            import lz4.block

            compressed_payload = lz4.block.compress(payload, store_size=False)
            if len(compressed_payload) < len(payload):
                payload = compressed_payload
                compressed = True

        self._batch_id += 1
        batch_id = self._batch_id

        frame = build_ingest_frame(
            payload=payload,
            batch_id=batch_id,
            record_count=1,
            topic_id=topic_id,
            compressed=compressed,
        )

        # Register pending ack future
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending_acks[batch_id] = fut

        # Check backpressure
        if self._conn.under_backpressure:
            log.warning("Under backpressure, delaying send")
            await asyncio.sleep(0.1)

        await self._conn.send_frame(frame)
        return batch_id

    async def _wait_ack(self, batch_id: int, timeout: float = 30.0) -> None:
        """Wait for a specific batch_id to be acknowledged."""
        fut = self._pending_acks.get(batch_id)
        if fut is None:
            return

        try:
            await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError as e:
            self._pending_acks.pop(batch_id, None)
            raise LanceError(f"ACK timeout for batch {batch_id}") from e

    async def _ack_reader_loop(self) -> None:
        """Background task that reads ACK frames and resolves pending futures."""
        try:
            while not self._closed and self._conn.connected:
                try:
                    header, payload = await self._conn.recv_frame(timeout=5.0)
                except Exception:
                    if self._closed:
                        break
                    # Short timeout is expected when idle
                    continue

                if header.is_ack:
                    fut = self._pending_acks.pop(header.batch_id, None)
                    if fut and not fut.done():
                        fut.set_result(header.batch_id)
                elif header.is_control:
                    cmd = header.command
                    if cmd == ControlCommand.ERROR_RESPONSE:
                        log.error(
                            "Server error: %s",
                            payload.decode("utf-8", errors="replace"),
                        )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("ACK reader error: %s", e)
