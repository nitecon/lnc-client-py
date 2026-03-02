"""Lance Producer — batched message production with ack handling.

Collects records until batch_size OR linger_ms is reached, then sends
an Ingest frame and waits for server ACK. Supports optional LZ4 compression.

Example (name-based, preferred)::

    producer, topic_id = await Producer.connect_for_topic(
        "10.0.10.11:1992", "rithmic-dev", ProducerConfig()
    )
    await producer.send(topic_id=topic_id, data=b'{"price": 6942.25}')
    await producer.flush()
    await producer.close()

Example (ID-based, legacy)::

    producer = await Producer.connect("10.0.10.11:1992", ProducerConfig())
    await producer.send(topic_id=1, data=b'{"price": 6942.25}')
    await producer.send_async(topic_id=1, data=b'fire and forget')
    await producer.flush()
    await producer.close()
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from lnc_client.config import ClientConfig, ProducerConfig, validate_topic_name
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

        Returns:
            A connected ``Producer`` instance.
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
        prod._ack_reader_task = asyncio.create_task(prod._ack_reader_loop())
        return prod

    @classmethod
    async def connect_for_topic(
        cls,
        address: str,
        topic_name: str,
        config: ProducerConfig | None = None,
    ) -> tuple[Producer, int]:
        """Connect to a Lance server and resolve a topic name to its numeric ID.

        This is the preferred entry-point for name-based usage.  It validates
        the topic name, opens a management connection to resolve (or create) the
        topic, then returns a ready-to-use ``Producer`` together with the
        resolved numeric topic ID.

        The topic creation call is idempotent — if the topic already exists the
        server returns its existing metadata without error.

        Args:
            address: "host:port" string for the Lance server.
            topic_name: Human-readable topic name.  Must match ``[a-zA-Z0-9-]+``.
            config: Optional producer configuration.

        Returns:
            A ``(Producer, topic_id)`` tuple.

        Raises:
            ValueError: If ``topic_name`` contains invalid characters.
        """
        validate_topic_name(topic_name)

        # Parse address for the management client
        host, _, port_str = address.rpartition(":")
        port = int(port_str) if port_str else 1992
        if not host:
            host = address

        # Resolve name → ID via management channel
        from lnc_client.client import LanceClient

        async with LanceClient(ClientConfig(host=host, port=port)) as client:
            topic = await client.create_topic(topic_name)

        topic_id: int = topic["id"]
        log.info(
            "Resolved topic '%s' to id=%d for producer at %s",
            topic_name,
            topic_id,
            address,
        )

        producer = await cls.connect(address, config)
        return producer, topic_id

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
        topic_id: int,
        data: bytes,
        *,
        record_type: int = RecordType.RAW_DATA,
    ) -> int:
        """Send data and wait for server ACK. Returns batch_id."""
        batch_id = await self.send_async(topic_id, data, record_type=record_type)
        await self._wait_ack(batch_id)
        return batch_id

    async def send_async(
        self,
        topic_id: int,
        data: bytes,
        *,
        record_type: int = RecordType.RAW_DATA,
    ) -> int:
        """Send data without waiting for ACK (pipelined). Returns batch_id."""
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

    async def send_batch(
        self,
        topic_id: int,
        records: list[TlvRecord],
    ) -> int:
        """Send multiple TLV records as a single batch. Waits for ACK."""
        if self._closed:
            raise ConnectionError("Producer is closed")

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
            topic_id=topic_id,
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
