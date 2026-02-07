"""Lance Consumer — offset-based message consumption.

Standalone consumer uses Fetch requests to pull data from specific offsets.
Supports seek, rewind, offset tracking, and commit.

Example::

    consumer = await StandaloneConsumer.connect(
        "10.0.10.11:1992",
        StandaloneConfig(consumer_name="my-app", topic_id=1),
    )

    while True:
        result = await consumer.poll()
        if result is None:
            await asyncio.sleep(0.05)
            continue
        for record in result.records:
            process(record)
        await consumer.commit()
"""

from __future__ import annotations

import asyncio
import logging
import struct
from dataclasses import dataclass, field

from lnc_client.config import StandaloneConfig
from lnc_client.connection import LwpConnection
from lnc_client.errors import ConnectionError, LanceError
from lnc_client.protocol import (
    ControlCommand,
    build_control_frame,
    build_fetch_payload,
    build_commit_offset_payload,
    parse_fetch_response,
)
from lnc_client.tlv import TlvRecord, decode_records

log = logging.getLogger("lnc_client.consumer")


@dataclass(slots=True)
class PollResult:
    """Result from a consumer poll operation."""

    data: bytes
    records: list[TlvRecord]
    start_offset: int
    end_offset: int
    high_water_mark: int
    record_count: int

    @property
    def is_empty(self) -> bool:
        return len(self.data) == 0

    @property
    def lag(self) -> int:
        """Consumer lag in bytes."""
        return max(0, self.high_water_mark - self.end_offset)


class StandaloneConsumer:
    """Standalone Lance consumer with client-managed offsets.

    Uses Fetch control frames to pull data from a topic at a specific offset.
    The consumer tracks its current offset and provides seek/rewind operations.
    """

    def __init__(self, conn: LwpConnection, config: StandaloneConfig) -> None:
        self._conn = conn
        self._config = config
        self._topic_id = config.topic_id
        self._current_offset: int = config.start_offset
        self._consumer_name = config.consumer_name
        self._consumer_id: int = hash(config.consumer_name) & 0xFFFFFFFFFFFFFFFF
        self._closed = False

    @classmethod
    async def connect(
        cls,
        address: str,
        config: StandaloneConfig,
    ) -> "StandaloneConsumer":
        """Connect to a Lance server and create a StandaloneConsumer.

        Args:
            address: "host:port" string.
            config: Consumer configuration.
        """
        host, _, port_str = address.rpartition(":")
        port = int(port_str) if port_str else 1992
        if not host:
            host = address

        conn = LwpConnection(
            host,
            port,
            keepalive_interval_s=config.keepalive_interval_s,
            connect_timeout_s=config.connect_timeout_s,
            ssl_context=config.ssl_context,
        )
        await conn.connect()

        consumer = cls(conn, config)
        log.info(
            "Consumer '%s' connected to %s for topic %d at offset %d",
            config.consumer_name,
            address,
            config.topic_id,
            config.start_offset,
        )
        return consumer

    async def close(self) -> None:
        """Close the consumer and its connection."""
        self._closed = True
        await self._conn.close()

    @property
    def current_offset(self) -> int:
        """Current consumer offset."""
        return self._current_offset

    @property
    def topic_id(self) -> int:
        return self._topic_id

    # ----- polling -----

    async def poll(self, timeout: float | None = None) -> PollResult | None:
        """Fetch the next batch of records from the topic.

        Returns None if no data is available, otherwise returns a PollResult.
        """
        if self._closed:
            raise ConnectionError("Consumer is closed")

        # Build and send Fetch request
        fetch_payload = build_fetch_payload(
            self._topic_id,
            self._current_offset,
            self._config.max_fetch_bytes,
        )
        frame = build_control_frame(ControlCommand.FETCH, fetch_payload)
        await self._conn.send_frame(frame)

        # Read response
        try:
            header, payload = await self._conn.recv_frame(
                timeout=timeout or 5.0
            )
        except Exception as e:
            if self._closed:
                return None
            raise

        # Handle error response
        if header.is_control and header.command == ControlCommand.ERROR_RESPONSE:
            err_msg = payload.decode("utf-8", errors="replace")
            if "Empty fetch response" in err_msg or "no data" in err_msg.lower():
                return None
            raise LanceError(f"Fetch error: {err_msg}")

        # Parse fetch response
        if header.is_control and header.command == ControlCommand.FETCH_RESPONSE:
            start_off, end_off, hwm, data = parse_fetch_response(payload)

            if not data:
                return None

            # Decode TLV records from data
            records = decode_records(data)

            # Advance offset
            self._current_offset = end_off

            return PollResult(
                data=data,
                records=records,
                start_offset=start_off,
                end_offset=end_off,
                high_water_mark=hwm,
                record_count=len(records),
            )

        # Unexpected frame type — might be raw data in payload
        if payload:
            self._current_offset += len(payload)
            records = decode_records(payload)
            return PollResult(
                data=payload,
                records=records,
                start_offset=self._current_offset - len(payload),
                end_offset=self._current_offset,
                high_water_mark=self._current_offset,
                record_count=len(records),
            )

        return None

    # ----- seek / rewind -----

    def seek(self, offset: int) -> None:
        """Seek to a specific byte offset."""
        self._current_offset = offset
        log.info("Consumer '%s' seeked to offset %d", self._consumer_name, offset)

    def rewind(self) -> None:
        """Rewind to the beginning (offset 0)."""
        self.seek(0)

    def seek_to_end(self) -> None:
        """Seek to end — next poll will return only new data.

        Note: The actual end offset is determined by the server's high_water_mark
        in the next FetchResponse. Setting to a very large value causes the server
        to return the latest data.
        """
        self._current_offset = 2**63 - 1  # Max offset, server returns latest

    # ----- commit -----

    async def commit(self) -> None:
        """Commit the current offset to the server.

        Note: In standalone mode, offset persistence is client-managed.
        This sends a CommitOffset frame to the server for acknowledgment,
        but the client is responsible for persisting offsets locally.
        """
        payload = build_commit_offset_payload(
            self._topic_id,
            self._consumer_id,
            self._current_offset,
        )
        frame = build_control_frame(ControlCommand.COMMIT_OFFSET, payload)
        await self._conn.send_frame(frame)

        # Wait for commit ack
        try:
            header, resp_payload = await self._conn.recv_frame(timeout=5.0)
            if header.is_control and header.command == ControlCommand.COMMIT_ACK:
                log.debug(
                    "Offset %d committed for topic %d",
                    self._current_offset,
                    self._topic_id,
                )
        except Exception as e:
            log.warning("Commit ack not received: %s", e)
