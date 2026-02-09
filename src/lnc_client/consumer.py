"""Lance Consumer — offset-based message consumption.

Standalone consumer uses Fetch requests to pull data from specific offsets.
Supports seek, rewind, offset tracking, commit, CATCHING_UP handling, and
optional file-based offset persistence.

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
from dataclasses import dataclass

from lnc_client.config import SeekPosition, StandaloneConfig
from lnc_client.connection import LwpConnection
from lnc_client.errors import ConnectionError, LanceError, ServerCatchingUpError
from lnc_client.offset import FileOffsetStore, OffsetStore
from lnc_client.protocol import (
    ControlCommand,
    build_commit_offset_payload,
    build_control_frame,
    build_fetch_payload,
    parse_fetch_response,
)
from lnc_client.tlv import TlvRecord, decode_records

log = logging.getLogger("lnc_client.consumer")

# CATCHING_UP backoff (matches Rust client: 5s)
_CATCHING_UP_BACKOFF_S = 5.0
# Max consecutive CATCHING_UP responses before raising
_MAX_CATCHING_UP_RETRIES = 3


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
    Supports optional file-based offset persistence via ``OffsetStore``.
    """

    def __init__(
        self,
        conn: LwpConnection,
        config: StandaloneConfig,
        offset_store: OffsetStore | None = None,
    ) -> None:
        self._conn = conn
        self._config = config
        self._address = ""
        self._topic_id = config.topic_id
        self._current_offset: int = config.start_offset
        self._consumer_name = config.consumer_name
        self._consumer_id: int = hash(config.consumer_name) & 0xFFFFFFFFFFFFFFFF
        self._closed = False
        self._catching_up_count = 0
        self._offset_store = offset_store

    @classmethod
    async def connect(
        cls,
        address: str,
        config: StandaloneConfig,
        offset_store: OffsetStore | None = None,
    ) -> StandaloneConsumer:
        """Connect to a Lance server and create a StandaloneConsumer.

        Args:
            address: "host:port" string.
            config: Consumer configuration.
            offset_store: Optional offset persistence backend.
                          If not provided and config.offset_dir is set, a
                          FileOffsetStore is created automatically.
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

        # Auto-create FileOffsetStore if offset_dir configured
        if offset_store is None and config.offset_dir is not None:
            offset_store = FileOffsetStore(config.offset_dir)

        consumer = cls(conn, config, offset_store)
        consumer._address = address

        # Restore persisted offset if available
        if offset_store is not None:
            saved = await offset_store.load(config.consumer_name, config.topic_id)
            if saved is not None:
                consumer._current_offset = saved
                log.info(
                    "Restored offset %d for consumer '%s' topic %d",
                    saved,
                    config.consumer_name,
                    config.topic_id,
                )

        log.info(
            "Consumer '%s' connected to %s for topic %d at offset %d",
            config.consumer_name,
            address,
            config.topic_id,
            consumer._current_offset,
        )
        return consumer

    async def close(self) -> None:
        """Close the consumer and its connection."""
        self._closed = True
        await self._conn.close()

    async def __aenter__(self) -> StandaloneConsumer:
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

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
        Handles CATCHING_UP responses with automatic 5s backoff (up to 3 retries).
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
                timeout=timeout or self._config.poll_timeout_s or 5.0,
            )
        except Exception:
            if self._closed:
                return None
            raise

        # Handle error response
        if header.is_control and header.command == ControlCommand.ERROR_RESPONSE:
            err_msg = payload.decode("utf-8", errors="replace")

            # CATCHING_UP: server hasn't replicated to our offset yet
            if "CATCHING_UP" in err_msg or "catching up" in err_msg.lower():
                self._catching_up_count += 1
                if self._catching_up_count >= _MAX_CATCHING_UP_RETRIES:
                    self._catching_up_count = 0
                    raise ServerCatchingUpError()
                log.info(
                    "Server catching up (%d/%d), backing off %.1fs",
                    self._catching_up_count,
                    _MAX_CATCHING_UP_RETRIES,
                    _CATCHING_UP_BACKOFF_S,
                )
                await asyncio.sleep(_CATCHING_UP_BACKOFF_S)
                return None

            if "Empty fetch response" in err_msg or "no data" in err_msg.lower():
                return None
            raise LanceError(f"Fetch error: {err_msg}")

        # Reset catching-up counter on successful response
        self._catching_up_count = 0

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

    def seek_to(self, position: SeekPosition | tuple[str, int]) -> None:
        """Seek using a SeekPosition value.

        Args:
            position: ``SeekPosition.BEGINNING``, ``SeekPosition.END``,
                      or ``SeekPosition.offset(n)``.
        """
        if isinstance(position, tuple) and position[0] == "offset":
            self.seek(position[1])
        elif position is SeekPosition.BEGINNING:
            self.seek(0)
        elif position is SeekPosition.END:
            self.seek_to_end()

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
        """Commit the current offset to the server and offset store.

        Sends a CommitOffset frame to the server. If an offset store is
        configured, the offset is also persisted locally.
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

        # Persist to offset store
        if self._offset_store is not None:
            await self._offset_store.save(
                self._consumer_name,
                self._topic_id,
                self._current_offset,
            )
