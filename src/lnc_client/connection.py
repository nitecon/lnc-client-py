"""Async TCP connection with keepalive, backpressure, and reconnection.

Provides a low-level connection wrapper that handles:
- Async TCP streams (asyncio)
- Keepalive frames every 10s
- Backpressure detection
- Frame-level read/write with CRC validation
- Exponential backoff reconnection
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import ssl

import zlib

from lnc_client.config import ReconnectConfig
from lnc_client.errors import (
    ConnectionError,
    InvalidFrameError,
    TimeoutError,
)
from lnc_client.protocol import (
    HEADER_SIZE,
    LwpHeader,
    build_keepalive_frame,
)

log = logging.getLogger("lnc_client.connection")


class LwpConnection:
    """Async TCP connection implementing the LWP transport layer."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        keepalive_interval_s: float = 10.0,
        connect_timeout_s: float = 10.0,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._keepalive_interval = keepalive_interval_s
        self._connect_timeout = connect_timeout_s
        self._ssl = ssl_context

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._keepalive_task: asyncio.Task | None = None
        self._connected = False
        self._backpressure = False
        self._write_lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def under_backpressure(self) -> bool:
        return self._backpressure

    # ----- lifecycle -----

    async def connect(self) -> None:
        """Open the TCP connection and start keepalive."""
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port, ssl=self._ssl),
                timeout=self._connect_timeout,
            )
        except asyncio.TimeoutError as e:
            raise TimeoutError(
                f"Connection to {self._host}:{self._port} timed out after {self._connect_timeout}s"
            ) from e
        except OSError as e:
            raise ConnectionError(f"Failed to connect to {self._host}:{self._port}: {e}") from e

        self._connected = True
        self._backpressure = False
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        log.info("Connected to %s:%d", self._host, self._port)

    async def close(self) -> None:
        """Close the connection and stop keepalive."""
        self._connected = False
        if self._keepalive_task:
            self._keepalive_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._keepalive_task
            self._keepalive_task = None

        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            self._reader = None

        log.info("Disconnected from %s:%d", self._host, self._port)

    async def reconnect(self, config: ReconnectConfig | None = None) -> None:
        """Reconnect with exponential backoff."""
        cfg = config or ReconnectConfig()
        await self.close()

        attempt = 0
        while True:
            attempt += 1
            if cfg.max_attempts > 0 and attempt > cfg.max_attempts:
                raise ConnectionError(f"Failed to reconnect after {cfg.max_attempts} attempts")

            delay = cfg.delay_for_attempt(attempt)
            log.info(
                "Reconnect attempt %d in %.1fs to %s:%d",
                attempt,
                delay,
                self._host,
                self._port,
            )
            await asyncio.sleep(delay)

            try:
                await self.connect()
                log.info("Reconnected on attempt %d", attempt)
                return
            except (ConnectionError, TimeoutError) as e:
                log.warning("Reconnect attempt %d failed: %s", attempt, e)

    # ----- I/O -----

    async def send_frame(self, frame: bytes) -> None:
        """Send a complete frame (header + optional payload)."""
        if not self._writer or not self._connected:
            raise ConnectionError("Not connected")

        async with self._write_lock:
            try:
                self._writer.write(frame)
                await self._writer.drain()
            except OSError as e:
                self._connected = False
                raise ConnectionError(f"Send failed: {e}") from e

    async def recv_header(self, timeout: float | None = None) -> LwpHeader:
        """Read and parse a 44-byte header from the stream."""
        buf = await self._recv_exact(HEADER_SIZE, timeout)
        return LwpHeader.decode(buf)

    async def recv_payload(self, header: LwpHeader, timeout: float | None = None) -> bytes:
        """Read the payload for a given header and validate its CRC."""
        if header.payload_length == 0:
            return b""

        data = await self._recv_exact(header.payload_length, timeout)

        # Validate payload CRC
        if header.payload_crc != 0:
            actual_crc = zlib.crc32(data) & 0xFFFFFFFF
            if actual_crc != header.payload_crc:
                raise InvalidFrameError(
                    f"Payload CRC mismatch: got {actual_crc:#010x}, "
                    f"expected {header.payload_crc:#010x}"
                )

        return data

    async def recv_frame(self, timeout: float | None = None) -> tuple[LwpHeader, bytes]:
        """Read a complete frame (header + payload).

        Automatically handles Keepalive and Backpressure frames inline.
        Returns the next non-control frame.
        """
        while True:
            header = await self.recv_header(timeout)

            # Handle keepalive transparently
            if header.is_keepalive:
                await self.send_frame(build_keepalive_frame())
                continue

            # Track backpressure state
            if header.is_backpressure:
                self._backpressure = True
                log.warning("Server signaled backpressure")
                continue

            # Clear backpressure on successful ack
            if header.is_ack:
                self._backpressure = False

            payload = await self.recv_payload(header, timeout)
            return header, payload

    # ----- internal -----

    async def _recv_exact(self, n: int, timeout: float | None = None) -> bytes:
        """Read exactly n bytes from the stream."""
        if not self._reader or not self._connected:
            raise ConnectionError("Not connected")

        try:
            if timeout:
                data = await asyncio.wait_for(self._reader.readexactly(n), timeout=timeout)
            else:
                data = await self._reader.readexactly(n)
        except asyncio.TimeoutError as e:
            raise TimeoutError(f"Read timed out after {timeout}s") from e
        except asyncio.IncompleteReadError as e:
            self._connected = False
            raise ConnectionError(f"Connection closed (read {len(e.partial)}/{n} bytes)") from e
        except OSError as e:
            self._connected = False
            raise ConnectionError(f"Read failed: {e}") from e

        return data

    async def _keepalive_loop(self) -> None:
        """Send keepalive frames periodically."""
        frame = build_keepalive_frame()
        try:
            while self._connected:
                await asyncio.sleep(self._keepalive_interval)
                if self._connected:
                    try:
                        await self.send_frame(frame)
                    except ConnectionError:
                        break
        except asyncio.CancelledError:
            pass


async def connect(
    host: str,
    port: int = 1992,
    **kwargs,
) -> LwpConnection:
    """Create and connect an LwpConnection."""
    conn = LwpConnection(host, port, **kwargs)
    await conn.connect()
    return conn
