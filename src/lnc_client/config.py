"""Configuration classes for Lance client components."""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class SeekPosition(Enum):
    """Position specifier for seeking within a stream.

    Mirrors Rust ``lnc_client::SeekPosition``.
    """

    BEGINNING = "beginning"
    END = "end"

    @staticmethod
    def offset(value: int) -> tuple[str, int]:
        """Seek to a specific byte offset.

        Returns a ``("offset", value)`` tuple that config helpers accept.
        """
        return ("offset", value)


@dataclass(slots=True)
class ClientConfig:
    """Configuration for the management LanceClient."""

    host: str = "127.0.0.1"
    port: int = 1992
    connect_timeout_s: float = 10.0
    request_timeout_s: float = 30.0
    keepalive_interval_s: float = 10.0
    ssl_context: ssl.SSLContext | None = None

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def with_host(self, host: str) -> ClientConfig:
        self.host = host
        return self

    def with_port(self, port: int) -> ClientConfig:
        self.port = port
        return self

    def with_connect_timeout(self, timeout_s: float) -> ClientConfig:
        self.connect_timeout_s = timeout_s
        return self

    def with_ssl(self, ctx: ssl.SSLContext) -> ClientConfig:
        self.ssl_context = ctx
        return self


@dataclass(slots=True)
class ProducerConfig:
    """Configuration for the Lance Producer."""

    batch_size: int = 32 * 1024  # bytes
    linger_ms: int = 5
    compression: bool = False
    max_pending_acks: int = 64
    connect_timeout_s: float = 10.0
    request_timeout_s: float = 30.0
    keepalive_interval_s: float = 10.0
    ssl_context: ssl.SSLContext | None = None
    auto_reconnect: bool = True

    def with_batch_size(self, size: int) -> ProducerConfig:
        self.batch_size = size
        return self

    def with_linger_ms(self, ms: int) -> ProducerConfig:
        self.linger_ms = ms
        return self

    def with_compression(self, enabled: bool) -> ProducerConfig:
        self.compression = enabled
        return self

    def with_max_pending_acks(self, n: int) -> ProducerConfig:
        self.max_pending_acks = n
        return self

    def with_connect_timeout(self, timeout_s: float) -> ProducerConfig:
        self.connect_timeout_s = timeout_s
        return self

    def with_request_timeout(self, timeout_s: float) -> ProducerConfig:
        self.request_timeout_s = timeout_s
        return self

    def with_ssl(self, ctx: ssl.SSLContext) -> ProducerConfig:
        self.ssl_context = ctx
        return self

    def with_auto_reconnect(self, enabled: bool) -> ProducerConfig:
        self.auto_reconnect = enabled
        return self


@dataclass(slots=True)
class StandaloneConfig:
    """Configuration for a standalone consumer."""

    consumer_name: str = ""
    topic_id: int = 0
    max_fetch_bytes: int = 1_048_576
    start_position: SeekPosition | tuple[str, int] = SeekPosition.BEGINNING
    offset_dir: Path | None = None
    auto_commit_interval_s: float | None = 5.0
    connect_timeout_s: float = 10.0
    poll_timeout_s: float = 0.1
    keepalive_interval_s: float = 10.0
    poll_interval_ms: int = 50
    ssl_context: ssl.SSLContext | None = None
    auto_reconnect: bool = True

    def __init__(
        self,
        consumer_name: str = "",
        topic_id: int = 0,
        **kwargs,
    ) -> None:
        self.consumer_name = consumer_name
        self.topic_id = topic_id
        self.max_fetch_bytes = kwargs.get("max_fetch_bytes", 1_048_576)
        self.start_position = kwargs.get("start_position", SeekPosition.BEGINNING)
        self.offset_dir = kwargs.get("offset_dir")
        self.auto_commit_interval_s = kwargs.get("auto_commit_interval_s", 5.0)
        self.connect_timeout_s = kwargs.get("connect_timeout_s", 10.0)
        self.poll_timeout_s = kwargs.get("poll_timeout_s", 0.1)
        self.keepalive_interval_s = kwargs.get("keepalive_interval_s", 10.0)
        self.poll_interval_ms = kwargs.get("poll_interval_ms", 50)
        self.ssl_context = kwargs.get("ssl_context")
        self.auto_reconnect = kwargs.get("auto_reconnect", True)
        # Backward compat: accept start_offset as int
        if "start_offset" in kwargs:
            self.start_position = SeekPosition.offset(kwargs["start_offset"])

    def with_max_fetch_bytes(self, n: int) -> StandaloneConfig:
        self.max_fetch_bytes = n
        return self

    def with_start_position(self, pos: SeekPosition | tuple[str, int]) -> StandaloneConfig:
        self.start_position = pos
        return self

    def with_offset_dir(self, path: Path | str) -> StandaloneConfig:
        self.offset_dir = Path(path) if isinstance(path, str) else path
        return self

    def with_auto_commit_interval(self, interval_s: float | None) -> StandaloneConfig:
        self.auto_commit_interval_s = interval_s
        return self

    def with_manual_commit(self) -> StandaloneConfig:
        self.auto_commit_interval_s = None
        return self

    def with_poll_timeout(self, timeout_s: float) -> StandaloneConfig:
        self.poll_timeout_s = timeout_s
        return self

    def with_connect_timeout(self, timeout_s: float) -> StandaloneConfig:
        self.connect_timeout_s = timeout_s
        return self

    def with_ssl(self, ctx: ssl.SSLContext) -> StandaloneConfig:
        self.ssl_context = ctx
        return self

    def with_auto_reconnect(self, enabled: bool) -> StandaloneConfig:
        self.auto_reconnect = enabled
        return self

    @property
    def start_offset(self) -> int:
        """Resolve start_position to a numeric offset for the initial fetch."""
        if isinstance(self.start_position, tuple) and self.start_position[0] == "offset":
            return self.start_position[1]
        if self.start_position is SeekPosition.BEGINNING:
            return 0
        if self.start_position is SeekPosition.END:
            return 2**63 - 1
        return 0


@dataclass(slots=True)
class ReconnectConfig:
    """Reconnection parameters with exponential backoff."""

    base_delay_ms: int = 100
    max_delay_ms: int = 30_000
    max_attempts: int = 0  # 0 = unlimited
    jitter_factor: float = 0.1

    def delay_for_attempt(self, attempt: int) -> float:
        """Return delay in seconds for the given attempt number."""
        import random

        delay_ms = min(
            self.base_delay_ms * (2**attempt),
            self.max_delay_ms,
        )
        jitter = random.uniform(0, delay_ms * self.jitter_factor)
        return (delay_ms + jitter) / 1000.0
