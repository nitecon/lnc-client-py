"""Configuration classes for Lance client components."""

from __future__ import annotations

import ssl
from dataclasses import dataclass


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


@dataclass(slots=True)
class ProducerConfig:
    """Configuration for the Lance Producer."""

    batch_size: int = 32 * 1024  # bytes
    linger_ms: int = 5
    compression: bool = False
    max_pending_acks: int = 64
    connect_timeout_s: float = 10.0
    keepalive_interval_s: float = 10.0
    ssl_context: ssl.SSLContext | None = None

    def with_batch_size(self, size: int) -> ProducerConfig:
        self.batch_size = size
        return self

    def with_linger_ms(self, ms: int) -> ProducerConfig:
        self.linger_ms = ms
        return self


@dataclass(slots=True)
class StandaloneConfig:
    """Configuration for a standalone consumer."""

    consumer_name: str = ""
    topic_id: int = 0
    max_fetch_bytes: int = 64 * 1024
    start_offset: int = 0
    connect_timeout_s: float = 10.0
    keepalive_interval_s: float = 10.0
    poll_interval_ms: int = 50
    ssl_context: ssl.SSLContext | None = None

    def __init__(
        self,
        consumer_name: str = "",
        topic_id: int = 0,
        **kwargs,
    ) -> None:
        self.consumer_name = consumer_name
        self.topic_id = topic_id
        self.max_fetch_bytes = kwargs.get("max_fetch_bytes", 64 * 1024)
        self.start_offset = kwargs.get("start_offset", 0)
        self.connect_timeout_s = kwargs.get("connect_timeout_s", 10.0)
        self.keepalive_interval_s = kwargs.get("keepalive_interval_s", 10.0)
        self.poll_interval_ms = kwargs.get("poll_interval_ms", 50)
        self.ssl_context = kwargs.get("ssl_context")


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
