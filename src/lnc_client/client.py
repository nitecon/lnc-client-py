"""LanceClient — management/control client for topic operations.

Provides async context manager for connecting to a Lance server and
performing topic management operations (create, list, delete, get,
set retention).

Example::

    async with LanceClient(ClientConfig(host="10.0.10.11")) as client:
        topic = await client.ensure_topic("my-events")
        await client.set_retention(topic.id, max_age_secs=86400)
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from lnc_client.config import ClientConfig, RetentionInfo, TopicInfo
from lnc_client.connection import LwpConnection
from lnc_client.errors import (
    LanceError,
    TopicNotFoundError,
    error_from_response,
    validate_topic_name,
)
from lnc_client.protocol import (
    ControlCommand,
    build_commit_offset_payload,
    build_control_frame,
    build_create_topic_with_retention_payload,
    build_keepalive_frame,
    build_set_retention_payload,
    build_subscribe_payload,
    build_unsubscribe_payload,
)

log = logging.getLogger("lnc_client.client")


class LanceClient:
    """Async management client for Lance topic operations."""

    def __init__(self, config: ClientConfig | None = None) -> None:
        self._config = config or ClientConfig()
        self._conn = LwpConnection(
            self._config.host,
            self._config.port,
            keepalive_interval_s=self._config.keepalive_interval_s,
            connect_timeout_s=self._config.connect_timeout_s,
            ssl_context=self._config.ssl_context,
        )
        self._topic_cache: dict[str, TopicInfo] = {}

    async def connect(self) -> LanceClient:
        """Connect to the Lance server."""
        await self._conn.connect()
        return self

    async def close(self) -> None:
        """Close the connection."""
        await self._conn.close()

    async def __aenter__(self) -> LanceClient:
        await self.connect()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    # ----- topic operations -----

    async def create_topic(self, name: str) -> TopicInfo:
        """Create a topic idempotently. Delegates to ``ensure_topic()``.

        Returns ``TopicInfo`` for the created or existing topic.
        """
        return await self.ensure_topic(name)

    async def create_topic_once(self, name: str) -> TopicInfo:
        """Single-attempt topic creation. Does NOT handle ``TopicAlreadyExistsError``.

        Validates the name, sends CREATE_TOPIC, and parses the response.
        """
        validate_topic_name(name)
        payload = name.encode("utf-8")
        frame = build_control_frame(ControlCommand.CREATE_TOPIC, payload)
        await self._conn.send_frame(frame)
        resp = await self._recv_topic_response()
        info = self._parse_topic_info(resp, fallback_name=name)
        self._cache_topic(info)
        return info

    async def ensure_topic(
        self,
        name: str,
        max_attempts: int = 20,
        base_backoff_ms: int = 500,
    ) -> TopicInfo:
        """Idempotent topic creation with retry.

        Mirrors Rust ``ensure_topic``. Tries to create the topic; if it
        already exists, resolves it from ``list_topics``. Retries on
        transient errors with exponential backoff.
        """
        validate_topic_name(name)

        # Check cache first
        cached = self._topic_cache.get(name)
        if cached is not None:
            return cached

        last_error: LanceError | None = None

        for attempt in range(max_attempts):
            retryable_this_attempt = False

            # 1. Try to create the topic
            try:
                return await self.create_topic_once(name)
            except LanceError as create_err:
                if create_err.is_retryable():
                    retryable_this_attempt = True
                last_error = create_err
                log.warning(
                    "create_topic failed for '%s' (attempt %d/%d): %s",
                    name,
                    attempt + 1,
                    max_attempts,
                    create_err,
                )

            # 2. Fallback: try list_topics to find the topic by name
            try:
                topics = await self.list_topics()
                for t in topics:
                    if t.name == name:
                        self._cache_topic(t)
                        return t
            except LanceError as list_err:
                if list_err.is_retryable():
                    retryable_this_attempt = True
                last_error = list_err
                log.warning(
                    "list_topics failed for '%s' (attempt %d/%d): %s",
                    name,
                    attempt + 1,
                    max_attempts,
                    list_err,
                )

            # 3. Non-retryable create errors with no list fallback match → raise
            if not retryable_this_attempt:
                raise last_error  # type: ignore[misc]

            # 4. Exponential backoff before next attempt
            delay_ms = base_backoff_ms * (attempt + 1)
            await asyncio.sleep(delay_ms / 1000.0)

        raise LanceError(f"Failed to ensure topic '{name}' after {max_attempts} attempts")

    async def ensure_topic_default(self, name: str) -> TopicInfo:
        """Shorthand for ``ensure_topic(name, 20, 500)``."""
        return await self.ensure_topic(name, 20, 500)

    async def resolve_topic_id(self, name: str) -> int:
        """Resolve a topic name to its numeric ID.

        Checks the internal cache first, then calls ``ensure_topic_default()``.
        """
        cached = self._topic_cache.get(name)
        if cached is not None:
            return cached.id
        info = await self.ensure_topic_default(name)
        return info.id

    async def delete_topic(self, topic_id: int) -> None:
        """Delete a topic by ID."""
        import struct

        payload = struct.pack("<I", topic_id)
        frame = build_control_frame(ControlCommand.DELETE_TOPIC, payload)
        await self._conn.send_frame(frame)
        await self._recv_topic_response()
        # Invalidate cache for this topic_id
        self._topic_cache = {k: v for k, v in self._topic_cache.items() if v.id != topic_id}

    async def list_topics(self) -> list[TopicInfo]:
        """List all topics. Returns list of ``TopicInfo``."""
        frame = build_control_frame(ControlCommand.LIST_TOPICS)
        await self._conn.send_frame(frame)
        resp = await self._recv_topic_response()
        # Response may be a list or a dict with a list inside
        raw_list: list[dict[str, Any]]
        if isinstance(resp, list):
            raw_list = resp
        elif isinstance(resp, dict) and "topics" in resp:
            raw_list = resp["topics"]
        elif resp:
            raw_list = [resp]
        else:
            raw_list = []

        result = [self._parse_topic_info(item) for item in raw_list]
        for info in result:
            self._cache_topic(info)
        return result

    async def get_topic(self, topic_id: int) -> TopicInfo:
        """Get topic metadata by ID."""
        import struct

        payload = struct.pack("<I", topic_id)
        frame = build_control_frame(ControlCommand.GET_TOPIC, payload)
        await self._conn.send_frame(frame)
        resp = await self._recv_topic_response()
        info = self._parse_topic_info(resp)
        self._cache_topic(info)
        return info

    async def get_topic_by_name(self, name: str) -> TopicInfo:
        """Look up a topic strictly by name without creating it.

        Unlike ``ensure_topic``, this method never creates a new topic.  It
        lists all topics and matches by name.  Use this when the topic *must*
        already exist and a missing topic should be surfaced as an error.

        Args:
            name: Topic name to look up.

        Returns:
            ``TopicInfo`` for the matched topic.

        Raises:
            ValueError: If ``name`` contains invalid characters.
            TopicNotFoundError: If no topic with that name exists.
        """
        validate_topic_name(name)
        all_topics = await self.list_topics()
        matched = [t for t in all_topics if t.name == name]
        if not matched:
            available = [t.name for t in all_topics]
            raise TopicNotFoundError(
                f"Lance topic '{name}' not found. Available topics: {available}."
            )
        return matched[0]

    async def set_retention(
        self,
        topic_id: int,
        max_age_secs: int = 0,
        max_bytes: int = 0,
    ) -> TopicInfo:
        """Set retention policy for a topic."""
        payload = build_set_retention_payload(topic_id, max_age_secs, max_bytes)
        frame = build_control_frame(ControlCommand.SET_RETENTION, payload)
        await self._conn.send_frame(frame)
        resp = await self._recv_topic_response()
        info = self._parse_topic_info(resp)
        self._cache_topic(info)
        return info

    async def create_topic_with_retention(
        self,
        name: str,
        max_age_secs: int = 0,
        max_bytes: int = 0,
    ) -> TopicInfo:
        """Create a topic with retention policy in a single operation."""
        validate_topic_name(name)
        payload = build_create_topic_with_retention_payload(name, max_age_secs, max_bytes)
        frame = build_control_frame(ControlCommand.CREATE_TOPIC_WITH_RETENTION, payload)
        await self._conn.send_frame(frame)
        resp = await self._recv_topic_response()
        info = self._parse_topic_info(resp, fallback_name=name)
        self._cache_topic(info)
        return info

    # ----- diagnostics -----

    async def ping(self) -> float:
        """Ping the server and measure round-trip latency.

        Returns latency in seconds.
        """
        import time

        start = time.monotonic()
        await self._conn.send_frame(build_keepalive_frame())
        await self._conn.recv_header(timeout=self._config.request_timeout_s)
        return time.monotonic() - start

    # ----- subscribe / unsubscribe -----

    async def subscribe(
        self,
        topic_id: int,
        start_offset: int,
        max_batch_bytes: int,
        consumer_id: int,
    ) -> dict[str, Any]:
        """Subscribe to a topic for streaming consumption.

        Args:
            topic_id: Topic to subscribe to.
            start_offset: Byte offset to start from.
            max_batch_bytes: Maximum bytes per server push.
            consumer_id: Numeric consumer identifier.
        """
        payload = build_subscribe_payload(topic_id, start_offset, max_batch_bytes, consumer_id)
        frame = build_control_frame(ControlCommand.SUBSCRIBE, payload)
        await self._conn.send_frame(frame)
        return await self._recv_topic_response()

    async def unsubscribe(self, topic_id: int, consumer_id: int) -> None:
        """Unsubscribe from a topic."""
        payload = build_unsubscribe_payload(topic_id, consumer_id)
        frame = build_control_frame(ControlCommand.UNSUBSCRIBE, payload)
        await self._conn.send_frame(frame)
        await self._recv_topic_response()

    async def commit_offset(
        self,
        topic_id: int,
        consumer_id: int,
        offset: int,
    ) -> dict[str, Any]:
        """Commit consumer offset for checkpointing."""
        payload = build_commit_offset_payload(topic_id, consumer_id, offset)
        frame = build_control_frame(ControlCommand.COMMIT_OFFSET, payload)
        await self._conn.send_frame(frame)
        return await self._recv_topic_response()

    # ----- internal -----

    @staticmethod
    def _parse_topic_info(
        data: dict[str, Any] | Any,
        fallback_name: str = "",
    ) -> TopicInfo:
        """Convert a JSON response dict into ``TopicInfo``.

        Handles missing fields gracefully with sensible defaults.
        Mirrors Rust ``parse_topic_response``.
        """
        if not isinstance(data, dict):
            data = {}

        retention = None
        ret_data = data.get("retention")
        if isinstance(ret_data, dict):
            retention = RetentionInfo(
                max_age_secs=ret_data.get("max_age_secs", 0),
                max_bytes=ret_data.get("max_bytes", 0),
            )

        return TopicInfo(
            id=data.get("id", data.get("topic_id", 0)),
            name=data.get("name", data.get("topic_name", fallback_name)),
            created_at=data.get("created_at", 0),
            topic_epoch=data.get("topic_epoch", 1),
            retention=retention,
        )

    def _cache_topic(self, info: TopicInfo) -> None:
        """Add a TopicInfo to the internal name cache."""
        if info.name:
            self._topic_cache[info.name] = info

    async def _recv_topic_response(self) -> Any:
        """Wait for a TopicResponse or ErrorResponse control frame."""
        header, payload = await self._conn.recv_frame(timeout=self._config.request_timeout_s)

        if not header.is_control:
            raise LanceError(f"Expected control frame, got flags={header.flags:#x}")

        cmd = header.command

        if cmd == ControlCommand.ERROR_RESPONSE:
            self._raise_error(payload)

        if cmd == ControlCommand.TOPIC_RESPONSE:
            if not payload:
                return {}
            return json.loads(payload)

        # Some commands return ack-style responses
        if header.is_ack or payload:
            try:
                return json.loads(payload)
            except (json.JSONDecodeError, ValueError):
                return {}

        return {}

    @staticmethod
    def _raise_error(payload: bytes) -> None:
        """Parse an error response payload and raise the appropriate exception."""
        try:
            data = json.loads(payload)
            code = data.get("code", 0x01)
            message = data.get("message", "Unknown error")
            details = data.get("details")
        except (json.JSONDecodeError, ValueError):
            message = payload.decode("utf-8", errors="replace")
            code = 0x01
            details = None

        raise error_from_response(code, message, details)
