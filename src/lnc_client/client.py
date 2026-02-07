"""LanceClient — management/control client for topic operations.

Provides async context manager for connecting to a Lance server and
performing topic management operations (create, list, delete, get,
set retention).

Example::

    async with LanceClient(ClientConfig(host="10.0.10.11")) as client:
        topics = await client.list_topics()
        topic = await client.create_topic("my-events")
        await client.set_retention(topic["id"], max_age_secs=86400)
"""

from __future__ import annotations

import json
import logging
from typing import Any

from lnc_client.config import ClientConfig
from lnc_client.connection import LwpConnection
from lnc_client.errors import LanceError, error_from_response
from lnc_client.protocol import (
    ControlCommand,
    build_control_frame,
    build_create_topic_with_retention_payload,
    build_set_retention_payload,
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

    async def create_topic(self, name: str) -> dict[str, Any]:
        """Create a new topic. Returns topic metadata dict."""
        payload = name.encode("utf-8")
        frame = build_control_frame(ControlCommand.CREATE_TOPIC, payload)
        await self._conn.send_frame(frame)
        return await self._recv_topic_response()

    async def delete_topic(self, topic_id: int) -> None:
        """Delete a topic by ID."""
        import struct

        payload = struct.pack("<I", topic_id)
        frame = build_control_frame(ControlCommand.DELETE_TOPIC, payload)
        await self._conn.send_frame(frame)
        await self._recv_topic_response()

    async def list_topics(self) -> list[dict[str, Any]]:
        """List all topics. Returns list of topic metadata dicts."""
        frame = build_control_frame(ControlCommand.LIST_TOPICS)
        await self._conn.send_frame(frame)
        resp = await self._recv_topic_response()
        # Response may be a list or a dict with a list inside
        if isinstance(resp, list):
            return resp
        if isinstance(resp, dict) and "topics" in resp:
            return resp["topics"]
        return [resp] if resp else []

    async def get_topic(self, topic_id: int) -> dict[str, Any]:
        """Get topic metadata by ID."""
        import struct

        payload = struct.pack("<I", topic_id)
        frame = build_control_frame(ControlCommand.GET_TOPIC, payload)
        await self._conn.send_frame(frame)
        return await self._recv_topic_response()

    async def set_retention(
        self,
        topic_id: int,
        max_age_secs: int = 0,
        max_bytes: int = 0,
    ) -> dict[str, Any]:
        """Set retention policy for a topic."""
        payload = build_set_retention_payload(topic_id, max_age_secs, max_bytes)
        frame = build_control_frame(ControlCommand.SET_RETENTION, payload)
        await self._conn.send_frame(frame)
        return await self._recv_topic_response()

    async def create_topic_with_retention(
        self,
        name: str,
        max_age_secs: int = 0,
        max_bytes: int = 0,
    ) -> dict[str, Any]:
        """Create a topic with retention policy in a single operation."""
        payload = build_create_topic_with_retention_payload(name, max_age_secs, max_bytes)
        frame = build_control_frame(ControlCommand.CREATE_TOPIC_WITH_RETENTION, payload)
        await self._conn.send_frame(frame)
        return await self._recv_topic_response()

    # ----- internal -----

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
