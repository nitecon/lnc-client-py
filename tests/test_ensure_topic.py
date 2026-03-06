"""Tests for ensure_topic, create_topic_once, and topic cache logic."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lnc_client.client import LanceClient
from lnc_client.config import ClientConfig, TopicInfo
from lnc_client.errors import (
    BackpressureError,
    InvalidTopicNameError,
    LanceError,
    TopicAlreadyExistsError,
)


def _make_client() -> LanceClient:
    """Create a LanceClient with a mocked connection."""
    client = LanceClient(ClientConfig())
    client._conn = MagicMock()
    client._conn.send_frame = AsyncMock()
    client._conn.recv_frame = AsyncMock()
    client._conn.connect = AsyncMock()
    client._conn.close = AsyncMock()
    return client


def _topic_resp(topic_id: int = 1, name: str = "test") -> dict:
    """Create a mock topic response dict."""
    return {"id": topic_id, "name": name, "created_at": 1700000000, "topic_epoch": 1}


class TestParseTopicInfo:
    """Test LanceClient._parse_topic_info static method."""

    def test_full_response(self):
        data = {
            "id": 3,
            "name": "events",
            "created_at": 1700000000,
            "topic_epoch": 2,
            "retention": {"max_age_secs": 86400, "max_bytes": 1000000},
        }
        info = LanceClient._parse_topic_info(data)
        assert info.id == 3
        assert info.name == "events"
        assert info.created_at == 1700000000
        assert info.topic_epoch == 2
        assert info.retention is not None
        assert info.retention.max_age_secs == 86400
        assert info.retention.max_bytes == 1000000

    def test_minimal_response(self):
        data = {"id": 1, "name": "t"}
        info = LanceClient._parse_topic_info(data)
        assert info.id == 1
        assert info.name == "t"
        assert info.topic_epoch == 1
        assert info.retention is None

    def test_fallback_name(self):
        data = {"id": 1}
        info = LanceClient._parse_topic_info(data, fallback_name="my-topic")
        assert info.name == "my-topic"

    def test_topic_id_alias(self):
        data = {"topic_id": 5, "topic_name": "foo"}
        info = LanceClient._parse_topic_info(data)
        assert info.id == 5
        assert info.name == "foo"

    def test_empty_dict(self):
        info = LanceClient._parse_topic_info({})
        assert info.id == 0
        assert info.name == ""

    def test_non_dict_input(self):
        info = LanceClient._parse_topic_info("not a dict")
        assert info.id == 0
        assert info.name == ""


class TestCreateTopicOnce:
    """Test create_topic_once — single attempt, no retry."""

    async def test_invalid_name_rejected(self):
        client = _make_client()
        with pytest.raises(InvalidTopicNameError):
            await client.create_topic_once("bad name!")

    async def test_success(self):
        client = _make_client()
        with patch.object(client, "_recv_topic_response", new_callable=AsyncMock) as mock_recv:
            mock_recv.return_value = _topic_resp(1, "my-topic")
            info = await client.create_topic_once("my-topic")
            assert info.id == 1
            assert info.name == "my-topic"
            assert "my-topic" in client._topic_cache


class TestEnsureTopic:
    """Test ensure_topic — idempotent creation with retry."""

    async def test_invalid_name_before_wire(self):
        client = _make_client()
        with pytest.raises(InvalidTopicNameError):
            await client.ensure_topic("invalid name")
        # No wire calls should have been made
        client._conn.send_frame.assert_not_called()

    async def test_success_on_first_create(self):
        client = _make_client()
        with patch.object(client, "create_topic_once", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = TopicInfo(id=1, name="events")
            info = await client.ensure_topic("events")
            assert info.id == 1
            assert info.name == "events"
            mock_create.assert_called_once_with("events")

    async def test_already_exists_fallback_to_list(self):
        client = _make_client()
        with (
            patch.object(client, "create_topic_once", new_callable=AsyncMock) as mock_create,
            patch.object(client, "list_topics", new_callable=AsyncMock) as mock_list,
        ):
            mock_create.side_effect = TopicAlreadyExistsError("exists")
            mock_list.return_value = [
                TopicInfo(id=5, name="events"),
                TopicInfo(id=6, name="other"),
            ]
            info = await client.ensure_topic("events")
            assert info.id == 5
            assert info.name == "events"

    async def test_retryable_error_retries(self):
        client = _make_client()
        with (
            patch.object(client, "create_topic_once", new_callable=AsyncMock) as mock_create,
            patch.object(client, "list_topics", new_callable=AsyncMock) as mock_list,
        ):
            # Fail twice with retryable (list also fails), then succeed
            mock_create.side_effect = [
                BackpressureError("slow"),
                BackpressureError("slow"),
                TopicInfo(id=1, name="t"),
            ]
            mock_list.return_value = []  # topic not found in list
            info = await client.ensure_topic("t", max_attempts=5, base_backoff_ms=1)
            assert info.id == 1
            assert mock_create.call_count == 3

    async def test_non_retryable_error_raises_after_list_fallback(self):
        client = _make_client()
        with (
            patch.object(client, "create_topic_once", new_callable=AsyncMock) as mock_create,
            patch.object(client, "list_topics", new_callable=AsyncMock) as mock_list,
        ):
            mock_create.side_effect = LanceError("permanent")
            mock_list.return_value = []  # not found in list either
            with pytest.raises(LanceError, match="permanent"):
                await client.ensure_topic("t", max_attempts=5, base_backoff_ms=1)
            mock_create.assert_called_once()

    async def test_non_retryable_create_but_found_in_list(self):
        client = _make_client()
        with (
            patch.object(client, "create_topic_once", new_callable=AsyncMock) as mock_create,
            patch.object(client, "list_topics", new_callable=AsyncMock) as mock_list,
        ):
            mock_create.side_effect = LanceError("permanent")
            mock_list.return_value = [TopicInfo(id=9, name="t")]
            info = await client.ensure_topic("t", max_attempts=5, base_backoff_ms=1)
            assert info.id == 9

    async def test_exhausts_attempts(self):
        client = _make_client()
        with (
            patch.object(client, "create_topic_once", new_callable=AsyncMock) as mock_create,
            patch.object(client, "list_topics", new_callable=AsyncMock) as mock_list,
        ):
            mock_create.side_effect = BackpressureError("slow")
            mock_list.return_value = []
            with pytest.raises(LanceError, match="after 3 attempts"):
                await client.ensure_topic("t", max_attempts=3, base_backoff_ms=1)
            assert mock_create.call_count == 3

    async def test_cache_hit_skips_wire(self):
        client = _make_client()
        client._topic_cache["cached"] = TopicInfo(id=42, name="cached")
        info = await client.ensure_topic("cached")
        assert info.id == 42
        client._conn.send_frame.assert_not_called()


class TestResolveTopicId:
    """Test resolve_topic_id — cache-first resolution."""

    async def test_from_cache(self):
        client = _make_client()
        client._topic_cache["cached"] = TopicInfo(id=10, name="cached")
        assert await client.resolve_topic_id("cached") == 10

    async def test_delegates_to_ensure(self):
        client = _make_client()
        with patch.object(client, "ensure_topic_default", new_callable=AsyncMock) as mock:
            mock.return_value = TopicInfo(id=7, name="new")
            assert await client.resolve_topic_id("new") == 7


class TestTopicCache:
    """Test that topic operations populate the cache."""

    def test_cache_topic(self):
        client = _make_client()
        info = TopicInfo(id=1, name="foo")
        client._cache_topic(info)
        assert client._topic_cache["foo"] is info

    def test_cache_empty_name_skipped(self):
        client = _make_client()
        info = TopicInfo(id=1, name="")
        client._cache_topic(info)
        assert "" not in client._topic_cache
