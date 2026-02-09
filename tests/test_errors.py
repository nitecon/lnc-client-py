"""Tests for error hierarchy, is_retryable(), and error_from_response."""

import pytest

from lnc_client.errors import (
    AccessDeniedError,
    BackpressureError,
    ConnectionError,
    InvalidFrameError,
    LanceError,
    NotLeaderError,
    ProtocolError,
    ServerCatchingUpError,
    TimeoutError,
    TopicAlreadyExistsError,
    TopicNotFoundError,
    error_from_response,
)


class TestIsRetryable:
    """Test is_retryable() on all error types."""

    def test_base_lance_error_not_retryable(self):
        assert not LanceError("boom").is_retryable()

    def test_connection_error_retryable(self):
        assert ConnectionError("refused").is_retryable()

    def test_timeout_error_retryable(self):
        assert TimeoutError("deadline exceeded").is_retryable()

    def test_backpressure_error_retryable(self):
        assert BackpressureError("slow down").is_retryable()

    def test_not_leader_error_retryable(self):
        assert NotLeaderError("10.0.0.2:1992").is_retryable()

    def test_server_catching_up_retryable(self):
        assert ServerCatchingUpError(42000).is_retryable()

    def test_protocol_error_not_retryable(self):
        assert not ProtocolError("version mismatch").is_retryable()

    def test_invalid_frame_not_retryable(self):
        assert not InvalidFrameError("bad crc").is_retryable()

    def test_topic_not_found_not_retryable(self):
        assert not TopicNotFoundError(99).is_retryable()

    def test_access_denied_not_retryable(self):
        assert not AccessDeniedError("nope").is_retryable()


class TestServerCatchingUpError:
    """Test ServerCatchingUpError attributes."""

    def test_default_offset(self):
        err = ServerCatchingUpError()
        assert err.server_offset == 0

    def test_custom_offset(self):
        err = ServerCatchingUpError(12345)
        assert err.server_offset == 12345

    def test_message(self):
        err = ServerCatchingUpError(500)
        assert "500" in str(err)


class TestNotLeaderError:
    """Test NotLeaderError attributes."""

    def test_no_leader(self):
        err = NotLeaderError()
        assert err.leader_addr is None

    def test_with_leader(self):
        err = NotLeaderError("10.0.0.2:1992")
        assert err.leader_addr == "10.0.0.2:1992"
        assert "10.0.0.2:1992" in str(err)


class TestErrorFromResponse:
    """Test error_from_response factory."""

    def test_unknown_code(self):
        err = error_from_response(0x99, "mystery error")
        assert isinstance(err, LanceError)

    def test_not_leader_with_details(self):
        err = error_from_response(0x20, "not leader", {"leader_addr": "10.0.0.5:1992"})
        assert isinstance(err, NotLeaderError)
        assert err.leader_addr == "10.0.0.5:1992"

    def test_topic_not_found(self):
        err = error_from_response(0x10, "topic 42")
        assert isinstance(err, TopicNotFoundError)

    def test_server_catching_up(self):
        err = error_from_response(0x14, "catching up", {"server_offset": 9000})
        assert isinstance(err, ServerCatchingUpError)
        assert err.server_offset == 9000

    def test_server_catching_up_no_details(self):
        err = error_from_response(0x14, "catching up")
        assert isinstance(err, ServerCatchingUpError)
        assert err.server_offset == 0

    def test_backpressure(self):
        err = error_from_response(0x31, "slow down")
        assert isinstance(err, BackpressureError)

    def test_access_denied(self):
        err = error_from_response(0x40, "auth required")
        assert isinstance(err, AccessDeniedError)

    def test_topic_already_exists(self):
        err = error_from_response(0x11, "exists")
        assert isinstance(err, TopicAlreadyExistsError)


class TestExceptionHierarchy:
    """Test that all errors inherit from LanceError."""

    @pytest.mark.parametrize(
        "exc_class",
        [
            ConnectionError,
            ProtocolError,
            InvalidFrameError,
            TimeoutError,
            BackpressureError,
            ServerCatchingUpError,
            TopicNotFoundError,
            TopicAlreadyExistsError,
            NotLeaderError,
            AccessDeniedError,
        ],
    )
    def test_inherits_lance_error(self, exc_class):
        assert issubclass(exc_class, LanceError)

    def test_invalid_frame_is_protocol_error(self):
        assert issubclass(InvalidFrameError, ProtocolError)
