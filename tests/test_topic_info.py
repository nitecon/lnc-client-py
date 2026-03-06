"""Tests for TopicInfo, RetentionInfo, and validate_topic_name."""

import pytest

from lnc_client.config import RetentionInfo, TopicInfo
from lnc_client.errors import InvalidTopicNameError, validate_topic_name


class TestTopicInfo:
    """Test TopicInfo construction and field access."""

    def test_construction(self):
        info = TopicInfo(id=1, name="my-topic", created_at=1700000000, topic_epoch=2)
        assert info.id == 1
        assert info.name == "my-topic"
        assert info.created_at == 1700000000
        assert info.topic_epoch == 2
        assert info.retention is None

    def test_defaults(self):
        info = TopicInfo(id=5, name="test")
        assert info.created_at == 0
        assert info.topic_epoch == 1
        assert info.retention is None

    def test_with_retention(self):
        ret = RetentionInfo(max_age_secs=86400, max_bytes=1_000_000)
        info = TopicInfo(id=1, name="t", retention=ret)
        assert info.retention is not None
        assert info.retention.max_age_secs == 86400
        assert info.retention.max_bytes == 1_000_000


class TestRetentionInfo:
    """Test RetentionInfo defaults."""

    def test_defaults(self):
        ret = RetentionInfo()
        assert ret.max_age_secs == 0
        assert ret.max_bytes == 0

    def test_custom(self):
        ret = RetentionInfo(max_age_secs=3600, max_bytes=500_000)
        assert ret.max_age_secs == 3600
        assert ret.max_bytes == 500_000


class TestValidateTopicName:
    """Test validate_topic_name with valid and invalid names."""

    @pytest.mark.parametrize(
        "name",
        ["my-topic", "topic123", "a", "A-B-C", "test-topic-1", "X"],
    )
    def test_valid_names(self, name):
        validate_topic_name(name)  # Should not raise

    @pytest.mark.parametrize(
        "name",
        [
            "",  # empty
            "hello world",  # spaces
            "my_topic",  # underscores
            "my.topic",  # dots
            "topic/sub",  # slashes
            "hello\ttab",  # tabs
        ],
    )
    def test_invalid_names(self, name):
        with pytest.raises(InvalidTopicNameError):
            validate_topic_name(name)


class TestInvalidTopicNameError:
    """Test InvalidTopicNameError attributes and message."""

    def test_message(self):
        err = InvalidTopicNameError("bad name!")
        assert "bad name!" in str(err)
        assert err.name == "bad name!"

    def test_inherits_lance_error(self):
        from lnc_client.errors import LanceError

        assert issubclass(InvalidTopicNameError, LanceError)

    def test_not_retryable(self):
        err = InvalidTopicNameError("x")
        assert not err.is_retryable()
