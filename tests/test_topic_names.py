"""Tests for topic name validation, StandaloneConfig topic_name field, and
name-based API on LanceClient, Producer, and StandaloneConsumer.

These are pure unit tests — no network access required.
"""

from __future__ import annotations

import pytest

from lnc_client.config import StandaloneConfig, validate_topic_name
from lnc_client.errors import InvalidTopicNameError


# ---------------------------------------------------------------------------
# validate_topic_name()
# ---------------------------------------------------------------------------


class TestValidateTopicName:
    """Unit tests for the validate_topic_name() helper."""

    @pytest.mark.parametrize(
        "name",
        [
            "a",
            "rithmic-dev",
            "RITHMIC-DEV",
            "my-events-123",
            "ABC",
            "topic1",
            "a-b-c",
            "ALL-CAPS",
            "MixedCase123",
        ],
    )
    def test_valid_names_pass(self, name: str) -> None:
        """Valid names are returned unchanged."""
        assert validate_topic_name(name) == name

    @pytest.mark.parametrize(
        "name",
        [
            "",
            "has space",
            "under_score",
            "dot.name",
            "slash/name",
            "colon:name",
            "at@sign",
            "bang!",
            "hash#tag",
            "dollar$",
            "percent%",
            "caret^",
            "ampersand&",
            "star*",
            "plus+",
            "equals=",
            "bracket[",
            "unicode-\u00e9",
        ],
    )
    def test_invalid_names_raise_value_error(self, name: str) -> None:
        """Names with disallowed characters raise ValueError."""
        with pytest.raises(ValueError, match="Invalid topic name|must not be empty"):
            validate_topic_name(name)

    def test_empty_string_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            validate_topic_name("")

    def test_error_message_contains_name(self) -> None:
        """The error message includes the offending name."""
        with pytest.raises(ValueError, match="bad name!"):
            validate_topic_name("bad name!")

    def test_error_message_describes_pattern(self) -> None:
        """The error message explains the allowed pattern."""
        with pytest.raises(ValueError, match=r"\[a-zA-Z0-9-\]"):
            validate_topic_name("bad_name")

    def test_return_value_is_identity(self) -> None:
        """validate_topic_name is a pass-through for valid names."""
        name = "valid-topic-99"
        result = validate_topic_name(name)
        assert result is name


# ---------------------------------------------------------------------------
# InvalidTopicNameError
# ---------------------------------------------------------------------------


class TestInvalidTopicNameError:
    """Test the InvalidTopicNameError exception class."""

    def test_inherits_from_lance_error(self) -> None:
        from lnc_client.errors import LanceError

        assert issubclass(InvalidTopicNameError, LanceError)

    def test_stores_name_attribute(self) -> None:
        err = InvalidTopicNameError("bad name!")
        assert err.name == "bad name!"

    def test_is_not_retryable(self) -> None:
        assert not InvalidTopicNameError("x y").is_retryable()

    def test_message_contains_name(self) -> None:
        err = InvalidTopicNameError("has space")
        assert "has space" in str(err)

    def test_message_describes_pattern(self) -> None:
        err = InvalidTopicNameError("x")
        assert "[a-zA-Z0-9-]" in str(err)


# ---------------------------------------------------------------------------
# StandaloneConfig — topic_name field
# ---------------------------------------------------------------------------


class TestStandaloneConfigTopicName:
    """StandaloneConfig accepts topic_name and validates it eagerly."""

    def test_topic_name_accepted(self) -> None:
        cfg = StandaloneConfig(consumer_name="test", topic_name="rithmic-dev")
        assert cfg.topic_name == "rithmic-dev"
        assert cfg.topic_id == 0  # not yet resolved

    def test_topic_id_still_works(self) -> None:
        """Legacy topic_id path is unaffected."""
        cfg = StandaloneConfig(consumer_name="test", topic_id=42)
        assert cfg.topic_id == 42
        assert cfg.topic_name == ""

    def test_both_can_be_set(self) -> None:
        """topic_name and topic_id may both be provided (ID takes precedence at runtime)."""
        cfg = StandaloneConfig(consumer_name="test", topic_id=5, topic_name="rithmic-dev")
        assert cfg.topic_id == 5
        assert cfg.topic_name == "rithmic-dev"

    def test_invalid_topic_name_raises_at_construction(self) -> None:
        """Validation fires in __init__, not later."""
        with pytest.raises(ValueError, match="Invalid topic name"):
            StandaloneConfig(consumer_name="test", topic_name="invalid name!")

    def test_empty_topic_name_is_ok(self) -> None:
        """Empty string means 'not set' and should not raise."""
        cfg = StandaloneConfig(consumer_name="test", topic_name="")
        assert cfg.topic_name == ""

    @pytest.mark.parametrize(
        "name",
        ["rithmic-dev", "my-events", "topic123", "A-B-C"],
    )
    def test_valid_names_accepted(self, name: str) -> None:
        cfg = StandaloneConfig(consumer_name="test", topic_name=name)
        assert cfg.topic_name == name

    @pytest.mark.parametrize(
        "name",
        ["has space", "under_score", "dot.name", "slash/path"],
    )
    def test_invalid_names_rejected_at_construction(self, name: str) -> None:
        with pytest.raises(ValueError):
            StandaloneConfig(consumer_name="test", topic_name=name)

    def test_default_topic_name_is_empty(self) -> None:
        cfg = StandaloneConfig(consumer_name="test")
        assert cfg.topic_name == ""

    def test_builder_pattern_unaffected(self) -> None:
        """Existing builder methods still work alongside topic_name."""
        from lnc_client.config import SeekPosition

        cfg = (
            StandaloneConfig(consumer_name="test", topic_name="my-topic")
            .with_start_position(SeekPosition.END)
            .with_manual_commit()
        )
        assert cfg.topic_name == "my-topic"
        assert cfg.start_position is SeekPosition.END
        assert cfg.auto_commit_interval_s is None


# ---------------------------------------------------------------------------
# error_from_response — InvalidTopicName (0x12)
# ---------------------------------------------------------------------------


class TestErrorFromResponseInvalidTopicName:
    """Verify error_from_response maps 0x12 to InvalidTopicNameError."""

    def test_code_0x12_maps_to_invalid_topic_name_error(self) -> None:
        from lnc_client.errors import error_from_response

        err = error_from_response(0x12, "bad-name-from-server")
        assert isinstance(err, InvalidTopicNameError)

    def test_invalid_topic_name_is_not_retryable(self) -> None:
        from lnc_client.errors import error_from_response

        err = error_from_response(0x12, "bad-name")
        assert not err.is_retryable()
