"""Lance Wire Protocol error types."""

from __future__ import annotations

import re


class LanceError(Exception):
    """Base exception for all Lance client errors."""

    def is_retryable(self) -> bool:
        """Whether this error is transient and the operation can be retried.

        Retryable errors: ConnectionError, TimeoutError, BackpressureError,
        ServerCatchingUpError, NotLeaderError.
        """
        return False


class ConnectionError(LanceError):
    """Connection-level errors (refused, closed, DNS failure)."""

    def is_retryable(self) -> bool:
        return True


class ProtocolError(LanceError):
    """Protocol-level errors (invalid frame, version mismatch)."""


class InvalidFrameError(ProtocolError):
    """CRC mismatch or malformed header."""


class TimeoutError(LanceError):
    """Operation exceeded deadline."""

    def is_retryable(self) -> bool:
        return True


class BackpressureError(LanceError):
    """Server requested slowdown."""

    def is_retryable(self) -> bool:
        return True


class ServerCatchingUpError(LanceError):
    """Server has not yet replicated to the requested offset — backoff and retry."""

    def __init__(self, server_offset: int = 0) -> None:
        super().__init__(f"Server catching up (at offset {server_offset})")
        self.server_offset = server_offset

    def is_retryable(self) -> bool:
        return True


class TopicNotFoundError(LanceError):
    """Specified topic ID does not exist."""

    def __init__(self, topic_id: int | str) -> None:
        super().__init__(f"Topic not found: {topic_id}")
        self.topic_id = topic_id


class TopicAlreadyExistsError(LanceError):
    """Topic with specified name already exists."""


class InvalidTopicNameError(LanceError):
    """Topic name does not match the required pattern ``[a-zA-Z0-9-]+``."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"Invalid topic name: {name!r}. "
            "Names must contain only alphanumeric characters and dashes ([a-zA-Z0-9-])."
        )
        self.name = name


class NotLeaderError(LanceError):
    """This node is not the cluster leader."""

    def __init__(self, leader_addr: str | None = None) -> None:
        msg = "Not leader"
        if leader_addr:
            msg += f" — redirect to {leader_addr}"
        super().__init__(msg)
        self.leader_addr = leader_addr

    def is_retryable(self) -> bool:
        return True


class AccessDeniedError(LanceError):
    """Client not authorized for requested operation."""


# Map server error codes to exception classes
ERROR_CODE_MAP: dict[int, type[LanceError]] = {
    0x01: LanceError,  # UnknownError
    0x02: InvalidFrameError,  # InvalidMagic
    0x03: ProtocolError,  # PayloadTooLarge
    0x04: ProtocolError,  # InvalidPayload
    0x05: InvalidFrameError,  # CrcMismatch
    0x06: ProtocolError,  # VersionMismatch
    0x10: TopicNotFoundError,  # TopicNotFound
    0x11: TopicAlreadyExistsError,
    0x12: InvalidTopicNameError,  # InvalidTopicName
    0x13: TopicNotFoundError,  # TopicDeleted
    0x14: ServerCatchingUpError,  # ServerCatchingUp
    0x20: NotLeaderError,  # NotLeader
    0x30: BackpressureError,  # RateLimited
    0x31: BackpressureError,  # Backpressure
    0x40: AccessDeniedError,  # AuthenticationRequired
    0x41: AccessDeniedError,  # AuthenticationFailed
    0x42: AccessDeniedError,  # AccessDenied
    0x50: LanceError,  # InvalidOffset
    0x51: LanceError,  # OffsetOutOfRange
    0x60: LanceError,  # InternalError
    0x61: LanceError,  # StorageError
    0x62: TimeoutError,  # TimeoutError
}


_TOPIC_NAME_RE = re.compile(r"^[a-zA-Z0-9-]+$")


def validate_topic_name(name: str) -> None:
    """Validate a topic name against the ``[a-zA-Z0-9-]+`` pattern.

    Raises ``InvalidTopicNameError`` if the name is invalid.
    """
    if not name or not _TOPIC_NAME_RE.match(name):
        raise InvalidTopicNameError(name)


def error_from_response(code: int, message: str, details: dict | None = None) -> LanceError:
    """Create the appropriate exception from a server error response."""
    exc_class = ERROR_CODE_MAP.get(code, LanceError)

    if exc_class is NotLeaderError:
        leader = details.get("leader_addr") if details else None
        return NotLeaderError(leader)
    if exc_class is TopicNotFoundError:
        return TopicNotFoundError(message)
    if exc_class is InvalidTopicNameError:
        # Extract raw name from message if possible; fall back to the full message
        return InvalidTopicNameError(message)
    if exc_class is ServerCatchingUpError:
        offset = details.get("server_offset", 0) if details else 0
        return ServerCatchingUpError(offset)
    if exc_class is InvalidTopicNameError:
        return InvalidTopicNameError(message)

    return exc_class(message)
