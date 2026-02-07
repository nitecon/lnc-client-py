"""Lance Wire Protocol (LWP) frame parsing and serialization.

Implements the 44-byte LWP header with CRC32C validation, flag handling,
and frame builders for all frame types (Ingest, Ack, Keepalive, Backpressure,
Control).

Wire format (44 bytes, little-endian):
    Offset  Size  Field
    0       4     Magic ("LANC")
    4       1     Version
    5       1     Flags
    6       2     Reserved
    8       4     Header CRC32C (of bytes 0-7)
    12      8     Batch ID
    20      8     Timestamp NS
    28      4     Record Count
    32      4     Payload Length
    36      4     Payload CRC32C
    40      4     Topic ID
"""

from __future__ import annotations

import struct
import time
from dataclasses import dataclass, field
from enum import IntEnum, IntFlag

import crc32c

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAGIC = b"LANC"
PROTOCOL_VERSION = 1
HEADER_SIZE = 44
DEFAULT_PORT = 1992
KEEPALIVE_INTERVAL_S = 10
KEEPALIVE_TIMEOUT_S = 30
MAX_RECORD_SIZE = 16 * 1024 * 1024  # 16 MB

# struct format: magic(4s) version(B) flags(B) reserved(H) header_crc(I)
#                batch_id(Q) timestamp_ns(Q) record_count(I)
#                payload_length(I) payload_crc(I) topic_id(I)
_HEADER_FMT = "<4sBBHIQQIIII"
_HEADER_STRUCT = struct.Struct(_HEADER_FMT)

assert _HEADER_STRUCT.size == HEADER_SIZE, (
    f"Header struct size mismatch: {_HEADER_STRUCT.size} != {HEADER_SIZE}"
)


# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------


class Flag(IntFlag):
    """LWP header flag bits."""

    COMPRESSED = 0x01
    ENCRYPTED = 0x02
    BATCH_MODE = 0x04
    ACK = 0x08
    BACKPRESSURE = 0x10
    KEEPALIVE = 0x20
    CONTROL = 0x40


# ---------------------------------------------------------------------------
# Control Commands
# ---------------------------------------------------------------------------


class ControlCommand(IntEnum):
    """Control frame command codes (carried in batch_id field)."""

    CREATE_TOPIC = 0x01
    DELETE_TOPIC = 0x02
    LIST_TOPICS = 0x03
    GET_TOPIC = 0x04
    SET_RETENTION = 0x05
    CREATE_TOPIC_WITH_RETENTION = 0x06

    FETCH = 0x10
    FETCH_RESPONSE = 0x11

    SUBSCRIBE = 0x20
    UNSUBSCRIBE = 0x21
    COMMIT_OFFSET = 0x22
    SUBSCRIBE_ACK = 0x23
    COMMIT_ACK = 0x24

    TOPIC_RESPONSE = 0x80
    ERROR_RESPONSE = 0xFF


# ---------------------------------------------------------------------------
# Header dataclass
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class LwpHeader:
    """Parsed LWP frame header."""

    version: int = PROTOCOL_VERSION
    flags: int = 0
    header_crc: int = 0
    batch_id: int = 0
    timestamp_ns: int = 0
    record_count: int = 0
    payload_length: int = 0
    payload_crc: int = 0
    topic_id: int = 0

    # ----- properties -----

    @property
    def is_ack(self) -> bool:
        return bool(self.flags & Flag.ACK)

    @property
    def is_keepalive(self) -> bool:
        return bool(self.flags & Flag.KEEPALIVE)

    @property
    def is_backpressure(self) -> bool:
        return bool(self.flags & Flag.BACKPRESSURE)

    @property
    def is_control(self) -> bool:
        return bool(self.flags & Flag.CONTROL)

    @property
    def is_batch(self) -> bool:
        return bool(self.flags & Flag.BATCH_MODE)

    @property
    def is_compressed(self) -> bool:
        return bool(self.flags & Flag.COMPRESSED)

    @property
    def command(self) -> ControlCommand | None:
        """Return the control command if this is a control frame."""
        if not self.is_control:
            return None
        try:
            return ControlCommand(self.batch_id)
        except ValueError:
            return None

    # ----- serialization -----

    def encode(self) -> bytes:
        """Serialize this header to 44 bytes with computed CRC."""
        # Build first 8 bytes for CRC computation
        pre = struct.pack("<4sBBH", MAGIC, self.version, self.flags, 0)
        hdr_crc = crc32c.crc32c(pre)

        return _HEADER_STRUCT.pack(
            MAGIC,
            self.version,
            self.flags,
            0,  # reserved
            hdr_crc,
            self.batch_id,
            self.timestamp_ns,
            self.record_count,
            self.payload_length,
            self.payload_crc,
            self.topic_id,
        )

    @classmethod
    def decode(cls, buf: bytes | bytearray | memoryview) -> "LwpHeader":
        """Parse a 44-byte buffer into an LwpHeader.

        Raises:
            lnc_client.errors.InvalidFrameError: on magic or CRC mismatch.
        """
        from lnc_client.errors import InvalidFrameError

        if len(buf) < HEADER_SIZE:
            raise InvalidFrameError(
                f"Buffer too small: {len(buf)} < {HEADER_SIZE}"
            )

        (
            magic,
            version,
            flags,
            _reserved,
            hdr_crc,
            batch_id,
            timestamp_ns,
            record_count,
            payload_length,
            payload_crc,
            topic_id,
        ) = _HEADER_STRUCT.unpack_from(buf)

        if magic != MAGIC:
            raise InvalidFrameError(f"Invalid magic: {magic!r}")

        # Validate header CRC (covers bytes 0-7)
        expected_crc = crc32c.crc32c(bytes(buf[:8]))
        if hdr_crc != expected_crc:
            raise InvalidFrameError(
                f"Header CRC mismatch: got {hdr_crc:#010x}, "
                f"expected {expected_crc:#010x}"
            )

        return cls(
            version=version,
            flags=flags,
            header_crc=hdr_crc,
            batch_id=batch_id,
            timestamp_ns=timestamp_ns,
            record_count=record_count,
            payload_length=payload_length,
            payload_crc=payload_crc,
            topic_id=topic_id,
        )


# ---------------------------------------------------------------------------
# Frame builders
# ---------------------------------------------------------------------------


def _now_ns() -> int:
    return int(time.time() * 1_000_000_000)


def build_ingest_frame(
    payload: bytes,
    batch_id: int,
    record_count: int,
    topic_id: int = 0,
    *,
    compressed: bool = False,
) -> bytes:
    """Build a complete Ingest frame (header + payload)."""
    flags = Flag.BATCH_MODE
    if compressed:
        flags |= Flag.COMPRESSED

    payload_crc = crc32c.crc32c(payload)
    hdr = LwpHeader(
        flags=flags,
        batch_id=batch_id,
        timestamp_ns=_now_ns(),
        record_count=record_count,
        payload_length=len(payload),
        payload_crc=payload_crc,
        topic_id=topic_id,
    )
    return hdr.encode() + payload


def build_keepalive_frame() -> bytes:
    """Build a Keepalive frame (header only, no payload)."""
    return LwpHeader(flags=Flag.KEEPALIVE).encode()


def build_control_frame(
    command: ControlCommand,
    payload: bytes = b"",
    topic_id: int = 0,
) -> bytes:
    """Build a Control frame for topic management or fetch operations."""
    payload_crc = crc32c.crc32c(payload) if payload else 0
    hdr = LwpHeader(
        flags=Flag.CONTROL,
        batch_id=int(command),
        payload_length=len(payload),
        payload_crc=payload_crc,
        topic_id=topic_id,
    )
    return hdr.encode() + payload


# --- Control payload builders ---


def build_fetch_payload(topic_id: int, offset: int, max_bytes: int) -> bytes:
    """Build Fetch request payload (16 bytes)."""
    return struct.pack("<IQI", topic_id, offset, max_bytes)


def build_subscribe_payload(
    topic_id: int,
    start_offset: int,
    max_batch_bytes: int,
    consumer_id: int,
) -> bytes:
    """Build Subscribe request payload (24 bytes)."""
    return struct.pack("<IQIQ", topic_id, start_offset, max_batch_bytes, consumer_id)


def build_unsubscribe_payload(topic_id: int, consumer_id: int) -> bytes:
    """Build Unsubscribe request payload (12 bytes)."""
    return struct.pack("<IQ", topic_id, consumer_id)


def build_commit_offset_payload(
    topic_id: int, consumer_id: int, offset: int
) -> bytes:
    """Build CommitOffset request payload (20 bytes)."""
    return struct.pack("<IQQ", topic_id, consumer_id, offset)


def build_set_retention_payload(
    topic_id: int, max_age_secs: int, max_bytes: int
) -> bytes:
    """Build SetRetention request payload (20 bytes)."""
    return struct.pack("<IQQ", topic_id, max_age_secs, max_bytes)


def build_create_topic_with_retention_payload(
    name: str, max_age_secs: int, max_bytes: int
) -> bytes:
    """Build CreateTopicWithRetention request payload."""
    name_bytes = name.encode("utf-8")
    return struct.pack("<H", len(name_bytes)) + name_bytes + struct.pack("<QQ", max_age_secs, max_bytes)


# --- Response parsers ---


def parse_fetch_response(payload: bytes) -> tuple[int, int, int, bytes]:
    """Parse a FetchResponse payload.

    Returns:
        (start_offset, end_offset, high_water_mark, data)
    """
    if len(payload) < 24:
        # Fallback to basic format: next_offset(8) + bytes_returned(4) + record_count(4) + data
        if len(payload) < 16:
            return (0, 0, 0, b"")
        next_off, bytes_ret, rec_count = struct.unpack_from("<QII", payload)
        data = payload[16 : 16 + bytes_ret]
        return (0, next_off, next_off, data)

    start_off, end_off, hwm = struct.unpack_from("<QQQ", payload)
    data = payload[24:]
    return (start_off, end_off, hwm, data)
