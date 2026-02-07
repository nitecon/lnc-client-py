"""TLV (Type-Length-Value) record encoding and decoding.

Each record in an LWP payload is encoded as:
    Offset  Size  Field
    0       1     Type (record type identifier)
    1       4     Length (little-endian u32, size of value)
    5       N     Value (record data, N = Length)

Total record size: 5 + Length bytes.
Records are packed contiguously with no alignment or padding.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum


class RecordType(IntEnum):
    """TLV record type identifiers."""

    RESERVED = 0x00
    RAW_DATA = 0x01
    JSON = 0x02
    MSGPACK = 0x03
    PROTOBUF = 0x04
    AVRO = 0x05
    KEY_VALUE = 0x10
    TIMESTAMPED = 0x11
    KEY_TIMESTAMPED = 0x12
    NULL = 0xFF


# TLV header: type(B) + length(I) = 5 bytes
_TLV_HDR_FMT = struct.Struct("<BI")
_TLV_HDR_SIZE = 5


@dataclass(slots=True)
class TlvRecord:
    """A single TLV record."""

    record_type: int
    value: bytes

    @property
    def total_size(self) -> int:
        return _TLV_HDR_SIZE + len(self.value)

    def encode(self) -> bytes:
        """Encode this record to bytes."""
        return _TLV_HDR_FMT.pack(self.record_type, len(self.value)) + self.value

    @classmethod
    def raw(cls, data: bytes) -> TlvRecord:
        """Create a RawData record."""
        return cls(record_type=RecordType.RAW_DATA, value=data)

    @classmethod
    def json(cls, data: bytes) -> TlvRecord:
        """Create a JSON record from already-encoded JSON bytes."""
        return cls(record_type=RecordType.JSON, value=data)

    @classmethod
    def key_value(cls, key: str, value: bytes) -> TlvRecord:
        """Create a KeyValue record."""
        key_bytes = key.encode("utf-8")
        payload = struct.pack("<H", len(key_bytes)) + key_bytes + value
        return cls(record_type=RecordType.KEY_VALUE, value=payload)

    @classmethod
    def timestamped(cls, timestamp_ns: int, data: bytes) -> TlvRecord:
        """Create a Timestamped record."""
        payload = struct.pack("<Q", timestamp_ns) + data
        return cls(record_type=RecordType.TIMESTAMPED, value=payload)

    @classmethod
    def null(cls) -> TlvRecord:
        """Create a Null/tombstone record."""
        return cls(record_type=RecordType.NULL, value=b"")

    # --- Accessors for structured types ---

    def as_key_value(self) -> tuple[str, bytes]:
        """Parse a KeyValue record into (key, value)."""
        if len(self.value) < 2:
            return ("", self.value)
        key_len = struct.unpack_from("<H", self.value)[0]
        key = self.value[2 : 2 + key_len].decode("utf-8", errors="replace")
        val = self.value[2 + key_len :]
        return (key, val)

    def as_timestamped(self) -> tuple[int, bytes]:
        """Parse a Timestamped record into (timestamp_ns, data)."""
        if len(self.value) < 8:
            return (0, self.value)
        ts = struct.unpack_from("<Q", self.value)[0]
        return (ts, self.value[8:])


def encode_records(records: list[TlvRecord]) -> bytes:
    """Encode a list of TLV records into a contiguous payload."""
    parts: list[bytes] = []
    for rec in records:
        parts.append(rec.encode())
    return b"".join(parts)


def decode_records(
    payload: bytes,
    expected_count: int | None = None,
) -> list[TlvRecord]:
    """Decode TLV records from a contiguous payload.

    Args:
        payload: Raw payload bytes containing packed TLV records.
        expected_count: Optional expected number of records (from header).

    Returns:
        List of decoded TlvRecord objects.
    """
    records: list[TlvRecord] = []
    offset = 0
    limit = expected_count if expected_count is not None else float("inf")

    while offset + _TLV_HDR_SIZE <= len(payload) and len(records) < limit:
        rec_type, length = _TLV_HDR_FMT.unpack_from(payload, offset)

        end = offset + _TLV_HDR_SIZE + length
        if end > len(payload):
            break  # Truncated record

        value = payload[offset + _TLV_HDR_SIZE : end]
        records.append(TlvRecord(record_type=rec_type, value=value))
        offset = end

    return records
