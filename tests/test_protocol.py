"""Tests for LWP header parsing, serialization, and CRC32C validation."""

import struct

import crc32c
import pytest

from lnc_client.errors import InvalidFrameError
from lnc_client.protocol import (
    HEADER_SIZE,
    MAGIC,
    PROTOCOL_VERSION,
    ControlCommand,
    Flag,
    LwpHeader,
    build_commit_offset_payload,
    build_control_frame,
    build_create_topic_with_retention_payload,
    build_fetch_payload,
    build_ingest_frame,
    build_keepalive_frame,
    build_set_retention_payload,
    build_subscribe_payload,
    build_unsubscribe_payload,
    parse_fetch_response,
)


class TestLwpHeader:
    """Test LWP header encode/decode roundtrip."""

    def test_header_size(self):
        hdr = LwpHeader()
        encoded = hdr.encode()
        assert len(encoded) == HEADER_SIZE == 44

    def test_roundtrip_defaults(self):
        hdr = LwpHeader()
        encoded = hdr.encode()
        decoded = LwpHeader.decode(encoded)
        assert decoded.version == PROTOCOL_VERSION
        assert decoded.flags == 0
        assert decoded.batch_id == 0
        assert decoded.topic_id == 0

    def test_roundtrip_ingest(self):
        hdr = LwpHeader(
            flags=Flag.BATCH_MODE,
            batch_id=42,
            timestamp_ns=1234567890,
            record_count=10,
            payload_length=4096,
            payload_crc=0xDEADBEEF,
            topic_id=3,
        )
        encoded = hdr.encode()
        decoded = LwpHeader.decode(encoded)

        assert decoded.flags == Flag.BATCH_MODE
        assert decoded.batch_id == 42
        assert decoded.timestamp_ns == 1234567890
        assert decoded.record_count == 10
        assert decoded.payload_length == 4096
        assert decoded.payload_crc == 0xDEADBEEF
        assert decoded.topic_id == 3

    def test_magic_bytes(self):
        hdr = LwpHeader()
        encoded = hdr.encode()
        assert encoded[:4] == MAGIC == b"LANC"

    def test_header_crc_validation(self):
        hdr = LwpHeader(flags=Flag.KEEPALIVE)
        encoded = bytearray(hdr.encode())

        # Corrupt a byte in the CRC-covered region (byte 5 = flags)
        encoded[5] ^= 0xFF

        with pytest.raises(InvalidFrameError, match="CRC mismatch"):
            LwpHeader.decode(bytes(encoded))

    def test_invalid_magic(self):
        bad = b"XYZW" + b"\x00" * 40
        with pytest.raises(InvalidFrameError, match="Invalid magic"):
            LwpHeader.decode(bad)

    def test_buffer_too_small(self):
        with pytest.raises(InvalidFrameError, match="Buffer too small"):
            LwpHeader.decode(b"\x00" * 10)

    def test_flag_properties(self):
        hdr = LwpHeader(flags=Flag.BATCH_MODE | Flag.COMPRESSED)
        assert hdr.is_batch
        assert hdr.is_compressed
        assert not hdr.is_ack
        assert not hdr.is_keepalive
        assert not hdr.is_control

        hdr2 = LwpHeader(flags=Flag.CONTROL, batch_id=ControlCommand.LIST_TOPICS)
        assert hdr2.is_control
        assert hdr2.command == ControlCommand.LIST_TOPICS


class TestKeepaliveFrame:
    """Test keepalive frame against spec test vector."""

    def test_keepalive_frame_structure(self):
        frame = build_keepalive_frame()
        assert len(frame) == HEADER_SIZE

        hdr = LwpHeader.decode(frame)
        assert hdr.is_keepalive
        assert hdr.batch_id == 0
        assert hdr.payload_length == 0

    def test_keepalive_crc_matches_spec(self):
        """Verify CRC matches the test vector from the spec."""
        frame = build_keepalive_frame()
        # Header CRC is at bytes 8-11 (LE)
        hdr_crc = struct.unpack_from("<I", frame, 8)[0]

        # Manually compute: CRC32C of first 8 bytes
        expected = crc32c.crc32c(frame[:8])
        assert hdr_crc == expected


class TestIngestFrame:
    """Test ingest frame building."""

    def test_ingest_frame_structure(self):
        payload = b"hello world"
        frame = build_ingest_frame(
            payload=payload,
            batch_id=1,
            record_count=1,
            topic_id=5,
        )
        assert len(frame) == HEADER_SIZE + len(payload)

        hdr = LwpHeader.decode(frame[:HEADER_SIZE])
        assert hdr.is_batch
        assert hdr.batch_id == 1
        assert hdr.record_count == 1
        assert hdr.payload_length == len(payload)
        assert hdr.topic_id == 5

        # Verify payload CRC
        actual_payload = frame[HEADER_SIZE:]
        assert actual_payload == payload
        assert hdr.payload_crc == crc32c.crc32c(payload)

    def test_compressed_flag(self):
        frame = build_ingest_frame(
            payload=b"test",
            batch_id=1,
            record_count=1,
            compressed=True,
        )
        hdr = LwpHeader.decode(frame[:HEADER_SIZE])
        assert hdr.is_compressed
        assert hdr.is_batch


class TestControlFrame:
    """Test control frame building."""

    def test_create_topic(self):
        frame = build_control_frame(ControlCommand.CREATE_TOPIC, b"my-events")
        hdr = LwpHeader.decode(frame[:HEADER_SIZE])
        assert hdr.is_control
        assert hdr.command == ControlCommand.CREATE_TOPIC
        assert hdr.payload_length == len(b"my-events")
        assert frame[HEADER_SIZE:] == b"my-events"

    def test_list_topics_no_payload(self):
        frame = build_control_frame(ControlCommand.LIST_TOPICS)
        hdr = LwpHeader.decode(frame[:HEADER_SIZE])
        assert hdr.is_control
        assert hdr.command == ControlCommand.LIST_TOPICS
        assert hdr.payload_length == 0


class TestControlPayloads:
    """Test control payload builders and parsers."""

    def test_fetch_payload(self):
        payload = build_fetch_payload(topic_id=3, offset=1000, max_bytes=65536)
        assert len(payload) == 16
        tid, off, mb = struct.unpack("<IQI", payload)
        assert tid == 3
        assert off == 1000
        assert mb == 65536

    def test_subscribe_payload(self):
        payload = build_subscribe_payload(
            topic_id=1, start_offset=0, max_batch_bytes=32768, consumer_id=42
        )
        assert len(payload) == 24

    def test_unsubscribe_payload(self):
        payload = build_unsubscribe_payload(topic_id=1, consumer_id=42)
        assert len(payload) == 12

    def test_commit_offset_payload(self):
        payload = build_commit_offset_payload(topic_id=1, consumer_id=42, offset=5000)
        assert len(payload) == 20

    def test_set_retention_payload(self):
        payload = build_set_retention_payload(topic_id=1, max_age_secs=86400, max_bytes=1073741824)
        assert len(payload) == 20
        tid, age, mb = struct.unpack("<IQQ", payload)
        assert tid == 1
        assert age == 86400
        assert mb == 1073741824

    def test_create_topic_with_retention(self):
        payload = build_create_topic_with_retention_payload(
            "events", max_age_secs=604800, max_bytes=0
        )
        name_len = struct.unpack_from("<H", payload)[0]
        assert name_len == 6
        name = payload[2 : 2 + name_len].decode()
        assert name == "events"

    def test_parse_fetch_response_extended(self):
        # Build extended format: start(8) + end(8) + hwm(8) + data
        data = b"record data here"
        resp = struct.pack("<QQQ", 100, 200, 500) + data
        start, end, hwm, parsed_data = parse_fetch_response(resp)
        assert start == 100
        assert end == 200
        assert hwm == 500
        assert parsed_data == data

    def test_parse_fetch_response_basic(self):
        # Build basic format: next_offset(8) + bytes_ret(4) + rec_count(4) + data
        data = b"abc"
        resp = struct.pack("<QII", 300, len(data), 1) + data
        start, end, hwm, parsed_data = parse_fetch_response(resp)
        assert end == 300
        assert parsed_data == data


class TestCrc32cVectors:
    """Verify CRC32C against spec test vectors."""

    def test_empty(self):
        assert crc32c.crc32c(b"") == 0x00000000

    def test_single_char(self):
        assert crc32c.crc32c(b"a") == 0xC1D04330

    def test_hello(self):
        assert crc32c.crc32c(b"hello") == 0x9A71BB4C
