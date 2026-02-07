"""Tests for TLV record encoding and decoding."""

import struct

import pytest

from lnc_client.tlv import (
    RecordType,
    TlvRecord,
    encode_records,
    decode_records,
)


class TestTlvRecord:
    """Test individual TLV record operations."""

    def test_raw_record(self):
        rec = TlvRecord.raw(b"hello")
        assert rec.record_type == RecordType.RAW_DATA
        assert rec.value == b"hello"
        assert rec.total_size == 5 + 5  # 5-byte header + 5-byte value

    def test_json_record(self):
        data = b'{"key": "value"}'
        rec = TlvRecord.json(data)
        assert rec.record_type == RecordType.JSON
        assert rec.value == data

    def test_null_record(self):
        rec = TlvRecord.null()
        assert rec.record_type == RecordType.NULL
        assert rec.value == b""
        assert rec.total_size == 5

    def test_key_value_record(self):
        rec = TlvRecord.key_value("mykey", b"myvalue")
        assert rec.record_type == RecordType.KEY_VALUE

        key, val = rec.as_key_value()
        assert key == "mykey"
        assert val == b"myvalue"

    def test_timestamped_record(self):
        ts = 1706918400_000_000_000
        rec = TlvRecord.timestamped(ts, b"event data")
        assert rec.record_type == RecordType.TIMESTAMPED

        parsed_ts, parsed_data = rec.as_timestamped()
        assert parsed_ts == ts
        assert parsed_data == b"event data"

    def test_encode_single(self):
        rec = TlvRecord.raw(b"test")
        encoded = rec.encode()
        # type(1) + length(4) + value(4) = 9 bytes
        assert len(encoded) == 9
        assert encoded[0] == RecordType.RAW_DATA
        length = struct.unpack_from("<I", encoded, 1)[0]
        assert length == 4
        assert encoded[5:] == b"test"


class TestEncodeDecodeRoundtrip:
    """Test encoding and decoding multiple records."""

    def test_spec_example(self):
        """Test the encoding example from the spec: 'hello' and 'world'."""
        records = [TlvRecord.raw(b"hello"), TlvRecord.raw(b"world")]
        payload = encode_records(records)

        # Expected: 01 05000000 68656C6C6F 01 05000000 776F726C64
        assert len(payload) == 20
        assert payload == (
            b"\x01\x05\x00\x00\x00hello"
            b"\x01\x05\x00\x00\x00world"
        )

    def test_roundtrip_single(self):
        original = TlvRecord.raw(b"single record")
        payload = encode_records([original])
        decoded = decode_records(payload)

        assert len(decoded) == 1
        assert decoded[0].record_type == original.record_type
        assert decoded[0].value == original.value

    def test_roundtrip_multiple(self):
        originals = [
            TlvRecord.raw(b"first"),
            TlvRecord.json(b'{"n":1}'),
            TlvRecord.key_value("k", b"v"),
            TlvRecord.null(),
        ]
        payload = encode_records(originals)
        decoded = decode_records(payload)

        assert len(decoded) == len(originals)
        for orig, dec in zip(originals, decoded):
            assert dec.record_type == orig.record_type
            assert dec.value == orig.value

    def test_roundtrip_large(self):
        data = b"x" * 100_000
        rec = TlvRecord.raw(data)
        payload = encode_records([rec])
        decoded = decode_records(payload)

        assert len(decoded) == 1
        assert decoded[0].value == data

    def test_decode_with_expected_count(self):
        records = [TlvRecord.raw(b"a"), TlvRecord.raw(b"b"), TlvRecord.raw(b"c")]
        payload = encode_records(records)

        # Only decode first 2
        decoded = decode_records(payload, expected_count=2)
        assert len(decoded) == 2
        assert decoded[0].value == b"a"
        assert decoded[1].value == b"b"

    def test_decode_truncated(self):
        rec = TlvRecord.raw(b"hello")
        payload = rec.encode()

        # Truncate the payload (remove last byte)
        truncated = payload[:-1]
        decoded = decode_records(truncated)
        assert len(decoded) == 0  # Can't decode truncated record

    def test_empty_payload(self):
        decoded = decode_records(b"")
        assert decoded == []

    def test_contiguous_packing(self):
        """Verify records are packed contiguously with no padding."""
        records = [TlvRecord.raw(b"ab"), TlvRecord.raw(b"cde")]
        payload = encode_records(records)

        # Record 1: 1 + 4 + 2 = 7 bytes
        # Record 2: 1 + 4 + 3 = 8 bytes
        # Total: 15 bytes (no padding)
        assert len(payload) == 15
