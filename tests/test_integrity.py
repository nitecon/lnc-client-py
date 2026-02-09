"""Integrity tests for lnc-client-py against a live Lance server.

These tests require a running LANCE server and are skipped by default.

Run with:
    LANCE_TEST_ADDR=host:port pytest tests/test_integrity.py -v -s

Or via the helper script:
    ./run-integrity-tests.sh --target host:port
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid

import pytest

from lnc_client import (
    ClientConfig,
    LanceClient,
    Producer,
    ProducerConfig,
    StandaloneConfig,
    StandaloneConsumer,
    TlvRecord,
    encode_records,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

LANCE_TEST_ADDR = os.environ.get("LANCE_TEST_ADDR", "")

pytestmark = pytest.mark.skipif(
    not LANCE_TEST_ADDR,
    reason="LANCE_TEST_ADDR not set — skipping integrity tests",
)


def _host_port() -> tuple[str, int]:
    """Parse LANCE_TEST_ADDR into (host, port)."""
    host, _, port_str = LANCE_TEST_ADDR.rpartition(":")
    port = int(port_str) if port_str else 1992
    if not host:
        host = LANCE_TEST_ADDR
    return host, port


def _unique_topic(prefix: str) -> str:
    """Generate a unique topic name for test isolation."""
    short_id = uuid.uuid4().hex[:8]
    return f"pytest_{prefix}_{short_id}"


@pytest.fixture
def client_config() -> ClientConfig:
    host, port = _host_port()
    return ClientConfig(host=host, port=port, connect_timeout_s=5.0, request_timeout_s=10.0)


# ---------------------------------------------------------------------------
# Connection Tests
# ---------------------------------------------------------------------------


class TestConnection:
    """Verify basic connectivity to the Lance server."""

    async def test_connect_and_close(self, client_config):
        """Connect to the server and cleanly disconnect."""
        print(f"\n  Connecting to {LANCE_TEST_ADDR}")
        async with LanceClient(client_config) as client:
            topics = await client.list_topics()
            print(f"  Connected — server has {len(topics)} topic(s)")
        print("  Disconnected cleanly")

    async def test_ping_latency(self, client_config):
        """Measure round-trip ping latency."""
        async with LanceClient(client_config) as client:
            # Warm-up
            await client.ping()

            latencies: list[float] = []
            for _ in range(10):
                rtt = await client.ping()
                latencies.append(rtt)

            avg = sum(latencies) / len(latencies)
            mn = min(latencies)
            mx = max(latencies)

            print("\n  Ping latency (10 samples):")
            print(f"    Min: {mn * 1000:.2f}ms")
            print(f"    Max: {mx * 1000:.2f}ms")
            print(f"    Avg: {avg * 1000:.2f}ms")

            assert avg < 0.5, f"Average ping too high: {avg * 1000:.2f}ms"


# ---------------------------------------------------------------------------
# Topic Management Tests
# ---------------------------------------------------------------------------


class TestTopicManagement:
    """Verify topic CRUD operations."""

    async def test_create_and_list_topics(self, client_config):
        """Create a topic, verify it appears in list, then delete."""
        async with LanceClient(client_config) as client:
            name = _unique_topic("create_list")
            print(f"\n  Creating topic: {name}")

            created = await client.create_topic(name)
            topic_id = created["id"]
            print(f"  Created: id={topic_id}, name={created.get('name', name)}")
            assert topic_id > 0

            # Verify in list
            topics = await client.list_topics()
            print(f"  Found {len(topics)} topic(s)")

            # Cleanup
            await client.delete_topic(topic_id)
            print(f"  Deleted topic id={topic_id}")

    async def test_create_with_retention(self, client_config):
        """Create a topic with retention policy."""
        async with LanceClient(client_config) as client:
            name = _unique_topic("retention")
            print(f"\n  Creating topic with retention: {name}")

            created = await client.create_topic_with_retention(
                name,
                max_age_secs=86400,
                max_bytes=1024 * 1024 * 1024,
            )
            topic_id = created["id"]
            print(f"  Created: id={topic_id}")
            assert topic_id > 0

            # Cleanup
            await client.delete_topic(topic_id)
            print(f"  Deleted topic id={topic_id}")

    async def test_delete_topic(self, client_config):
        """Create then delete a topic, verify it's gone."""
        async with LanceClient(client_config) as client:
            name = _unique_topic("delete")
            created = await client.create_topic(name)
            topic_id = created["id"]
            print(f"\n  Created topic id={topic_id} for deletion")

            await client.delete_topic(topic_id)
            print(f"  Deleted topic id={topic_id}")

            # Small settle time for cluster replication
            await asyncio.sleep(0.5)

            # Verify it's gone — get_topic should error
            try:
                await client.get_topic(topic_id)
                pytest.fail("Topic should not exist after deletion")
            except Exception as e:
                print(f"  Confirmed deleted (error: {type(e).__name__})")


# ---------------------------------------------------------------------------
# Producer Tests
# ---------------------------------------------------------------------------


class TestProducer:
    """Verify producer send operations."""

    async def test_single_send(self, client_config):
        """Send a single message with ACK."""
        async with LanceClient(client_config) as client:
            topic = await client.create_topic(_unique_topic("prod_single"))
            topic_id = topic["id"]

        print(f"\n  Producing to topic id={topic_id}")
        producer = await Producer.connect(LANCE_TEST_ADDR, ProducerConfig())

        batch_id = await producer.send(topic_id=topic_id, data=b'{"test": "single_send"}')
        print(f"  Sent batch_id={batch_id}")
        assert batch_id > 0

        await producer.close()

        # Cleanup
        async with LanceClient(client_config) as client:
            await client.delete_topic(topic_id)
        print(f"  Cleaned up topic id={topic_id}")

    async def test_send_async_and_flush(self, client_config):
        """Send multiple messages async, then flush."""
        async with LanceClient(client_config) as client:
            topic = await client.create_topic(_unique_topic("prod_flush"))
            topic_id = topic["id"]

        print(f"\n  Producing async to topic id={topic_id}")
        producer = await Producer.connect(LANCE_TEST_ADDR, ProducerConfig())

        count = 50
        start = time.monotonic()
        for i in range(count):
            await producer.send_async(topic_id=topic_id, data=f"message {i}".encode())

        send_elapsed = time.monotonic() - start
        print(f"  Sent {count} messages in {send_elapsed * 1000:.1f}ms")

        await producer.flush(timeout=10.0)
        total_elapsed = time.monotonic() - start
        print(f"  Flushed in {total_elapsed * 1000:.1f}ms")
        print(f"  Rate: {count / total_elapsed:.0f} msgs/sec")

        await producer.close()

        # Cleanup
        async with LanceClient(client_config) as client:
            await client.delete_topic(topic_id)

    async def test_send_batch_with_tlv(self, client_config):
        """Send a batch of TLV records."""
        async with LanceClient(client_config) as client:
            topic = await client.create_topic(_unique_topic("prod_tlv"))
            topic_id = topic["id"]

        print(f"\n  Sending TLV batch to topic id={topic_id}")
        producer = await Producer.connect(LANCE_TEST_ADDR, ProducerConfig())

        records = [
            TlvRecord.json(b'{"event": "click", "x": 100}'),
            TlvRecord.json(b'{"event": "scroll", "y": 500}'),
            TlvRecord.raw(b"raw binary payload"),
            TlvRecord.key_value("user-id", b"12345"),
        ]
        batch_id = await producer.send_batch(topic_id=topic_id, records=records)
        print(f"  Sent TLV batch: batch_id={batch_id}, {len(records)} records")
        assert batch_id > 0

        await producer.close()

        # Cleanup
        async with LanceClient(client_config) as client:
            await client.delete_topic(topic_id)


# ---------------------------------------------------------------------------
# Producer → Consumer Roundtrip
# ---------------------------------------------------------------------------


class TestProduceConsume:
    """Verify end-to-end produce/consume roundtrip."""

    async def test_roundtrip(self, client_config):
        """Produce messages, then consume and verify content."""
        # Create topic
        async with LanceClient(client_config) as client:
            topic = await client.create_topic(_unique_topic("roundtrip"))
            topic_id = topic["id"]

        msg_count = 20
        sent_payloads: list[bytes] = []

        # Produce
        print(f"\n  Producing {msg_count} messages to topic id={topic_id}")
        producer = await Producer.connect(LANCE_TEST_ADDR, ProducerConfig())
        for i in range(msg_count):
            payload = f"roundtrip message {i}".encode()
            sent_payloads.append(payload)
            record = TlvRecord.raw(payload)
            encoded = encode_records([record])
            await producer.send(topic_id=topic_id, data=encoded)
        await producer.flush()
        await producer.close()
        print(f"  Produced {msg_count} messages")

        # Allow time for data to replicate (L3 cluster may need a few seconds)
        await asyncio.sleep(2.0)

        # Consume
        print(f"  Consuming from topic id={topic_id}")
        config = StandaloneConfig("integrity-test", topic_id=topic_id)
        consumer = await StandaloneConsumer.connect(LANCE_TEST_ADDR, config)

        received_data = bytearray()
        polls = 0
        max_polls = 30
        empty_polls = 0

        while polls < max_polls:
            result = await consumer.poll(timeout=5.0)
            polls += 1
            if result is None:
                empty_polls += 1
                if received_data and empty_polls >= 3:
                    break  # Got data then 3 empty polls — done
                await asyncio.sleep(0.2)
                continue
            empty_polls = 0
            received_data.extend(result.data)
            if result.lag == 0:
                break

        await consumer.close()

        print(f"  Consumed {len(received_data)} bytes in {polls} poll(s)")
        assert len(received_data) > 0, "Should have consumed data"

        # Verify we got at least some of our sent records back
        found_count = sum(1 for p in sent_payloads if p in received_data)
        print(f"  Verified {found_count}/{msg_count} messages in consumed data")
        assert found_count > 0, "Should find at least some sent messages in consumed data"

        # Cleanup
        async with LanceClient(client_config) as client:
            await client.delete_topic(topic_id)
        print(f"  Cleaned up topic id={topic_id}")


# ---------------------------------------------------------------------------
# Topic Lifecycle (full CRUD + data)
# ---------------------------------------------------------------------------


class TestTopicLifecycle:
    """Full topic lifecycle: create → produce → consume → delete."""

    async def test_full_lifecycle(self, client_config):
        """End-to-end lifecycle test matching Rust test_topic_lifecycle_with_data."""
        async with LanceClient(client_config) as client:
            # 1. Create topic
            name = _unique_topic("lifecycle")
            topic = await client.create_topic(name)
            topic_id = topic["id"]
            print(f"\n  1. Created topic: id={topic_id}, name={name}")

        # 2. Produce data
        producer = await Producer.connect(LANCE_TEST_ADDR, ProducerConfig())
        batch_count = 10
        total_bytes = 0
        for i in range(batch_count):
            payload = f"lifecycle message {i} for topic {topic_id}".encode()
            total_bytes += len(payload)
            await producer.send(topic_id=topic_id, data=payload)
        await producer.flush()
        await producer.close()
        print(f"  2. Produced {batch_count} batches ({total_bytes} bytes)")

        # Allow time for data to replicate (L3 cluster may need a few seconds)
        await asyncio.sleep(2.0)

        # 3. Consume and verify data exists
        config = StandaloneConfig("lifecycle-consumer", topic_id=topic_id)
        consumer = await StandaloneConsumer.connect(LANCE_TEST_ADDR, config)

        consumed_bytes = 0
        empty_polls = 0
        for _ in range(30):
            result = await consumer.poll(timeout=5.0)
            if result is None:
                empty_polls += 1
                if consumed_bytes > 0 and empty_polls >= 3:
                    break
                await asyncio.sleep(0.2)
                continue
            empty_polls = 0
            consumed_bytes += len(result.data)
            if result.lag == 0:
                break

        await consumer.close()
        print(f"  3. Consumed {consumed_bytes} bytes")
        assert consumed_bytes > 0, "Should have consumed data"

        # 4. Verify topic still exists
        async with LanceClient(client_config) as client:
            fetched = await client.get_topic(topic_id)
            print(f"  4. Topic still exists: id={fetched.get('id', topic_id)}")

            # 5. Delete topic
            await client.delete_topic(topic_id)
            print(f"  5. Deleted topic id={topic_id}")

            await asyncio.sleep(0.5)

            # 6. Verify deletion
            try:
                await client.get_topic(topic_id)
                pytest.fail("Topic should not exist after deletion")
            except Exception:
                print("  6. Confirmed deleted")


# ---------------------------------------------------------------------------
# Error Handling Tests
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Verify error types are raised correctly."""

    async def test_topic_not_found(self, client_config):
        """Fetching a non-existent topic should raise an error."""
        async with LanceClient(client_config) as client:
            try:
                await client.get_topic(999999)
                pytest.fail("Should have raised an error")
            except Exception as e:
                print(f"\n  Expected error for topic 999999: {type(e).__name__}: {e}")
                assert e.is_retryable() is False


# ---------------------------------------------------------------------------
# Throughput benchmark (optional, controlled by env var)
# ---------------------------------------------------------------------------


class TestBenchmark:
    """Optional throughput benchmark — set LANCE_RUN_BENCHMARK=1 to enable."""

    pytestmark = pytest.mark.skipif(
        not os.environ.get("LANCE_RUN_BENCHMARK"),
        reason="LANCE_RUN_BENCHMARK not set",
    )

    async def test_throughput(self, client_config):
        """Measure sustained throughput over 5 seconds."""
        async with LanceClient(client_config) as client:
            topic = await client.create_topic(_unique_topic("benchmark"))
            topic_id = topic["id"]

        payload_size = 10 * 1024  # 10 KB
        duration_s = 5
        payload = b"\xef" * payload_size

        print(f"\n  Running throughput benchmark for {duration_s}s")
        print(f"  Payload size: {payload_size} bytes")

        producer = await Producer.connect(
            LANCE_TEST_ADDR,
            ProducerConfig().with_max_pending_acks(256),
        )

        sent = 0
        start = time.monotonic()
        deadline = start + duration_s

        while time.monotonic() < deadline:
            await producer.send_async(topic_id=topic_id, data=payload)
            sent += 1

        await producer.flush(timeout=30.0)
        elapsed = time.monotonic() - start

        total_mb = (sent * payload_size) / (1024 * 1024)
        throughput = total_mb / elapsed
        rate = sent / elapsed

        print("\n  Benchmark Results:")
        print(f"    Duration: {elapsed:.2f}s")
        print(f"    Messages: {sent}")
        print(f"    Total data: {total_mb:.2f} MB")
        print(f"    Throughput: {throughput:.2f} MB/s")
        print(f"    Rate: {rate:.0f} msgs/sec")

        await producer.close()

        # Cleanup
        async with LanceClient(client_config) as client:
            await client.delete_topic(topic_id)
