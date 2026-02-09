# lnc-client

[![PyPI version](https://img.shields.io/pypi/v/lnc-client.svg)](https://pypi.org/project/lnc-client/)
[![Python](https://img.shields.io/pypi/pyversions/lnc-client.svg)](https://pypi.org/project/lnc-client/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Python client for the **Lance Wire Protocol (LWP)** — high-performance, low-latency data streaming.

[Lance](https://github.com/nitecon/lance) is an io\_uring-based streaming server designed to saturate 100G NICs with minimal latency. This client implements the full LWP binary protocol for Python applications with API semantics matching the official [Rust client](https://docs.rs/lnc-client).

## Installation

```bash
pip install lnc-client
```

**Requirements:** Python 3.10+

## Topic Management

Topics are created by name and referenced by numeric ID in all subsequent operations:

```python
import asyncio
from lnc_client import LanceClient, ClientConfig

async def main():
    async with LanceClient(ClientConfig(host="127.0.0.1")) as client:
        # Create a topic — returns {"id": 1, "name": "my-events", ...}
        topic = await client.create_topic("my-events")
        topic_id = topic["id"]

        # Create with retention policy (7-day TTL, 1 GB max)
        topic = await client.create_topic_with_retention(
            "logs", max_age_secs=7 * 86400, max_bytes=1024**3,
        )

        # List all topics
        topics = await client.list_topics()

        # Get topic metadata by ID
        info = await client.get_topic(topic_id)

        # Update retention policy
        await client.set_retention(topic_id, max_age_secs=86400)

        # Delete a topic
        await client.delete_topic(topic_id)

        # Latency measurement
        rtt = await client.ping()
        print(f"Round-trip: {rtt * 1000:.2f}ms")

asyncio.run(main())
```

## Producer

```python
import asyncio
from lnc_client import Producer, ProducerConfig

async def main():
    config = (
        ProducerConfig()
        .with_batch_size(64 * 1024)
        .with_linger_ms(10)
        .with_compression(True)
    )
    producer = await Producer.connect("127.0.0.1:1992", config)

    # Send with ACK (guaranteed delivery) — returns batch_id
    batch_id = await producer.send(topic_id=1, data=b'{"price": 6942.25}')

    # Send without waiting for ACK (pipelined, higher throughput)
    batch_id = await producer.send_async(topic_id=1, data=b"fire and forget")

    # Send multiple records as a single batch
    from lnc_client import TlvRecord
    records = [TlvRecord.json(b'{"a":1}'), TlvRecord.json(b'{"b":2}')]
    await producer.send_batch(topic_id=1, records=records)

    # Wait for all pending ACKs
    await producer.flush()
    await producer.close()

asyncio.run(main())
```

## Consumer (Standalone)

For independent consumption with client-managed offsets:

```python
import asyncio
from lnc_client import StandaloneConsumer, StandaloneConfig, SeekPosition
from pathlib import Path

async def main():
    config = (
        StandaloneConfig("my-consumer", topic_id=1)
        .with_start_position(SeekPosition.BEGINNING)
        .with_offset_dir(Path("/var/lib/lance/offsets"))
        .with_max_fetch_bytes(1_048_576)
    )
    consumer = await StandaloneConsumer.connect("127.0.0.1:1992", config)

    while True:
        result = await consumer.poll()
        if result is None:
            await asyncio.sleep(0.05)
            continue

        for record in result.records:
            print(f"Type={record.record_type}, Data={record.value}")

        print(f"Offset: {result.end_offset}, Lag: {result.lag} bytes")
        await consumer.commit()

asyncio.run(main())
```

### Seek Operations

```python
consumer.seek(42000)                       # Absolute byte offset
consumer.seek_to(SeekPosition.BEGINNING)   # Start of stream
consumer.seek_to(SeekPosition.END)         # Tail — only new data
consumer.seek_to(SeekPosition.offset(500)) # Specific offset
consumer.rewind()                          # Alias for seek(0)
```

## Low-Level Client

`LanceClient` provides direct access to all protocol operations on a single
TCP connection. `Producer` and `StandaloneConsumer` are higher-level
abstractions built on top of it.

```python
async with LanceClient(ClientConfig(host="127.0.0.1")) as client:
    # Subscribe for server-side streaming
    await client.subscribe(topic_id=1, start_offset=0, max_batch_bytes=65536, consumer_id=42)
    await client.commit_offset(topic_id=1, consumer_id=42, offset=1024)
    await client.unsubscribe(topic_id=1, consumer_id=42)
```

## Offset Persistence

Consumers can persist offsets to disk for crash recovery:

```python
from lnc_client import FileOffsetStore, MemoryOffsetStore

# File-based (survives restarts) — auto-created when offset_dir is set
config = StandaloneConfig("my-consumer", topic_id=1).with_offset_dir("/var/lib/lance/offsets")

# Or provide a store explicitly
store = FileOffsetStore("/var/lib/lance/offsets")
consumer = await StandaloneConsumer.connect("127.0.0.1:1992", config, offset_store=store)

# In-memory (testing)
store = MemoryOffsetStore()
```

## Configuration

### ProducerConfig

| Option | Default | Builder Method | Description |
|--------|---------|----------------|-------------|
| `batch_size` | `32768` | `with_batch_size()` | Max batch size in bytes |
| `linger_ms` | `5` | `with_linger_ms()` | Max wait before sending partial batch |
| `compression` | `False` | `with_compression()` | Enable LZ4 compression |
| `max_pending_acks` | `64` | `with_max_pending_acks()` | Max unacknowledged batches |
| `connect_timeout_s` | `10.0` | `with_connect_timeout()` | Connection timeout |
| `request_timeout_s` | `30.0` | `with_request_timeout()` | Per-request timeout |
| `ssl_context` | `None` | `with_ssl()` | TLS context |
| `auto_reconnect` | `True` | `with_auto_reconnect()` | Auto-reconnect on failure |

### StandaloneConfig

| Option | Default | Builder Method | Description |
|--------|---------|----------------|-------------|
| `consumer_name` | `""` | _(positional)_ | Consumer identifier |
| `topic_id` | `0` | _(positional)_ | Topic to consume from |
| `max_fetch_bytes` | `1048576` | `with_max_fetch_bytes()` | Max bytes per fetch |
| `start_position` | `BEGINNING` | `with_start_position()` | Initial seek position |
| `offset_dir` | `None` | `with_offset_dir()` | Directory for persistent offsets |
| `auto_commit_interval_s` | `5.0` | `with_auto_commit_interval()` | Auto-commit interval |
| `poll_timeout_s` | `0.1` | `with_poll_timeout()` | Poll response timeout |
| `ssl_context` | `None` | `with_ssl()` | TLS context |

### ClientConfig

| Option | Default | Builder Method | Description |
|--------|---------|----------------|-------------|
| `host` | `"127.0.0.1"` | `with_host()` | Lance server hostname |
| `port` | `1992` | `with_port()` | Lance server port |
| `connect_timeout_s` | `10.0` | `with_connect_timeout()` | Connection timeout |
| `request_timeout_s` | `30.0` | — | Per-request timeout |
| `ssl_context` | `None` | `with_ssl()` | TLS context |

## TLV Record Types

Records use Type-Length-Value encoding:

| Type | Code | Description |
|------|------|-------------|
| `RawData` | `0x01` | Unstructured binary |
| `JSON` | `0x02` | JSON-encoded record |
| `MessagePack` | `0x03` | MessagePack-encoded |
| `KeyValue` | `0x10` | Key-value pair |
| `Timestamped` | `0x11` | Timestamp + data |
| `Null` | `0xFF` | Tombstone/empty |

```python
from lnc_client import TlvRecord

rec = TlvRecord.raw(b"binary data")
rec = TlvRecord.json(b'{"key": "value"}')
rec = TlvRecord.key_value("my-key", b"my-value")
rec = TlvRecord.timestamped(1706918400_000_000_000, b"event data")
rec = TlvRecord.null()

# Accessors for structured types
key, value = rec.as_key_value()
timestamp_ns, data = rec.as_timestamped()
```

## Error Handling

All operations return typed exceptions with `is_retryable()` support:

```python
from lnc_client import (
    LanceError,
    ConnectionError,
    BackpressureError,
    TopicNotFoundError,
    NotLeaderError,
    ServerCatchingUpError,
    AccessDeniedError,
    InvalidFrameError,
)

try:
    await producer.send(topic_id=99, data=b"test")
except NotLeaderError as e:
    print(f"Redirect to leader: {e.leader_addr}")
except ServerCatchingUpError as e:
    print(f"Server at offset {e.server_offset}, backing off")
except TopicNotFoundError:
    print("Topic doesn't exist")
except BackpressureError:
    print("Server is overloaded, slow down")
except ConnectionError:
    print("Connection lost")
except LanceError as e:
    if e.is_retryable():
        print("Transient error, retrying...")
```

## Protocol Details

This client implements the [Lance Wire Protocol (LWP)](https://github.com/nitecon/lance) v1.0:

- **44-byte fixed header** with CRC32C validation
- **Hardware-accelerated checksums** (SSE4.2 / ARM CRC)
- **Backpressure signaling** from server
- **Keepalive** with 30-second timeout
- **Batched production** with ACK tracking
- **Offset-based consumption** with seek/rewind
- **CATCHING_UP protocol** — automatic 5s backoff when server is behind
- **LZ4 compression** (optional, per-batch)
- **Reconnection** with exponential backoff (100ms base, 30s max, jitter)

## Development

```bash
git clone https://github.com/nitecon/lnc-client-py.git
cd lnc-client-py
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -v
```

## License

[MIT](LICENSE)
