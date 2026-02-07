# lnc-client

[![PyPI version](https://img.shields.io/pypi/v/lnc-client.svg)](https://pypi.org/project/lnc-client/)
[![Python](https://img.shields.io/pypi/pyversions/lnc-client.svg)](https://pypi.org/project/lnc-client/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Python client for the **Lance Wire Protocol (LWP)** — high-performance, low-latency data streaming.

[Lance](https://github.com/nitecon/lance) is an io\_uring-based streaming server designed to saturate 100G NICs with minimal latency. This client implements the full LWP binary protocol for Python applications.

## Installation

```bash
pip install lnc-client
```

**Requirements:** Python 3.10+

## Quick Start

### Producer

```python
import asyncio
from lnc_client import Producer, ProducerConfig

async def main():
    producer = await Producer.connect("localhost:1992", ProducerConfig())

    # Send with ACK (guaranteed delivery)
    batch_id = await producer.send(topic_id=1, data=b'{"price": 6942.25}')

    # Send without waiting for ACK (pipelined, higher throughput)
    batch_id = await producer.send_async(topic_id=1, data=b'fire and forget')

    await producer.close()

asyncio.run(main())
```

### Consumer

```python
import asyncio
from lnc_client import StandaloneConsumer, StandaloneConfig

async def main():
    consumer = await StandaloneConsumer.connect(
        "localhost:1992",
        StandaloneConfig(consumer_name="my-app", topic_id=1),
    )

    while True:
        result = await consumer.poll()
        if result is None:
            await asyncio.sleep(0.05)
            continue

        for record in result.records:
            print(f"Type={record.record_type}, Data={record.value}")

        print(f"Lag: {result.lag} bytes")

asyncio.run(main())
```

### Topic Management

```python
import asyncio
from lnc_client import LanceClient, ClientConfig

async def main():
    async with LanceClient(ClientConfig(host="localhost")) as client:
        topics = await client.list_topics()

        topic = await client.create_topic("my-events")

        # Create with retention (7-day, 1GB max)
        topic = await client.create_topic_with_retention(
            "logs", max_age_secs=7*86400, max_bytes=1024**3
        )

        await client.set_retention(topic["id"], max_age_secs=86400)

asyncio.run(main())
```

## Configuration

### ProducerConfig

| Option | Default | Description |
|--------|---------|-------------|
| `batch_size` | `32768` | Max batch size in bytes |
| `linger_ms` | `5` | Max wait before sending partial batch |
| `compression` | `False` | Enable LZ4 compression |
| `max_pending_acks` | `64` | Max unacknowledged batches |

### StandaloneConfig

| Option | Default | Description |
|--------|---------|-------------|
| `consumer_name` | `""` | Consumer identifier |
| `topic_id` | `0` | Topic to consume from |
| `max_fetch_bytes` | `65536` | Max bytes per fetch |
| `start_offset` | `0` | Initial byte offset |
| `poll_interval_ms` | `50` | Polling interval when idle |

### ClientConfig

| Option | Default | Description |
|--------|---------|-------------|
| `host` | `"localhost"` | Lance server hostname |
| `port` | `1992` | Lance server port |

## TLV Record Types

Records use Type-Length-Value encoding:

| Type | Code | Description |
|------|------|-------------|
| RawData | `0x01` | Unstructured binary |
| JSON | `0x02` | JSON-encoded record |
| MessagePack | `0x03` | MessagePack-encoded |
| KeyValue | `0x10` | Key-value pair |
| Timestamped | `0x11` | Timestamp + data |
| Null | `0xFF` | Tombstone/empty |

## Error Handling

```python
from lnc_client import (
    LanceError,
    ConnectionError,
    BackpressureError,
    TopicNotFoundError,
    InvalidFrameError,
)

try:
    await producer.send(topic_id=99, data=b"test")
except TopicNotFoundError:
    print("Topic doesn't exist")
except BackpressureError:
    print("Server is overloaded, slow down")
except ConnectionError:
    print("Connection lost")
```

## Protocol Details

This client implements the [Lance Wire Protocol (LWP)](https://github.com/nitecon/lance) v1.0:

- **44-byte fixed header** with CRC32C validation
- **Hardware-accelerated checksums** (SSE4.2 / ARM CRC)
- **Backpressure signaling** from server
- **Keepalive** with 30-second timeout
- **Batched production** with ACK tracking
- **Offset-based consumption** with seek/rewind
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

39 tests covering protocol parsing, CRC32C validation, TLV encoding/decoding, and frame builders.

## License

[MIT](LICENSE)
