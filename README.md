# lnc-client

Python client for the **Lance Wire Protocol (LWP)** — high-performance, low-latency data streaming.

Lance is an io_uring-based streaming server designed to saturate 100G NICs with minimal latency. This client implements the full LWP binary protocol for Python applications.

## Prerequisites

- **Python 3.10+** (tested on 3.12)
- **Lance server** accessible (default: `10.0.10.11:1992`)
- **WSL2** (if developing on Windows)

## Setup

### Using setup script

```bash
cd ml/lnc-client-py
chmod +x setup_venv.sh
./setup_venv.sh
source .venv/bin/activate
```

### Manual setup

```bash
cd ml/lnc-client-py
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Verify installation

```bash
pytest -v
```

All 39 tests should pass.

## Installation (as dependency)

```bash
# From local source (for rithmic-ml)
pip install -e /path/to/ml/lnc-client-py

# Future: from PyPI
pip install lnc-client
```

## Quick Start

### Producer

```python
import asyncio
from lnc_client import Producer, ProducerConfig

async def main():
    producer = await Producer.connect("10.0.10.11:1992", ProducerConfig())

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
        "10.0.10.11:1992",
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
    async with LanceClient(ClientConfig(host="10.0.10.11")) as client:
        # List topics
        topics = await client.list_topics()

        # Create topic
        topic = await client.create_topic("my-events")

        # Create with retention (7-day, 1GB max)
        topic = await client.create_topic_with_retention(
            "logs", max_age_secs=7*86400, max_bytes=1024**3
        )

        # Set retention on existing topic
        await client.set_retention(topic["id"], max_age_secs=86400)

asyncio.run(main())
```

## Configuration

### ProducerConfig

| Option | Default | Description |
|--------|---------|-------------|
| `batch_size` | 32768 | Max batch size in bytes |
| `linger_ms` | 5 | Max wait before sending partial batch |
| `compression` | False | Enable LZ4 compression |
| `max_pending_acks` | 64 | Max unacknowledged batches |

### StandaloneConfig

| Option | Default | Description |
|--------|---------|-------------|
| `consumer_name` | "" | Consumer identifier |
| `topic_id` | 0 | Topic to consume from |
| `max_fetch_bytes` | 65536 | Max bytes per fetch |
| `start_offset` | 0 | Initial byte offset |
| `poll_interval_ms` | 50 | Polling interval when idle |

## TLV Record Types

Records use Type-Length-Value encoding:

| Type | Code | Description |
|------|------|-------------|
| RawData | 0x01 | Unstructured binary |
| JSON | 0x02 | JSON-encoded record |
| MessagePack | 0x03 | MessagePack-encoded |
| KeyValue | 0x10 | Key-value pair |
| Timestamped | 0x11 | Timestamp + data |
| Null | 0xFF | Tombstone/empty |

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

## Protocol

This client implements the Lance Wire Protocol (LWP) v1.0:

- **44-byte fixed header** with CRC32C validation
- **Hardware-accelerated checksums** (SSE4.2/ARM CRC)
- **Backpressure signaling** from server
- **Keepalive** with 30-second timeout
- **Batched production** with ACK tracking
- **Offset-based consumption** with seek/rewind

### Lance Server Endpoints

| Environment | Address |
|-------------|---------|
| K8s internal | `lance.lance.svc.cluster.local:1992` |
| External | `10.0.10.11:1992` |

### Topic Names

| Environment | Data Topic | Actions Topic |
|-------------|-----------|---------------|
| Dev (UAT) | `rithmic-dev` | `rithmic-dev-actions` |
| Paper | `rithmic-paper` | `rithmic-paper-actions` |
| Prod | `rithmic-prod` | `rithmic-prod-actions` |

## Project Structure

```
ml/lnc-client-py/
├── src/lnc_client/
│   ├── __init__.py      # Public API exports
│   ├── protocol.py      # LWP 44-byte header, CRC32C, frame builders
│   ├── tlv.py           # Type-Length-Value record encoding/decoding
│   ├── config.py        # Client, producer, consumer configurations
│   ├── connection.py    # Async TCP with keepalive, backpressure, reconnect
│   ├── client.py        # Topic management (create, list, delete, retention)
│   ├── producer.py      # Batched producer with ACK tracking, LZ4 compression
│   ├── consumer.py      # Standalone consumer with offset-based seek/rewind
│   └── errors.py        # Custom exceptions + error code mapping
├── tests/
│   ├── test_protocol.py # LWP header, CRC32C, frame builder tests
│   └── test_tlv.py      # TLV encoding/decoding tests
├── setup_venv.sh        # Venv setup automation
└── pyproject.toml       # Package definition + dependencies
```

## Tests

```bash
# Run all tests (39 tests)
pytest -v

# Protocol tests only
pytest tests/test_protocol.py -v

# TLV tests only
pytest tests/test_tlv.py -v
```

## License

MIT
