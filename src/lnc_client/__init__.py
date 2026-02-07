"""lnc-client — Python client for the Lance Wire Protocol (LWP).

High-performance, low-latency data streaming client implementing the LWP
binary protocol with CRC32C validation, TLV record encoding, batched
production, and offset-based consumption.

Example usage::

    import asyncio
    from lnc_client import LanceClient, ClientConfig, Producer, ProducerConfig
    from lnc_client import StandaloneConsumer, StandaloneConfig

    async def main():
        # Management client
        cfg = ClientConfig(host="127.0.0.1", port=1992)
        async with LanceClient(cfg) as client:
            topics = await client.list_topics()
            print(topics)

        # Producer
        prod = await Producer.connect("127.0.0.1:1992", ProducerConfig())
        await prod.send(topic_id=1, data=b'hello world')
        await prod.close()

        # Consumer
        cons = await StandaloneConsumer.connect(
            "127.0.0.1:1992",
            StandaloneConfig(consumer_name="my-consumer", topic_id=1),
        )
        records = await cons.poll()
        await cons.close()

    asyncio.run(main())
"""

from lnc_client.protocol import (
    MAGIC,
    HEADER_SIZE,
    PROTOCOL_VERSION,
    DEFAULT_PORT,
    Flag,
    ControlCommand,
    LwpHeader,
)
from lnc_client.tlv import RecordType, TlvRecord, encode_records, decode_records
from lnc_client.errors import (
    LanceError,
    ConnectionError,
    ProtocolError,
    TimeoutError,
    BackpressureError,
    TopicNotFoundError,
    InvalidFrameError,
)
from lnc_client.config import ClientConfig, ProducerConfig, StandaloneConfig
from lnc_client.client import LanceClient
from lnc_client.producer import Producer
from lnc_client.consumer import StandaloneConsumer, PollResult

__version__ = "0.1.0"

__all__ = [
    # Protocol
    "MAGIC",
    "HEADER_SIZE",
    "PROTOCOL_VERSION",
    "DEFAULT_PORT",
    "Flag",
    "ControlCommand",
    "LwpHeader",
    # TLV
    "RecordType",
    "TlvRecord",
    "encode_records",
    "decode_records",
    # Errors
    "LanceError",
    "ConnectionError",
    "ProtocolError",
    "TimeoutError",
    "BackpressureError",
    "TopicNotFoundError",
    "InvalidFrameError",
    # Config
    "ClientConfig",
    "ProducerConfig",
    "StandaloneConfig",
    # Client
    "LanceClient",
    # Producer
    "Producer",
    # Consumer
    "StandaloneConsumer",
    "PollResult",
]
