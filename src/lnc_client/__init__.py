"""lnc-client — Python client for the Lance Wire Protocol (LWP).

High-performance, low-latency data streaming client implementing the LWP
binary protocol with CRC32C validation, TLV record encoding, batched
production, and offset-based consumption.

Example usage::

    import asyncio
    from lnc_client import LanceClient, ClientConfig, Producer, ProducerConfig
    from lnc_client import StandaloneConsumer, StandaloneConfig, SeekPosition

    async def main():
        # Management client
        cfg = ClientConfig(host="127.0.0.1", port=1992)
        async with LanceClient(cfg) as client:
            topic = await client.create_topic("my-events")
            topics = await client.list_topics()
            print(topics)

        # Producer
        prod = await Producer.connect("127.0.0.1:1992", ProducerConfig())
        await prod.send(topic_id=1, data=b'hello world')
        await prod.flush()
        await prod.close()

        # Consumer
        cons = await StandaloneConsumer.connect(
            "127.0.0.1:1992",
            StandaloneConfig(consumer_name="my-consumer", topic_id=1),
        )
        records = await cons.poll()
        await cons.commit()
        await cons.close()

    asyncio.run(main())
"""

from lnc_client.client import LanceClient
from lnc_client.config import (
    ClientConfig,
    ProducerConfig,
    ReconnectConfig,
    SeekPosition,
    StandaloneConfig,
)
from lnc_client.consumer import PollResult, StandaloneConsumer
from lnc_client.errors import (
    AccessDeniedError,
    BackpressureError,
    ConnectionError,
    InvalidFrameError,
    LanceError,
    NotLeaderError,
    ProtocolError,
    ServerCatchingUpError,
    TimeoutError,
    TopicAlreadyExistsError,
    TopicNotFoundError,
)
from lnc_client.offset import FileOffsetStore, MemoryOffsetStore, OffsetStore
from lnc_client.producer import Producer
from lnc_client.protocol import (
    DEFAULT_PORT,
    HEADER_SIZE,
    MAGIC,
    PROTOCOL_VERSION,
    ControlCommand,
    Flag,
    LwpHeader,
)
from lnc_client.tlv import RecordType, TlvRecord, decode_records, encode_records

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
    "TopicAlreadyExistsError",
    "NotLeaderError",
    "ServerCatchingUpError",
    "AccessDeniedError",
    "InvalidFrameError",
    # Config
    "ClientConfig",
    "ProducerConfig",
    "StandaloneConfig",
    "ReconnectConfig",
    "SeekPosition",
    # Client
    "LanceClient",
    # Producer
    "Producer",
    # Consumer
    "StandaloneConsumer",
    "PollResult",
    # Offset persistence
    "OffsetStore",
    "FileOffsetStore",
    "MemoryOffsetStore",
]
