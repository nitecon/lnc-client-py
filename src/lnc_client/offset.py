"""Offset persistence backends for consumer offset tracking.

Mirrors Rust ``lnc_client::offset`` module — provides MemoryOffsetStore
and FileOffsetStore for durable offset checkpointing.

Example::

    from lnc_client.offset import FileOffsetStore

    store = FileOffsetStore("/var/lib/lance/offsets")
    await store.save("my-consumer", topic_id=1, offset=42000)
    offset = await store.load("my-consumer", topic_id=1)
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path

log = logging.getLogger("lnc_client.offset")


class OffsetStore(ABC):
    """Abstract base for offset persistence backends."""

    @abstractmethod
    async def load(self, consumer_name: str, topic_id: int) -> int | None:
        """Load the last committed offset. Returns None if not found."""

    @abstractmethod
    async def save(self, consumer_name: str, topic_id: int, offset: int) -> None:
        """Persist the current offset."""

    @abstractmethod
    async def delete(self, consumer_name: str, topic_id: int) -> None:
        """Remove a stored offset."""


class MemoryOffsetStore(OffsetStore):
    """In-memory offset store — offsets are lost on process exit."""

    def __init__(self) -> None:
        self._offsets: dict[tuple[str, int], int] = {}

    async def load(self, consumer_name: str, topic_id: int) -> int | None:
        return self._offsets.get((consumer_name, topic_id))

    async def save(self, consumer_name: str, topic_id: int, offset: int) -> None:
        self._offsets[(consumer_name, topic_id)] = offset

    async def delete(self, consumer_name: str, topic_id: int) -> None:
        self._offsets.pop((consumer_name, topic_id), None)


class FileOffsetStore(OffsetStore):
    """File-based offset store — persists offsets as JSON files.

    Mirrors Rust ``LockFileOffsetStore``. Each consumer/topic pair gets a
    file at ``{base_dir}/{consumer_name}_{topic_id}.offset``.
    """

    def __init__(self, base_dir: str | Path) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, consumer_name: str, topic_id: int) -> Path:
        safe_name = consumer_name.replace("/", "_").replace("\\", "_")
        return self._base_dir / f"{safe_name}_{topic_id}.offset"

    async def load(self, consumer_name: str, topic_id: int) -> int | None:
        path = self._path(consumer_name, topic_id)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
            return data.get("offset")
        except (json.JSONDecodeError, OSError) as e:
            log.warning("Failed to load offset from %s: %s", path, e)
            return None

    async def save(self, consumer_name: str, topic_id: int, offset: int) -> None:
        path = self._path(consumer_name, topic_id)
        tmp = path.with_suffix(".tmp")
        try:
            tmp.write_text(
                json.dumps({"consumer": consumer_name, "topic_id": topic_id, "offset": offset})
            )
            tmp.replace(path)
        except OSError as e:
            log.error("Failed to save offset to %s: %s", path, e)
            raise

    async def delete(self, consumer_name: str, topic_id: int) -> None:
        path = self._path(consumer_name, topic_id)
        try:
            path.unlink(missing_ok=True)
        except OSError as e:
            log.warning("Failed to delete offset file %s: %s", path, e)
