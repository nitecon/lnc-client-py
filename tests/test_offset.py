"""Tests for offset persistence backends."""

import json

import pytest

from lnc_client.offset import FileOffsetStore, MemoryOffsetStore


class TestMemoryOffsetStore:
    """Test in-memory offset store."""

    @pytest.fixture
    def store(self):
        return MemoryOffsetStore()

    async def test_load_missing(self, store):
        assert await store.load("c1", 1) is None

    async def test_save_and_load(self, store):
        await store.save("c1", 1, 42000)
        assert await store.load("c1", 1) == 42000

    async def test_update(self, store):
        await store.save("c1", 1, 100)
        await store.save("c1", 1, 200)
        assert await store.load("c1", 1) == 200

    async def test_delete(self, store):
        await store.save("c1", 1, 100)
        await store.delete("c1", 1)
        assert await store.load("c1", 1) is None

    async def test_multiple_consumers(self, store):
        await store.save("c1", 1, 100)
        await store.save("c2", 1, 200)
        await store.save("c1", 2, 300)
        assert await store.load("c1", 1) == 100
        assert await store.load("c2", 1) == 200
        assert await store.load("c1", 2) == 300

    async def test_delete_nonexistent(self, store):
        # Should not raise
        await store.delete("c1", 99)


class TestFileOffsetStore:
    """Test file-based offset store."""

    @pytest.fixture
    def store(self, tmp_path):
        return FileOffsetStore(tmp_path)

    async def test_load_missing(self, store):
        assert await store.load("c1", 1) is None

    async def test_save_and_load(self, store):
        await store.save("c1", 1, 42000)
        assert await store.load("c1", 1) == 42000

    async def test_update(self, store):
        await store.save("c1", 1, 100)
        await store.save("c1", 1, 200)
        assert await store.load("c1", 1) == 200

    async def test_delete(self, store):
        await store.save("c1", 1, 100)
        await store.delete("c1", 1)
        assert await store.load("c1", 1) is None

    async def test_file_created(self, store, tmp_path):
        await store.save("my-app", 3, 5000)
        path = tmp_path / "my-app_3.offset"
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["consumer"] == "my-app"
        assert data["topic_id"] == 3
        assert data["offset"] == 5000

    async def test_creates_base_dir(self, tmp_path):
        base = tmp_path / "nested" / "dir"
        store = FileOffsetStore(base)
        await store.save("c1", 1, 100)
        assert await store.load("c1", 1) == 100

    async def test_corrupted_file_returns_none(self, store, tmp_path):
        path = tmp_path / "c1_1.offset"
        path.write_text("not json!!!")
        assert await store.load("c1", 1) is None

    async def test_safe_filename(self, store, tmp_path):
        # Slashes in consumer name are replaced with underscores
        await store.save("group/worker-1", 1, 100)
        path = tmp_path / "group_worker-1_1.offset"
        assert path.exists()
        assert await store.load("group/worker-1", 1) == 100


class TestMemoryOffsetStoreTopicName:
    """Test MemoryOffsetStore with topic_name support."""

    @pytest.fixture
    def store(self):
        return MemoryOffsetStore()

    async def test_save_load_by_name(self, store):
        await store.save("c1", 0, 42000, topic_name="events")
        assert await store.load("c1", 0, topic_name="events") == 42000

    async def test_name_vs_id_isolation(self, store):
        await store.save("c1", 1, 100)  # by id
        await store.save("c1", 0, 200, topic_name="events")  # by name
        assert await store.load("c1", 1) == 100
        assert await store.load("c1", 0, topic_name="events") == 200

    async def test_delete_by_name(self, store):
        await store.save("c1", 0, 100, topic_name="events")
        await store.delete("c1", 0, topic_name="events")
        assert await store.load("c1", 0, topic_name="events") is None


class TestFileOffsetStoreTopicName:
    """Test FileOffsetStore with topic_name support."""

    @pytest.fixture
    def store(self, tmp_path):
        return FileOffsetStore(tmp_path)

    async def test_name_based_filename(self, store, tmp_path):
        await store.save("my-app", 0, 5000, topic_name="events")
        path = tmp_path / "my-app_events.offset"
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["consumer"] == "my-app"
        assert data["topic_name"] == "events"
        assert data["offset"] == 5000

    async def test_id_based_filename_when_no_name(self, store, tmp_path):
        await store.save("my-app", 3, 5000)
        path = tmp_path / "my-app_3.offset"
        assert path.exists()

    async def test_load_by_name(self, store):
        await store.save("c1", 0, 42000, topic_name="events")
        assert await store.load("c1", 0, topic_name="events") == 42000

    async def test_delete_by_name(self, store, tmp_path):
        await store.save("c1", 0, 100, topic_name="events")
        await store.delete("c1", 0, topic_name="events")
        assert not (tmp_path / "c1_events.offset").exists()
