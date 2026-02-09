"""Tests for configuration classes, builder methods, and SeekPosition."""

from pathlib import Path

from lnc_client.config import (
    ClientConfig,
    ProducerConfig,
    ReconnectConfig,
    SeekPosition,
    StandaloneConfig,
)


class TestSeekPosition:
    """Test SeekPosition enum and offset helper."""

    def test_beginning(self):
        assert SeekPosition.BEGINNING.value == "beginning"

    def test_end(self):
        assert SeekPosition.END.value == "end"

    def test_offset(self):
        pos = SeekPosition.offset(42000)
        assert pos == ("offset", 42000)


class TestClientConfig:
    """Test ClientConfig defaults and builder methods."""

    def test_defaults(self):
        cfg = ClientConfig()
        assert cfg.host == "127.0.0.1"
        assert cfg.port == 1992
        assert cfg.connect_timeout_s == 10.0
        assert cfg.request_timeout_s == 30.0
        assert cfg.ssl_context is None

    def test_address_property(self):
        cfg = ClientConfig(host="10.0.0.1", port=2000)
        assert cfg.address == "10.0.0.1:2000"

    def test_builder_chain(self):
        cfg = ClientConfig().with_host("10.0.0.5").with_port(3000).with_connect_timeout(5.0)
        assert cfg.host == "10.0.0.5"
        assert cfg.port == 3000
        assert cfg.connect_timeout_s == 5.0


class TestProducerConfig:
    """Test ProducerConfig defaults and builder methods."""

    def test_defaults(self):
        cfg = ProducerConfig()
        assert cfg.batch_size == 32 * 1024
        assert cfg.linger_ms == 5
        assert cfg.compression is False
        assert cfg.max_pending_acks == 64
        assert cfg.auto_reconnect is True

    def test_builder_chain(self):
        cfg = (
            ProducerConfig()
            .with_batch_size(64 * 1024)
            .with_linger_ms(10)
            .with_compression(True)
            .with_max_pending_acks(128)
            .with_connect_timeout(5.0)
            .with_request_timeout(15.0)
            .with_auto_reconnect(False)
        )
        assert cfg.batch_size == 64 * 1024
        assert cfg.linger_ms == 10
        assert cfg.compression is True
        assert cfg.max_pending_acks == 128
        assert cfg.connect_timeout_s == 5.0
        assert cfg.request_timeout_s == 15.0
        assert cfg.auto_reconnect is False


class TestStandaloneConfig:
    """Test StandaloneConfig defaults, builder methods, and SeekPosition integration."""

    def test_defaults(self):
        cfg = StandaloneConfig("my-consumer", topic_id=1)
        assert cfg.consumer_name == "my-consumer"
        assert cfg.topic_id == 1
        assert cfg.max_fetch_bytes == 1_048_576
        assert cfg.start_position is SeekPosition.BEGINNING
        assert cfg.offset_dir is None
        assert cfg.auto_commit_interval_s == 5.0
        assert cfg.auto_reconnect is True

    def test_start_offset_from_beginning(self):
        cfg = StandaloneConfig("c", topic_id=1)
        assert cfg.start_offset == 0

    def test_start_offset_from_end(self):
        cfg = StandaloneConfig("c", topic_id=1, start_position=SeekPosition.END)
        assert cfg.start_offset == 2**63 - 1

    def test_start_offset_from_explicit(self):
        cfg = StandaloneConfig("c", topic_id=1, start_position=SeekPosition.offset(5000))
        assert cfg.start_offset == 5000

    def test_backward_compat_start_offset_kwarg(self):
        cfg = StandaloneConfig("c", topic_id=1, start_offset=9999)
        assert cfg.start_offset == 9999

    def test_builder_chain(self):
        cfg = (
            StandaloneConfig("c", topic_id=1)
            .with_max_fetch_bytes(512 * 1024)
            .with_start_position(SeekPosition.END)
            .with_offset_dir("/tmp/offsets")
            .with_auto_commit_interval(10.0)
            .with_poll_timeout(1.0)
            .with_connect_timeout(3.0)
            .with_auto_reconnect(False)
        )
        assert cfg.max_fetch_bytes == 512 * 1024
        assert cfg.start_position is SeekPosition.END
        assert cfg.offset_dir == Path("/tmp/offsets")
        assert cfg.auto_commit_interval_s == 10.0
        assert cfg.poll_timeout_s == 1.0
        assert cfg.connect_timeout_s == 3.0
        assert cfg.auto_reconnect is False

    def test_manual_commit(self):
        cfg = StandaloneConfig("c", topic_id=1).with_manual_commit()
        assert cfg.auto_commit_interval_s is None


class TestReconnectConfig:
    """Test ReconnectConfig defaults and backoff calculation."""

    def test_defaults(self):
        cfg = ReconnectConfig()
        assert cfg.base_delay_ms == 100
        assert cfg.max_delay_ms == 30_000
        assert cfg.max_attempts == 0
        assert cfg.jitter_factor == 0.1

    def test_delay_exponential(self):
        cfg = ReconnectConfig(jitter_factor=0.0)
        # Attempt 0 => 100ms = 0.1s
        assert cfg.delay_for_attempt(0) == 0.1
        # Attempt 1 => 200ms = 0.2s
        assert cfg.delay_for_attempt(1) == 0.2
        # Attempt 2 => 400ms = 0.4s
        assert cfg.delay_for_attempt(2) == 0.4

    def test_delay_capped(self):
        cfg = ReconnectConfig(base_delay_ms=100, max_delay_ms=500, jitter_factor=0.0)
        # Attempt 10 => min(100 * 2^10, 500) = 500ms = 0.5s
        assert cfg.delay_for_attempt(10) == 0.5
