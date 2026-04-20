"""
Tests for producer/config.py.

Config reads os.getenv at class-body (import) time, so we use
importlib.reload() after monkeypatching env vars to force re-evaluation.
"""
import importlib

import pytest


def _reload_config(monkeypatch, **env_overrides):
    """Helper: patch env vars then reload config module. Returns reloaded module."""
    for key, value in env_overrides.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)
    import config as cfg
    importlib.reload(cfg)
    return cfg


@pytest.mark.unit
class TestConfigDefaults:
    def test_default_bootstrap_servers(self, monkeypatch):
        cfg = _reload_config(monkeypatch, KAFKA_BOOTSTRAP_SERVERS=None)
        assert cfg.Config.bootstrap_servers == "kafka:9092"

    def test_default_topic(self, monkeypatch):
        cfg = _reload_config(monkeypatch, KAFKA_TOPIC=None)
        assert cfg.Config.topic == "web-logs"

    def test_default_messages_per_second(self, monkeypatch):
        cfg = _reload_config(monkeypatch, MESSAGES_PER_SECOND=None)
        assert cfg.Config.messages_per_second == 10.0

    def test_default_client_id(self, monkeypatch):
        cfg = _reload_config(monkeypatch, PRODUCER_CLIENT_ID=None)
        assert cfg.Config.client_id == "web-log-producer"


@pytest.mark.unit
class TestConfigOverrides:
    def test_bootstrap_servers_override(self, monkeypatch):
        cfg = _reload_config(monkeypatch, KAFKA_BOOTSTRAP_SERVERS="broker1:9092,broker2:9092")
        assert cfg.Config.bootstrap_servers == "broker1:9092,broker2:9092"

    def test_topic_override(self, monkeypatch):
        cfg = _reload_config(monkeypatch, KAFKA_TOPIC="custom-topic")
        assert cfg.Config.topic == "custom-topic"

    def test_messages_per_second_float_string(self, monkeypatch):
        cfg = _reload_config(monkeypatch, MESSAGES_PER_SECOND="2.5")
        assert cfg.Config.messages_per_second == 2.5

    def test_messages_per_second_integer_string(self, monkeypatch):
        cfg = _reload_config(monkeypatch, MESSAGES_PER_SECOND="5")
        assert cfg.Config.messages_per_second == 5.0

    def test_messages_per_second_is_float_type(self, monkeypatch):
        cfg = _reload_config(monkeypatch, MESSAGES_PER_SECOND="10")
        assert isinstance(cfg.Config.messages_per_second, float)

    def test_client_id_override(self, monkeypatch):
        cfg = _reload_config(monkeypatch, PRODUCER_CLIENT_ID="my-producer")
        assert cfg.Config.client_id == "my-producer"
