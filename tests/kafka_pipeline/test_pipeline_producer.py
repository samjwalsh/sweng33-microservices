import pytest
from unittest.mock import Mock

from kafka_pipeline import pipeline_producer as pp


# -------------------------
# helpers (dummy kafka)
# -------------------------

class DummyFuture:
    def __init__(self, metadata):
        self._metadata = metadata
        self.timeout = None

    def get(self, timeout=None):
        self.timeout = timeout
        return self._metadata


class DummyMetadata:
    def __init__(self):
        self.topic = "test-topic"
        self.partition = 1
        self.offset = 42


class DummyProducer:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.sent = []
        self.flushed = False
        self.closed = False
        self.future = DummyFuture(DummyMetadata())

    def send(self, topic, key=None, value=None):
        self.sent.append({"topic": topic, "key": key, "value": value})
        return self.future

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


# -------------------------
# resolve_bootstrap_server
# -------------------------

def test_resolve_bootstrap_server_prefers_explicit(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVER", "env1")
    monkeypatch.setenv("KAFKA_HOST", "env2")

    assert pp.resolve_bootstrap_server("explicit") == "explicit"


def test_resolve_bootstrap_server_env_bootstrap(monkeypatch):
    monkeypatch.delenv("KAFKA_HOST", raising=False)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVER", "env1")

    assert pp.resolve_bootstrap_server(None) == "env1"


def test_resolve_bootstrap_server_env_host(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVER", raising=False)
    monkeypatch.setenv("KAFKA_HOST", "env2")

    assert pp.resolve_bootstrap_server(None) == "env2"


# -------------------------
# build_producer
# -------------------------

def test_build_producer(monkeypatch):
    captured = {}

    def fake_producer(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return "producer"

    monkeypatch.setattr(pp, "KafkaProducer", fake_producer)

    producer = pp.build_producer("broker:9092")

    assert producer == "producer"
    assert captured["kwargs"]["bootstrap_servers"] == ["broker:9092"]

    # test serializers exist
    assert callable(captured["kwargs"]["value_serializer"])
    assert callable(captured["kwargs"]["key_serializer"])


# -------------------------
# send_kafka_message
# -------------------------

def test_send_kafka_message_success(monkeypatch):
    producer = DummyProducer()

    monkeypatch.setattr(pp, "build_producer", lambda bootstrap_server=None: producer)

    result = pp.send_kafka_message(
        topic="topic",
        key="key",
        value={"x": 1},
        bootstrap_server="broker",
        timeout_seconds=5,
    )

    # verify send
    assert producer.sent == [
        {"topic": "topic", "key": "key", "value": {"x": 1}}
    ]

    # verify metadata
    assert result == {
        "topic": "test-topic",
        "partition": 1,
        "offset": 42,
        "key": "key",
    }

    # verify timeout passed
    assert producer.future.timeout == 5

    # verify cleanup
    assert producer.flushed is True
    assert producer.closed is True


def test_send_kafka_message_closes_on_error(monkeypatch):
    class FailingProducer(DummyProducer):
        def send(self, *args, **kwargs):
            raise RuntimeError("send failed")

    producer = FailingProducer()

    monkeypatch.setattr(pp, "build_producer", lambda bootstrap_server=None: producer)

    with pytest.raises(RuntimeError):
        pp.send_kafka_message(
            topic="topic",
            value={"x": 1},
        )

    # still must close
    assert producer.closed is True


def test_send_kafka_message_without_key(monkeypatch):
    producer = DummyProducer()

    monkeypatch.setattr(pp, "build_producer", lambda bootstrap_server=None: producer)

    result = pp.send_kafka_message(
        topic="topic",
        value={"x": 1},
        key=None,
    )

    assert producer.sent[0]["key"] is None
    assert result["key"] is None