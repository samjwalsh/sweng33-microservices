import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from kafka_pipeline import microservice_template as svc


class DummyFuture:
    def __init__(self):
        self.timeout_received = None

    def get(self, timeout=None):
        self.timeout_received = timeout
        return None


class DummyProducer:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.sent = []
        self.flushed = False
        self.closed = False
        self.future = DummyFuture()

    def send(self, topic, key=None, value=None):
        self.sent.append({"topic": topic, "key": key, "value": value})
        return self.future

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


class DummyConsumer:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.closed = False
        self.poll_calls = 0
        self._poll_results = []

    def poll(self, timeout_ms=1000):
        self.poll_calls += 1
        if self._poll_results:
            return self._poll_results.pop(0)
        return {}

    def close(self):
        self.closed = True


@pytest.fixture
def patched_kafka(monkeypatch):
    created = {}

    def fake_consumer(*args, **kwargs):
        consumer = DummyConsumer(*args, **kwargs)
        created["consumer"] = consumer
        return consumer

    def fake_producer(*args, **kwargs):
        producer = DummyProducer(*args, **kwargs)
        created["producer"] = producer
        return producer

    monkeypatch.setattr(svc, "KafkaConsumer", fake_consumer)
    monkeypatch.setattr(svc, "KafkaProducer", fake_producer)
    return created


def test_resolve_bootstrap_server_prefers_explicit(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVER", "env-bootstrap:9092")
    monkeypatch.setenv("KAFKA_HOST", "env-host:9092")

    result = svc.resolve_bootstrap_server("explicit:9092")

    assert result == "explicit:9092"


def test_resolve_bootstrap_server_falls_back_to_kafka_bootstrap_server(monkeypatch):
    monkeypatch.delenv("KAFKA_HOST", raising=False)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVER", "env-bootstrap:9092")

    result = svc.resolve_bootstrap_server()

    assert result == "env-bootstrap:9092"


def test_resolve_bootstrap_server_falls_back_to_kafka_host(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVER", raising=False)
    monkeypatch.setenv("KAFKA_HOST", "env-host:9092")

    result = svc.resolve_bootstrap_server()

    assert result == "env-host:9092"


def test_safe_deserialize_json():
    payload = b'{"hello": "world", "n": 1}'

    result = svc.KafkaMicroservice._safe_deserialize(payload)

    assert result == {"hello": "world", "n": 1}


def test_safe_deserialize_plain_text():
    payload = b"not-json"

    result = svc.KafkaMicroservice._safe_deserialize(payload)

    assert result == "not-json"


def test_kafka_microservice_init_latest(patched_kafka):
    service = svc.KafkaMicroservice(
        service_name="test-service",
        input_topic="input-topic",
        group_id="group-1",
        bootstrap_server="broker:9092",
        from_beginning=False,
    )

    consumer = patched_kafka["consumer"]
    producer = patched_kafka["producer"]

    assert service.service_name == "test-service"
    assert service.input_topic == "input-topic"
    assert service.group_id == "group-1"
    assert service.bootstrap_server == "broker:9092"
    assert service.running is True

    assert consumer.args == ("input-topic",)
    assert consumer.kwargs["bootstrap_servers"] == ["broker:9092"]
    assert consumer.kwargs["group_id"] == "group-1"
    assert consumer.kwargs["auto_offset_reset"] == "latest"
    assert consumer.kwargs["enable_auto_commit"] is True
    assert callable(consumer.kwargs["value_deserializer"])
    assert callable(consumer.kwargs["key_deserializer"])
    assert consumer.kwargs["consumer_timeout_ms"] == 1000

    assert producer.kwargs["bootstrap_servers"] == ["broker:9092"]
    assert callable(producer.kwargs["value_serializer"])
    assert callable(producer.kwargs["key_serializer"])


def test_kafka_microservice_init_earliest(patched_kafka):
    svc.KafkaMicroservice(
        service_name="test-service",
        input_topic="input-topic",
        group_id="group-1",
        bootstrap_server="broker:9092",
        from_beginning=True,
    )

    consumer = patched_kafka["consumer"]
    assert consumer.kwargs["auto_offset_reset"] == "earliest"


def test_publish_sends_and_waits(patched_kafka):
    service = svc.KafkaMicroservice(
        service_name="test-service",
        input_topic="input-topic",
        group_id="group-1",
        bootstrap_server="broker:9092",
    )

    service.publish("output-topic", {"a": 1}, key="abc", timeout_seconds=7)

    producer = patched_kafka["producer"]
    assert producer.sent == [
        {"topic": "output-topic", "key": "abc", "value": {"a": 1}}
    ]
    assert producer.future.timeout_received == 7


def test_serialize_for_log_jsonable(patched_kafka):
    service = svc.KafkaMicroservice(
        service_name="test-service",
        input_topic="input-topic",
        group_id="group-1",
        bootstrap_server="broker:9092",
    )

    result = service._serialize_for_log({"x": 1})

    assert result == json.dumps({"x": 1}, ensure_ascii=False)


def test_serialize_for_log_non_jsonable_uses_repr(patched_kafka):
    service = svc.KafkaMicroservice(
        service_name="test-service",
        input_topic="input-topic",
        group_id="group-1",
        bootstrap_server="broker:9092",
    )

    result = service._serialize_for_log({1, 2, 3})

    assert result == repr({1, 2, 3})


def test_stop_sets_running_false(patched_kafka):
    service = svc.KafkaMicroservice(
        service_name="test-service",
        input_topic="input-topic",
        group_id="group-1",
        bootstrap_server="broker:9092",
    )

    service.stop(None, None)

    assert service.running is False


def test_example_handler_missing_src_blob_does_not_publish(capsys):
    service = SimpleNamespace(service_name="svc", publish=Mock())
    context = svc.MessageContext(topic="t", partition=0, offset=5, key=None)

    svc.example_handler({}, context, service)

    service.publish.assert_not_called()
    captured = capsys.readouterr()
    assert "Missing src_blob at offset 5" in captured.out


def test_example_handler_publishes_when_src_blob_present(capsys):
    service = SimpleNamespace(service_name="svc", publish=Mock())
    context = svc.MessageContext(topic="t", partition=0, offset=8, key=None)

    svc.example_handler({"src_blob": "blob-123"}, context, service)

    service.publish.assert_called_once_with(
        topic="reconstruct_video",
        key="blob-123",
        value={"src_blob": "blob-123"},
    )
    captured = capsys.readouterr()
    assert "Processing src_blob=blob-123 offset=8" in captured.out


def test_build_parser_defaults(monkeypatch):
    monkeypatch.setattr(svc.time, "time", lambda: 1234567890)

    parser = svc.build_parser()
    args = parser.parse_args([])

    assert args.service_name == "template-service"
    assert args.input_topic == "text_to_speech"
    assert args.group_id == "template-1234567890"
    assert args.bootstrap_server == svc.DEFAULT_BOOTSTRAP_SERVER
    assert args.from_beginning is False


def test_build_parser_from_beginning_flag():
    parser = svc.build_parser()
    args = parser.parse_args(["--from-beginning"])

    assert args.from_beginning is True


def test_run_processes_dict_messages_and_cleans_up(monkeypatch, patched_kafka, capsys):
    signal_calls = []

    def fake_signal(sig, handler):
        signal_calls.append((sig, handler))

    monkeypatch.setattr(svc.signal, "signal", fake_signal)

    service = svc.KafkaMicroservice(
        service_name="runner",
        input_topic="input-topic",
        group_id="group-1",
        bootstrap_server="broker:9092",
    )

    msg = SimpleNamespace(
        topic="input-topic",
        partition=1,
        offset=42,
        key="k1",
        value={"src_blob": "abc"},
    )

    consumer = patched_kafka["consumer"]
    consumer._poll_results = [
        {"tp": [msg]},
        {},
    ]

    seen = {}

    def handler(payload, context, service_instance):
        seen["payload"] = payload
        seen["context"] = context
        seen["service"] = service_instance
        service_instance.running = False

    service.run(handler)

    assert len(signal_calls) == 2
    assert seen["payload"] == {"src_blob": "abc"}
    assert seen["context"] == svc.MessageContext(
        topic="input-topic",
        partition=1,
        offset=42,
        key="k1",
    )
    assert seen["service"] is service

    producer = patched_kafka["producer"]
    assert producer.flushed is True
    assert producer.closed is True
    assert consumer.closed is True

    captured = capsys.readouterr()
    assert "[runner] Listening on topic='input-topic'" in captured.out
    assert "--- START processing" in captured.out
    assert "=== FINISHED processing" in captured.out
    assert "[runner] Stopped" in captured.out


def test_run_skips_non_dict_messages(monkeypatch, patched_kafka, capsys):
    monkeypatch.setattr(svc.signal, "signal", lambda *_: None)

    service = svc.KafkaMicroservice(
        service_name="runner",
        input_topic="input-topic",
        group_id="group-1",
        bootstrap_server="broker:9092",
    )

    msg = SimpleNamespace(
        topic="input-topic",
        partition=1,
        offset=99,
        key="k1",
        value="plain-text",
    )

    consumer = patched_kafka["consumer"]
    consumer._poll_results = [
        {"tp": [msg]},
    ]

    def handler(payload, context, service_instance):
        raise AssertionError("Handler should not be called for non-dict payloads")

    service.running = True

    original_poll = consumer.poll

    def wrapped_poll(timeout_ms=1000):
        result = original_poll(timeout_ms)
        service.running = False
        return result

    consumer.poll = wrapped_poll

    service.run(handler)

    captured = capsys.readouterr()
    assert "Skipping non-object payload at offset 99" in captured.out


def test_run_logs_handler_exceptions_and_continues_cleanup(monkeypatch, patched_kafka, capsys):
    monkeypatch.setattr(svc.signal, "signal", lambda *_: None)

    service = svc.KafkaMicroservice(
        service_name="runner",
        input_topic="input-topic",
        group_id="group-1",
        bootstrap_server="broker:9092",
    )

    msg = SimpleNamespace(
        topic="input-topic",
        partition=0,
        offset=12,
        key="err",
        value={"src_blob": "abc"},
    )

    consumer = patched_kafka["consumer"]
    consumer._poll_results = [{"tp": [msg]}]

    original_poll = consumer.poll

    def wrapped_poll(timeout_ms=1000):
        result = original_poll(timeout_ms)
        service.running = False
        return result

    consumer.poll = wrapped_poll

    def bad_handler(payload, context, service_instance):
        raise RuntimeError("boom")

    service.run(bad_handler)

    captured = capsys.readouterr()
    assert "Handler failed for topic='input-topic' partition=0 offset=12: boom" in captured.out

    producer = patched_kafka["producer"]
    assert producer.flushed is True
    assert producer.closed is True
    assert consumer.closed is True