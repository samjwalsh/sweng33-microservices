import json
import os
from typing import Any

from dotenv import load_dotenv
from kafka import KafkaProducer


load_dotenv()


def resolve_bootstrap_server(explicit: str | None = None) -> str:
    return explicit or os.getenv("KAFKA_BOOTSTRAP_SERVER") or os.getenv("KAFKA_HOST")


DEFAULT_BOOTSTRAP_SERVER = resolve_bootstrap_server()


def build_producer(bootstrap_server: str | None = DEFAULT_BOOTSTRAP_SERVER) -> KafkaProducer:
    resolved_bootstrap_server = resolve_bootstrap_server(bootstrap_server)
    return KafkaProducer(
        bootstrap_servers=[resolved_bootstrap_server],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
    )


def send_kafka_message(
    topic: str,
    value: dict[str, Any],
    key: str | None = None,
    bootstrap_server: str | None = DEFAULT_BOOTSTRAP_SERVER,
    timeout_seconds: int = 10,
) -> dict[str, Any]:
    producer = build_producer(bootstrap_server=bootstrap_server)
    try:
        future = producer.send(topic, key=key, value=value)
        metadata = future.get(timeout=timeout_seconds)
        producer.flush()
        return {
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
            "key": key,
        }
    finally:
        producer.close()
