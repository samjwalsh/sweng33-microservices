#!/usr/bin/env python3
"""Simple Kafka health/check script used for debugging topic retention and offsets.

Usage: set KAFKA_BOOTSTRAP_SERVER or rely on default in this repo's .env
"""
from __future__ import annotations

import os
from typing import Iterable

try:
    from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
except Exception:  # pragma: no cover - runtime dependency
    print("Missing kafka library (kafka-python). Install with: pip install kafka-python")
    raise


def bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVER") or os.environ.get("KAFKA_HOST") or "kafka.samjw.xyz:9092"


def list_topics(admin: KafkaAdminClient) -> Iterable[str]:
    return sorted(admin.list_topics())


def show_topic_offsets(bootstrap_server: str, topic: str) -> None:
    consumer = KafkaConsumer(bootstrap_servers=[bootstrap_server], enable_auto_commit=False)
    parts = consumer.partitions_for_topic(topic)
    if not parts:
        print(f"Topic '{topic}' not found or has no partitions")
        return

    for p in sorted(parts):
        tp = TopicPartition(topic, p)
        consumer.assign([tp])
        consumer.seek_to_beginning(tp)
        beg = consumer.position(tp)
        consumer.seek_to_end(tp)
        end = consumer.position(tp)
        print(f"{topic}:{p} beginning={beg} end={end}")


def sample_messages(bootstrap_server: str, topic: str, max_messages: int = 10) -> None:
    print(f"Consuming up to {max_messages} messages from '{topic}' (from beginning)")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_server],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )
    count = 0
    try:
        for msg in consumer:
            print("--- MESSAGE ---")
            print(f"topic={msg.topic} partition={msg.partition} offset={msg.offset} key={msg.key} value={msg.value}")
            count += 1
            if count >= max_messages:
                break
    except Exception as e:
        print("Error while consuming messages:", e)
    finally:
        print(f"Read {count} messages")


def main() -> int:
    bs = bootstrap()
    topic = os.environ.get("KAFKA_CHECK_TOPIC", "ingest")

    print("Using bootstrap server:", bs)

    try:
        admin = KafkaAdminClient(bootstrap_servers=[bs], request_timeout_ms=10000)
    except Exception as e:
        print("Failed to connect to broker with KafkaAdminClient:", e)
        return 2

    try:
        topics = list_topics(admin)
        print("Topics:", topics)
    except Exception as e:
        print("Failed to list topics:", e)
        return 3

    if topic not in topics:
        print(f"Requested check topic '{topic}' not present in broker topics")

    show_topic_offsets(bs, topic)
    sample_messages(bs, topic, max_messages=10)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
