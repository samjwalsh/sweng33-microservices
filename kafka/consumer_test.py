import argparse
import json
import signal
import sys
import time
from typing import Any

from kafka import KafkaConsumer


RUNNING = True


def stop_consumer(_signum: int, _frame: Any) -> None:
    global RUNNING
    RUNNING = False


def safe_deserialize(value: bytes) -> Any:
    text = value.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume test events from Kafka.")
    parser.add_argument("--bootstrap-server", default="kafka.samjw.xyz:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="test", help="Kafka topic")
    parser.add_argument(
        "--group-id",
        default="",
        help="Kafka consumer group (default: unique group per run)",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read from earliest offset for this group",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after this many messages (0 means keep running)",
    )
    parser.add_argument(
        "--startup-timeout-seconds",
        type=int,
        default=15,
        help="Seconds to wait for partition assignment before consuming",
    )
    return parser


def main() -> None:
    global RUNNING
    args = build_parser().parse_args()

    auto_offset_reset = "earliest" if args.from_beginning else "latest"
    group_id = args.group_id or f"kafka-test-consumer-{int(time.time())}"

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[args.bootstrap_server],
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=safe_deserialize,
        key_deserializer=lambda key: key.decode("utf-8", errors="replace") if key else None,
        consumer_timeout_ms=1000,  # lets loop check RUNNING/max-messages regularly
    )

    signal.signal(signal.SIGINT, stop_consumer)
    signal.signal(signal.SIGTERM, stop_consumer)

    print(
        f"Connecting to topic='{args.topic}' at {args.bootstrap_server} "
        f"(group='{group_id}', offset='{auto_offset_reset}')"
    )

    deadline = time.time() + args.startup_timeout_seconds
    while RUNNING and time.time() < deadline and not consumer.assignment():
        consumer.poll(timeout_ms=500)

    assignment = consumer.assignment()
    if assignment:
        print(f"Consumer ready. Assigned partitions: {[str(tp) for tp in assignment]}")
    else:
        print(
            "Warning: no partition assignment yet. "
            "If no messages are consumed, retry with --from-beginning or increase --startup-timeout-seconds."
        )

    read_count = 0

    try:
        while RUNNING:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue

            for _, messages in records.items():
                for message in messages:
                    print(
                        {
                            "topic": message.topic,
                            "partition": message.partition,
                            "offset": message.offset,
                            "key": message.key,
                            "value": message.value,
                        }
                    )
                    read_count += 1

                    if args.max_messages > 0 and read_count >= args.max_messages:
                        print(f"Reached max messages: {args.max_messages}")
                        RUNNING = False
                        break
                if not RUNNING:
                    break
    finally:
        consumer.close()

    print("Consumer stopped.")
    sys.exit(0)


if __name__ == "__main__":
    main()
