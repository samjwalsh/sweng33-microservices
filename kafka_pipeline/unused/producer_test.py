import argparse
import json
import time
import uuid

from kafka import KafkaProducer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish test events to Kafka.")
    parser.add_argument("--bootstrap-server", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="test", help="Kafka topic")
    parser.add_argument("--count", type=int, default=1, help="Number of events to publish")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay between sends in seconds")
    return parser


def main() -> None:
    args = build_parser().parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_server],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    run_id = str(uuid.uuid4())

    try:
        for index in range(args.count):
            event = {
                "run_id": run_id,
                "sequence": index,
                "sent_at": time.time(),
                "message": f"kafka test event {index}",
            }
            future = producer.send(args.topic, value=event)
            metadata = future.get(timeout=10)
            print(
                "Published:",
                {
                    "topic": metadata.topic,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                    "event": event,
                },
            )
            if args.delay > 0:
                time.sleep(args.delay)
    finally:
        producer.flush()
        producer.close()

    print(f"Done. run_id={run_id}")


if __name__ == "__main__":
    main()
