import argparse
import json
import os
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer


load_dotenv()


def resolve_bootstrap_server(explicit: str | None = None) -> str:
    return explicit or os.getenv("KAFKA_BOOTSTRAP_SERVER") or os.getenv("KAFKA_HOST")


DEFAULT_BOOTSTRAP_SERVER = resolve_bootstrap_server()


@dataclass(frozen=True)
class MessageContext:
    topic: str
    partition: int
    offset: int
    key: str | None


class KafkaMicroservice:
    def __init__(
        self,
        *,
        service_name: str,
        input_topic: str,
        group_id: str,
        bootstrap_server: str | None = DEFAULT_BOOTSTRAP_SERVER,
        from_beginning: bool = False,
    ) -> None:
        self.service_name = service_name
        self.input_topic = input_topic
        self.group_id = group_id
        self.bootstrap_server = resolve_bootstrap_server(bootstrap_server)
        self.from_beginning = from_beginning
        self.running = True

        auto_offset_reset = "earliest" if from_beginning else "latest"

        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=[self.bootstrap_server],
            group_id=self.group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=self._safe_deserialize,
            key_deserializer=lambda key: key.decode("utf-8", errors="replace") if key else None,
            consumer_timeout_ms=1000,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=[self.bootstrap_server],
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
        )

    @staticmethod
    def _safe_deserialize(value: bytes) -> Any:
        text = value.decode("utf-8", errors="replace")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    def publish(self, topic: str, value: dict[str, Any], key: str | None = None, timeout_seconds: int = 10) -> None:
        future = self.producer.send(topic, key=key, value=value)
        future.get(timeout=timeout_seconds)

    def stop(self, _signum: int, _frame: Any) -> None:
        self.running = False

    def run(self, handler: Callable[[dict[str, Any], MessageContext, "KafkaMicroservice"], None]) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        print(
            f"[{self.service_name}] Listening on topic='{self.input_topic}' "
            f"group='{self.group_id}' broker='{self.bootstrap_server}'"
        )

        while self.running:
            records = self.consumer.poll(timeout_ms=1000)
            if not records:
                continue

            for _, messages in records.items():
                for message in messages:
                    value = message.value
                    if not isinstance(value, dict):
                        print(f"[{self.service_name}] Skipping non-object payload at offset {message.offset}")
                        continue

                    context = MessageContext(
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        key=message.key,
                    )
                    handler(value, context, self)

        self.producer.flush()
        self.producer.close()
        self.consumer.close()
        print(f"[{self.service_name}] Stopped")


def example_handler(payload: dict[str, Any], context: MessageContext, service: KafkaMicroservice) -> None:
    src_blob = payload.get("src_blob")
    if not src_blob:
        print(f"[{service.service_name}] Missing src_blob at offset {context.offset}")
        return

    print(f"[{service.service_name}] Processing src_blob={src_blob} offset={context.offset}")

    outbound = {"src_blob": src_blob}
    service.publish(topic="reconstruct_video", key=src_blob, value=outbound)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kafka microservice wrapper template")
    parser.add_argument("--service-name", default="template-service")
    parser.add_argument("--input-topic", default="text_to_speech")
    parser.add_argument("--group-id", default=f"template-{int(time.time())}")
    parser.add_argument("--bootstrap-server", default=DEFAULT_BOOTSTRAP_SERVER)
    parser.add_argument("--from-beginning", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()

    service = KafkaMicroservice(
        service_name=args.service_name,
        input_topic=args.input_topic,
        group_id=args.group_id,
        bootstrap_server=args.bootstrap_server,
        from_beginning=args.from_beginning,
    )
    service.run(example_handler)


if __name__ == "__main__":
    main()
