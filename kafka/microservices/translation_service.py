import argparse
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

CURRENT_DIR = Path(__file__).resolve().parent
KAFKA_DIR = CURRENT_DIR.parent
if str(KAFKA_DIR) not in sys.path:
    sys.path.insert(0, str(KAFKA_DIR))

from db_helper import upsert_tts_placeholder
from microservice_template import KafkaMicroservice, MessageContext
from payload_validation import PayloadValidationError, validate_translate_payload
from topics import TOPIC_TEXT_TO_SPEECH, TOPIC_TRANSLATE_SEGMENTS, key_by_src_blob_and_speaker


def translate_segment_text(text: str, src_lang: str, dest_lang: str) -> str:
    raise NotImplementedError(
        "Translate and return the text"
    )


def handler(payload: dict[str, Any], context: MessageContext, service: KafkaMicroservice) -> None:
    try:
        validate_translate_payload(payload)
    except PayloadValidationError as error:
        print(f"[{service.service_name}] Invalid payload at offset={context.offset}: {error}")
        return

    src_blob = payload["src_blob"]
    src_lang = payload["src_lang"]
    dest_lang = payload["dest_lang"]
    segments = payload["segments"]

    translated_segments = []
    for segment in segments:
        translated_text = translate_segment_text(segment["text"], src_lang, dest_lang)
        translated_segment = {
            **segment,
            "text": translated_text,
        }
        translated_segments.append(translated_segment)

        upsert_tts_placeholder(
            src_blob=src_blob,
            segment_id=segment["segment_id"],
            speaker_id=segment["speaker_id"],
            start=float(segment["start"]),
            end=float(segment["end"]),
        )

    grouped_by_speaker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for segment in translated_segments:
        grouped_by_speaker[segment["speaker_id"]].append(segment)

    for speaker_id, speaker_segments in grouped_by_speaker.items():
        outbound = {
            "src_blob": src_blob,
            "src_lang": src_lang,
            "dest_lang": dest_lang,
            "speaker_id": speaker_id,
            "segments": speaker_segments,
        }
        service.publish(
            topic=TOPIC_TEXT_TO_SPEECH,
            key=key_by_src_blob_and_speaker(src_blob, speaker_id),
            value=outbound,
        )

    print(
        f"[{service.service_name}] Processed {len(segments)} segments and "
        f"published {len(grouped_by_speaker)} speaker batch(es) for src_blob={src_blob}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Translation worker")
    parser.add_argument("--group-id", default=f"translation-{int(time.time())}")
    parser.add_argument("--bootstrap-server", default=None)
    parser.add_argument("--from-beginning", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    service = KafkaMicroservice(
        service_name="translation-service",
        input_topic=TOPIC_TRANSLATE_SEGMENTS,
        group_id=args.group_id,
        bootstrap_server=args.bootstrap_server,
        from_beginning=args.from_beginning,
    )
    service.run(handler)


if __name__ == "__main__":
    main()
