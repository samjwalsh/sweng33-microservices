import time
import argparse
from typing import Any

from kafka_pipeline.microservice_template import KafkaMicroservice, MessageContext
from kafka_pipeline.payload_validation import PayloadValidationError, validate_ingest_payload
from kafka_pipeline.topics import TOPIC_INGEST, TOPIC_TRANSLATE_SEGMENTS, key_by_src_blob

# src_blob is a link to a video file in blob storage.
# This function will have to take the link to the video, download the video, process it in some way (probably to isolate the audio track),
# then use some kind of model to detect what language is being spoken.
# Return the name of that language
def detect_source_language(src_blob: str) -> str:
    raise NotImplementedError(
        "Implement source-language detection here using your team's tooling. "
        "Return a short language code such as 'en' or 'fr'."
    )

# This function will take in a link to the video in blob storage as well as the language the video is in.
# It will diarize and transcribe the file and return an array of segments, each segment looks like this:
# {'segment_id': int, 'speaker_id': str, 'start': float, 'end': float, 'text': str}
def diarize_and_transcribe(src_blob: str, src_lang: str) -> list[dict[str, Any]]:
    raise NotImplementedError(
        "Implement diarization/transcription here using your team's tooling. "
        "Return a list of segments shaped as: "
        "{'segment_id': int, 'speaker_id': str, 'start': float, 'end': float, 'text': str}."
    )


def handler(payload: dict[str, Any], context: MessageContext, service: KafkaMicroservice) -> None:
    try:
        validate_ingest_payload(payload)
    except PayloadValidationError as error:
        print(f"[{service.service_name}] Invalid payload at offset={context.offset}: {error}")
        return

    src_blob = payload["src_blob"]
    src_lang = payload.get("src_lang")
    if src_lang is None:
        src_lang = detect_source_language(src_blob)

    segments = diarize_and_transcribe(src_blob=src_blob, src_lang=src_lang)

    if not segments:
        print(f"[{service.service_name}] No segments produced for src_blob={src_blob}")
        return

    outbound = {
        "src_blob": src_blob,
        "src_lang": src_lang,
        "dest_lang": payload["dest_lang"],
        "segments": segments,
    }
    service.publish(
        topic=TOPIC_TRANSLATE_SEGMENTS,
        key=key_by_src_blob(src_blob),
        value=outbound,
    )
    print(
        f"[{service.service_name}] Published {len(segments)} segments "
        f"for src_blob={src_blob} from offset={context.offset}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diarization + language-detection worker")
    parser.add_argument("--group-id", default=f"diarization-{int(time.time())}")
    parser.add_argument("--bootstrap-server", default=None)
    parser.add_argument("--from-beginning", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    service = KafkaMicroservice(
        service_name="diarization-service",
        input_topic=TOPIC_INGEST,
        group_id=args.group_id,
        bootstrap_server=args.bootstrap_server,
        from_beginning=args.from_beginning,
    )
    service.run(handler)


if __name__ == "__main__":
    main()