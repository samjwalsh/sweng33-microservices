import argparse
import sys
import time
from pathlib import Path
from typing import Any

CURRENT_DIR = Path(__file__).resolve().parent
KAFKA_DIR = CURRENT_DIR.parent
if str(KAFKA_DIR) not in sys.path:
    sys.path.insert(0, str(KAFKA_DIR))

from microservice_template import KafkaMicroservice, MessageContext
from payload_validation import PayloadValidationError, validate_ingest_payload
from topics import TOPIC_INGEST, TOPIC_TRANSLATE_SEGMENTS, key_by_src_blob

from tempfile import TemporaryDirectory
from blob_helper import download_blob_to_file
from src.ml_models.diarization import diarize
from src.ml_models.transcriber import transcribe_with_timestamps


def detect_source_language(src_blob: str) -> str:
    raise NotImplementedError(
        "Implement source-language detection here using your team's tooling. "
        "Return a short language code such as 'en' or 'fr'."
    )

def text_for_time_range(words, start: float, end: float) -> str:
    matched_words: list[str] = []

    for word in words:
        midpoint = (word.start + word.end) / 2
        if start <= midpoint < end:
            matched_words.append(word.word.strip())

    return " ".join(matched_words).strip()


def diarize_and_transcribe(src_blob: str, src_lang: str) -> list[dict[str, Any]]:
    with TemporaryDirectory() as tmpdir:
        local_wav = Path(tmpdir) / "input.wav"

        download_blob_to_file(
            blob_location=src_blob,
            output_path=local_wav,
        )

        diarization_segments = diarize(str(local_wav))
        diarization_segments = sorted(diarization_segments, key=lambda seg: seg["start"])

        transcription = transcribe_with_timestamps(
            wav_path=local_wav,
            timestamp_level="word",
        )

        segments: list[dict[str, Any]] = []
        for idx, seg in enumerate(diarization_segments):
            seg_start = float(seg["start"])
            seg_end = float(seg["end"])

            segments.append(
                {
                    "segment_id": idx,
                    "speaker_id": seg["speaker"],
                    "start": seg_start,
                    "end": seg_end,
                    "text": text_for_time_range(
                        transcription.words,
                        seg_start,
                        seg_end,
                    ),
                }
            )

        return segments


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