import argparse
import time
from typing import Any

from payload_validation import (
    validate_ingest_payload,
    validate_reconstruct_payload,
    validate_translate_payload,
    validate_tts_payload,
)
from pipeline_producer import send_kafka_message
from topics import (
    TOPIC_INGEST,
    TOPIC_RECONSTRUCT_VIDEO,
    TOPIC_TEXT_TO_SPEECH,
    TOPIC_TRANSLATE_SEGMENTS,
    key_by_src_blob,
    key_by_src_blob_and_speaker,
)


def build_segments(
    *,
    speakers: int,
    segments_per_speaker: int,
    start_seconds: float,
    segment_duration: float,
    gap_seconds: float,
) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    segment_id = 0
    current_start = start_seconds

    for speaker_index in range(speakers):
        speaker_id = f"speaker_{speaker_index}"
        for speaker_segment_index in range(segments_per_speaker):
            segment_start = current_start
            segment_end = segment_start + segment_duration
            segments.append(
                {
                    "segment_id": segment_id,
                    "speaker_id": speaker_id,
                    "start": segment_start,
                    "end": segment_end,
                    "text": f"Sample text {segment_id} for {speaker_id} segment {speaker_segment_index}",
                }
            )
            segment_id += 1
            current_start = segment_end + gap_seconds

    return segments


def build_ingest_payload(*, src_blob: str, src_lang: str | None, dest_lang: str) -> dict[str, Any]:
    payload = {
        "src_blob": src_blob,
        "src_lang": src_lang,
        "dest_lang": dest_lang,
    }
    validate_ingest_payload(payload)
    return payload


def build_translate_payload(
    *,
    src_blob: str,
    src_lang: str,
    dest_lang: str,
    segments: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "src_blob": src_blob,
        "src_lang": src_lang,
        "dest_lang": dest_lang,
        "segments": segments,
    }
    validate_translate_payload(payload)
    return payload


def build_tts_payload(
    *,
    src_blob: str,
    src_lang: str,
    dest_lang: str,
    segments: list[dict[str, Any]],
    speaker_id: str,
) -> dict[str, Any]:
    speaker_segments = [segment for segment in segments if segment["speaker_id"] == speaker_id]
    payload = {
        "src_blob": src_blob,
        "src_lang": src_lang,
        "dest_lang": dest_lang,
        "speaker_id": speaker_id,
        "segments": speaker_segments,
    }
    validate_tts_payload(payload)
    return payload


def build_reconstruct_payload(*, src_blob: str) -> dict[str, Any]:
    payload = {"src_blob": src_blob}
    validate_reconstruct_payload(payload)
    return payload


def publish(
    *,
    topic: str,
    key: str,
    payload: dict[str, Any],
    bootstrap_server: str | None,
) -> None:
    metadata = send_kafka_message(
        topic=topic,
        key=key,
        value=payload,
        bootstrap_server=bootstrap_server,
    )
    print(f"Published {topic}: {metadata}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create test jobs for microservice queues (single queue or all queues)."
    )
    parser.add_argument(
        "--target",
        choices=["all", "ingest", "translate", "tts", "reconstruct"],
        default="all",
        help="Which queue(s) to publish test jobs to.",
    )
    parser.add_argument(
        "--src-blob",
        default=f"video-test-{int(time.time())}",
        help="Source blob identifier used in test payloads.",
    )
    parser.add_argument("--src-lang", default="en", help="Source language code.")
    parser.add_argument("--dest-lang", default="fr", help="Destination language code.")
    parser.add_argument("--speakers", type=int, default=2, help="Number of speakers to generate.")
    parser.add_argument(
        "--segments-per-speaker",
        type=int,
        default=2,
        help="Number of segments per speaker.",
    )
    parser.add_argument(
        "--tts-speaker-id",
        default="speaker_0",
        help="Speaker ID to include in the TTS payload when target includes tts.",
    )
    parser.add_argument("--start-seconds", type=float, default=0.0)
    parser.add_argument("--segment-duration", type=float, default=2.0)
    parser.add_argument("--gap-seconds", type=float, default=0.2)
    parser.add_argument("--bootstrap-server", default=None, help="Kafka bootstrap server override.")
    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.speakers < 1:
        raise ValueError("--speakers must be >= 1")
    if args.segments_per_speaker < 1:
        raise ValueError("--segments-per-speaker must be >= 1")

    all_segments = build_segments(
        speakers=args.speakers,
        segments_per_speaker=args.segments_per_speaker,
        start_seconds=args.start_seconds,
        segment_duration=args.segment_duration,
        gap_seconds=args.gap_seconds,
    )

    valid_speaker_ids = {segment["speaker_id"] for segment in all_segments}
    if args.tts_speaker_id not in valid_speaker_ids:
        raise ValueError(
            f"--tts-speaker-id={args.tts_speaker_id} not present in generated segments. "
            f"Available: {sorted(valid_speaker_ids)}"
        )

    target = args.target

    if target in {"all", "ingest"}:
        ingest_payload = build_ingest_payload(
            src_blob=args.src_blob,
            src_lang=args.src_lang,
            dest_lang=args.dest_lang,
        )
        publish(
            topic=TOPIC_INGEST,
            key=key_by_src_blob(args.src_blob),
            payload=ingest_payload,
            bootstrap_server=args.bootstrap_server,
        )

    if target in {"all", "translate"}:
        translate_payload = build_translate_payload(
            src_blob=args.src_blob,
            src_lang=args.src_lang,
            dest_lang=args.dest_lang,
            segments=all_segments,
        )
        publish(
            topic=TOPIC_TRANSLATE_SEGMENTS,
            key=key_by_src_blob(args.src_blob),
            payload=translate_payload,
            bootstrap_server=args.bootstrap_server,
        )

    if target in {"all", "tts"}:
        tts_payload = build_tts_payload(
            src_blob=args.src_blob,
            src_lang=args.src_lang,
            dest_lang=args.dest_lang,
            segments=all_segments,
            speaker_id=args.tts_speaker_id,
        )
        publish(
            topic=TOPIC_TEXT_TO_SPEECH,
            key=key_by_src_blob_and_speaker(args.src_blob, args.tts_speaker_id),
            payload=tts_payload,
            bootstrap_server=args.bootstrap_server,
        )

    if target in {"all", "reconstruct"}:
        reconstruct_payload = build_reconstruct_payload(src_blob=args.src_blob)
        publish(
            topic=TOPIC_RECONSTRUCT_VIDEO,
            key=key_by_src_blob(args.src_blob),
            payload=reconstruct_payload,
            bootstrap_server=args.bootstrap_server,
        )

    print(f"Done. target={target} src_blob={args.src_blob}")


if __name__ == "__main__":
    main()
