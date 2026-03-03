import argparse
import sys
import time
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
KAFKA_DIR = CURRENT_DIR.parent
if str(KAFKA_DIR) not in sys.path:
    sys.path.insert(0, str(KAFKA_DIR))

from db_helper import TTSSegment, get_segments_for_src_blob
from microservice_template import KafkaMicroservice, MessageContext
from payload_validation import PayloadValidationError, validate_reconstruct_payload
from topics import TOPIC_RECONSTRUCT_VIDEO


def fit_segment_audio_to_timing(*, segment: TTSSegment, input_audio_path: str, output_audio_path: str) -> str:
    raise NotImplementedError(
        "Implement time alignment for one segment in this function. "
        "Input: TTSSegment (start, end) and source generated audio path. "
        "Required behavior: stretch/compress the segment audio so its duration matches (end - start), "
        "then write the adjusted audio to output_audio_path and return that path."
    )


def reconstruct_video(*, src_blob: str, segments: list[TTSSegment]) -> str:
    raise NotImplementedError(
        "Implement reconstruction end-to-end in this function. "
        "Available input: src_blob plus ordered TTSSegment rows (segment_id, speaker_id, start, end, gen_blob). "
        "Required behavior: build the final dubbed output using all generated segment audio, align to timing, compress or stretch audio tracks if necessary"
        "create the new video file, upload to blob storage and return the storage url."
    )


def handler(payload: dict, context: MessageContext, service: KafkaMicroservice) -> None:
    try:
        validate_reconstruct_payload(payload)
    except PayloadValidationError as error:
        print(f"[{service.service_name}] Invalid payload at offset={context.offset}: {error}")
        return

    src_blob = payload["src_blob"]
    segments = get_segments_for_src_blob(src_blob)
    if not segments:
        print(f"[{service.service_name}] No segments found for src_blob={src_blob}")
        return

    segments_missing_audio = [segment.segment_id for segment in segments if not segment.gen_blob]
    if segments_missing_audio:
        print(
            f"[{service.service_name}] Missing generated audio for src_blob={src_blob} "
            f"segment_ids={segments_missing_audio}"
        )
        return

    # Data available to the implementation:
    # - src_blob: source video blob identifier from the pipeline
    # - segments: ordered TTSSegment rows from DB with timing and generated audio refs
    #   (segment_id, speaker_id, start, end, gen_blob)
    #
    # Work to complete inside reconstruct_video:
    # - decide reconstruction strategy (time-stretch/compress, mixing, silence handling, etc.)
    # - produce final dubbed media artifact
    # - store/persist/upload output in the destination your team chooses
    # - return final output reference for logging/integration
    final_output_ref = reconstruct_video(src_blob=src_blob, segments=segments)

    print(
        f"[{service.service_name}] Reconstructed src_blob={src_blob} "
        f"with {len(segments)} segment(s) output={final_output_ref}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconstruction worker")
    parser.add_argument("--group-id", default=f"reconstruct-{int(time.time())}")
    parser.add_argument("--bootstrap-server", default=None)
    parser.add_argument("--from-beginning", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    service = KafkaMicroservice(
        service_name="reconstruction-service",
        input_topic=TOPIC_RECONSTRUCT_VIDEO,
        group_id=args.group_id,
        bootstrap_server=args.bootstrap_server,
        from_beginning=args.from_beginning,
    )
    service.run(handler)


if __name__ == "__main__":
    main()
