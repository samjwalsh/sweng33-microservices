from __future__ import annotations  # optional, helps with type hints in Python <3.11

import argparse
import time
import subprocess
import json
from pathlib import Path
import tempfile

from typing import Union

from kafka_pipeline.db_helper import TTSSegment, get_segments_for_src_blob, set_video_completed_blob
from kafka_pipeline.audio_utils import merge_audio, stitch_audio_with_timestamps
from kafka_pipeline.microservice_template import KafkaMicroservice, MessageContext
from kafka_pipeline.payload_validation import PayloadValidationError, validate_reconstruct_payload
from kafka_pipeline.topics import TOPIC_RECONSTRUCT_VIDEO
from kafka_pipeline.blob_helper import upload_file, download_blob_to_file, build_reconstruction_blob_name

def fit_segment_audio_to_timing(*, segment: TTSSegment, input_audio_path: str, output_audio_path: str) -> str:
    "Given a TTSSegment with start/end timing and an input audio file, stretch or compress the audio to fit the target duration. "

    if segment.end <= segment.start:
        raise ValueError(f"Invalid segment timing: start={segment.start} end={segment.end}")
    
    target_duration = segment.end - segment.start

    if target_duration < 0:
        raise ValueError(f"Invalid segment timing: start={segment.start} end={segment.end}")
    
    elif target_duration == 0:
        "Segment has zero duration, skipping audio generation."

    else:
        orig = get_audio_duration(input_audio_path)
        if orig <= 0:
            raise ValueError(f"Could not determine duration for: {input_audio_path}")
        time_ratio = target_duration / orig

        try:
            subprocess.run(
                [
                    "rubberband",
                    "-t",
                    f"{time_ratio:.8f}",
                    str(input_audio_path),
                    str(output_audio_path),
                ],
                check=True,
            )
        except FileNotFoundError as e:
            raise FileNotFoundError(
                "Could not find `rubberband` on PATH"
            ) from e

        return str(output_audio_path)

    
def get_audio_duration(path: Union[str, Path]) -> float:
    "Use ffprobe to get the duration of an audio file in seconds. "
        
    try:
        p = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "json",
                str(path),
            ],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        )
    except FileNotFoundError as e:
        raise FileNotFoundError(
            "Could not find `ffprobe` on PATH"
        ) from e

    return float(json.loads(p.stdout)["format"]["duration"])


def reconstruct_video(*, src_blob: str, segments: list[TTSSegment]) -> str:
    """
    Implement reconstruction end-to-end in this function. 
    Available input: src_blob plus ordered TTSSegment rows (segment_id, speaker_id, start, end, gen_blob). 
    Required behavior: build the final dubbed output using all generated segment audio, align to timing, compress or stretch audio tracks if necessary
    create the new video file, upload to blob storage and return the storage url.
    """

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir)
        original_video = download_blob_to_file(blob_location=src_blob, output_path=tmp_dir / "original.mp4")
        wav_dir = tmp_dir / "wavs"
        wav_dir.mkdir()
        segments_info = []

        for seg in sorted(segments, key=lambda s: s.start):
            raw_path = wav_dir / f"raw_{seg.segment_id}.wav"
            fitted_path = wav_dir / f"fitted_{seg.segment_id}.wav"

            download_blob_to_file(blob_location=seg.gen_blob, output_path=raw_path)
            fit_segment_audio_to_timing(
                segment=seg,
                input_audio_path=raw_path,
                output_audio_path=fitted_path,
            )
            segments_info.append({
                "path": fitted_path,
                "start": seg.start,
                "end": seg.end,
            })

        stitched_wav = tmp_dir / "stitched.wav"
        stitch_audio_with_timestamps(segments_info, stitched_wav)
        output_mp4 = tmp_dir / "output.mp4"
        merge_audio(input_wav=stitched_wav,input_mp4=original_video,output_mp4=output_mp4)
        upload_name = build_reconstruction_blob_name(src_blob=src_blob, ext=".mp4")
        upload_file(local_path=output_mp4,blob_name=upload_name)
        return upload_name
        
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
    updated_video = set_video_completed_blob(src_blob=src_blob, completed_blob=final_output_ref)
    if not updated_video:
        print(
            f"[{service.service_name}] Could not update completed_blob for src_blob={src_blob}"
        )

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
