import argparse
import time
import subprocess
import logging
import os
import sys
import hashlib
from pathlib import Path
from typing import Any
from kafka_pipeline.blob_helper import download_blob, download_blob_to_file, upload_file

CURRENT_DIR = Path(__file__).resolve().parent
KAFKA_DIR = CURRENT_DIR.parent
if str(KAFKA_DIR) not in sys.path:
    sys.path.insert(0, str(KAFKA_DIR))

from db_helper import are_all_segments_generated, set_tts_generated_blob
from microservice_template import KafkaMicroservice, MessageContext
from payload_validation import PayloadValidationError, validate_tts_payload
from topics import TOPIC_RECONSTRUCT_VIDEO, TOPIC_TEXT_TO_SPEECH, key_by_src_blob

logger = logging.getLogger("tts-service")

from src.ml_models.elevenlabs_tts import (clone_voice_from_refs, generate_tts_audio, convert_mp3_to_wav, save_audio_stream )
from tempfile import TemporaryDirectory
_VOICE_PROFILE_CACHE: dict[tuple[str, str, str], str] = {}


def _stable_id(value: str, length: int = 16) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def select_voice_clone_training_segments(
    segments: list[dict[str, Any]],
    max_training_segments: int = 5,
) -> list[dict[str, Any]]:
    candidates = [segment for segment in segments if str(segment.get("text", "")).strip()]
    ranked = sorted(
        candidates or segments,
        key=lambda segment: float(segment["end"]) - float(segment["start"]),
        reverse=True,
    )
    return ranked[:max_training_segments]


# This function takes in the link to the source video, and some segments
# It finds those segments in the source video and extracts the audio of each one into a new
# wav file (to be used for voice cloning training), and uploads this wav file to blob storage
# and returns the link to that file.
def prepare_voice_clone_training_data(
    *,
    src_blob: str,
    speaker_id: str,
    training_segments: list[dict[str, Any]],
) -> str:
    # Setup paths
    blob_key = _stable_id(f"{src_blob}|{speaker_id}")
    video_tmp = Path(f"/tmp/src_{blob_key}.mp4")
    output_wav = Path(f"/tmp/training_{blob_key}.wav")

    try:
        # Download source video
        download_blob_to_file(blob_location=src_blob, output_path=video_tmp)

        # Construct FFmpeg Filter String
        # want to select segments like: between(t,10,15)+between(t,30,35)
        if not training_segments:
            raise ValueError("No training segments provided.")

        filter_parts = []
        for seg in training_segments:
            start = seg['start']
            end = seg['end']
            filter_parts.append(f"between(t,{start},{end})")
        
        filter_str = "+".join(filter_parts)

        # Run FFmpeg
        # extracts audio, filters for the timestamps, and concats them
        # -ar 22050 -ac 1 is standard for voice cloning models
        subprocess.run([
            'ffmpeg', '-i', str(video_tmp),
            '-af', f'aselect=\'{filter_str}\',asetpts=N/SR/TB',
            '-ar', '22050', '-ac', '1', str(output_wav), '-y'
        ], check=True, capture_output=True)

        # Upload resulting file
        # store as voice_cloning/training_{blob_key}.wav in blob storage
        remote_path = upload_file(
            local_path=str(output_wav),
            folder="voice_cloning",
            blob_name=f"training_{blob_key}.wav",
            overwrite=True,
        )
        
        return remote_path

    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.decode() if e.stderr else str(e)
        logger.error(f"FFmpeg error during voice clone prep for {src_blob}: {error_msg}")
        raise  # Re-raise so the worker knows this task failed

    except Exception as e:
        logger.error(f"Unexpected failure in voice clone prep for {src_blob}: {str(e)}")
        raise

    finally:
        # Cleanup: Always remove local temp files
        if video_tmp.exists(): video_tmp.unlink()
        if output_wav.exists(): output_wav.unlink()


# This function takes the training audio link and uses it to clone the voice to be used for the next step
def clone_voice_once(
    *,
    src_blob: str,
    speaker_id: str,
    training_audio_refs: str,
) -> str:
    cache_key = (src_blob, speaker_id, training_audio_refs)

    if cache_key in _VOICE_PROFILE_CACHE:
        return _VOICE_PROFILE_CACHE[cache_key]

    def load_audio_bytes(ref: str) -> bytes:
        ref_path = Path(ref)
        if ref_path.exists():
            return ref_path.read_bytes()
        return download_blob(ref)

    voice_profile_id = clone_voice_from_refs(
        voice_name=f"clone_{_stable_id('|'.join(cache_key), 20)}",
        training_audio_refs=[training_audio_refs],
        load_audio_bytes=load_audio_bytes,
    )

    _VOICE_PROFILE_CACHE[cache_key] = voice_profile_id
    return voice_profile_id


# This function takes in some text and a voice profile. 
# Generate audio using the profile and text and return the blob url of the wav file where it is stored. 
def synthesize_segment_audio(
    *,
    text: str,
    voice_profile_id: str,
) -> str:
    
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        mp3_path = tmpdir_path / "segment.mp3"
        wav_path = tmpdir_path / "segment.wav"

        audio = generate_tts_audio(
            voice_id=voice_profile_id,
            text=text,
        )

        save_audio_stream(audio, mp3_path)
        convert_mp3_to_wav(
            mp3_path=mp3_path,
            wav_path=wav_path,
        )

        return upload_file(
            local_path=wav_path,
            folder="generated/segments",
            overwrite=True,
        )


def handler(payload: dict[str, Any], context: MessageContext, service: KafkaMicroservice) -> None:
    try:
        validate_tts_payload(payload)
    except PayloadValidationError as error:
        print(f"[{service.service_name}] Invalid payload at offset={context.offset}: {error}")
        return

    src_blob = payload["src_blob"]
    speaker_id = payload["speaker_id"]
    segments = payload["segments"]

    training_segments = select_voice_clone_training_segments(segments)

    training_audio_refs = prepare_voice_clone_training_data(
        src_blob=src_blob,
        speaker_id=speaker_id,
        training_segments=training_segments,
    )

    voice_profile_id = clone_voice_once(
        src_blob=src_blob,
        speaker_id=speaker_id,
        training_audio_refs=training_audio_refs,
    )

    for segment in segments:
        gen_blob = synthesize_segment_audio(
            text=segment["text"],
            voice_profile_id=voice_profile_id,
        )
        set_tts_generated_blob(
            src_blob=src_blob,
            segment_id=segment["segment_id"],
            gen_blob=gen_blob,
        )

    if are_all_segments_generated(src_blob):
        service.publish(
            topic=TOPIC_RECONSTRUCT_VIDEO,
            key=key_by_src_blob(src_blob),
            value={"src_blob": src_blob},
        )

    print(
        f"[{service.service_name}] Processed speaker={speaker_id} "
        f"segments={len(segments)} src_blob={src_blob}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Text-to-speech worker")
    parser.add_argument("--group-id", default=f"tts-{int(time.time())}")
    parser.add_argument("--bootstrap-server", default=None)
    parser.add_argument("--from-beginning", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    service = KafkaMicroservice(
        service_name="tts-service",
        input_topic=TOPIC_TEXT_TO_SPEECH,
        group_id=args.group_id,
        bootstrap_server=args.bootstrap_server,
        from_beginning=args.from_beginning,
    )
    service.run(handler)


if __name__ == "__main__":
    main()
