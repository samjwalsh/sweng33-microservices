import time
import argparse
import sys
import time
import subprocess  # For running ffmpeg
import logging     # For error logging
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
from kafka_pipeline.microservice_template import KafkaMicroservice, MessageContext
from kafka_pipeline.payload_validation import PayloadValidationError, validate_ingest_payload
from kafka_pipeline.topics import TOPIC_INGEST, TOPIC_TRANSLATE_SEGMENTS, key_by_src_blob
CURRENT_DIR = Path(__file__).resolve().parent
KAFKA_DIR = CURRENT_DIR.parent
if str(KAFKA_DIR) not in sys.path:
    sys.path.insert(0, str(KAFKA_DIR))

from microservice_template import KafkaMicroservice, MessageContext
from payload_validation import PayloadValidationError, validate_ingest_payload
from topics import TOPIC_INGEST, TOPIC_TRANSLATE_SEGMENTS, key_by_src_blob
from kafka.blob_helper import download_blob_to_file

import whisper 
logger = logging.getLogger("diarization-service")
model = whisper.load_model("base") # This loads the AI into memory

# src_blob is a link to a video file in blob storage.
# This function will have to take the link to the video, download the video, process it in some way (probably to isolate the audio track),
# then use some kind of model to detect what language is being spoken.
# Return the name of that language
def detect_source_language(src_blob: str) -> str:
    # Setup temporary file paths
    video_tmp = Path(f"/tmp/video_{hash(src_blob)}.mp4")
    audio_tmp = video_tmp.with_suffix(".wav")
    
    try:
        # Download from Blob Storage
        download_blob_to_file(src_blob, video_tmp)
        
        # Extract Audio (using ffmpeg )
        # only use the first 30 seconds for language ID
        subprocess.run([
            'ffmpeg', '-i', str(video_tmp), '-t', '30', 
            '-ar', '16000', '-ac', '1', str(audio_tmp), '-y'
        ], check=True, capture_output=True)

        # Detect Language
        # load audio and pad/trim it to fit 30 seconds
        audio = whisper.load_audio(str(audio_tmp))
        audio = whisper.pad_or_trim(audio)
        
        # make log-Mel spectrogram and move to the same device as the model
        mel = whisper.log_mel_spectrogram(audio).to(model.device)
        
        # detect the spoken language
        _, probs = model.detect_language(mel)
        lang_code = max(probs, key=probs.get)

        return lang_code # Returns 'en', 'fr', etc.

    except Exception as e:
        #Log clear failure 
        logger.error(f"Diarization Error: Language detection failed for {src_blob}. Error: {str(e)}")
        
        #Return 'und' (undetermined)
        return "und"

    finally:
        # Cleanup Remove temp files 
        for p in [video_tmp, audio_tmp]:
            if p.exists():
                p.unlink()
    

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