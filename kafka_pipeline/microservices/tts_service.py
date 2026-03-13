import argparse
import time
from typing import Any

from kafka_pipeline.db_helper import are_all_segments_generated, set_tts_generated_blob
from kafka_pipeline.microservice_template import KafkaMicroservice, MessageContext
from kafka_pipeline.payload_validation import PayloadValidationError, validate_tts_payload
from kafka_pipeline.topics import TOPIC_RECONSTRUCT_VIDEO, TOPIC_TEXT_TO_SPEECH, key_by_src_blob

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
    training_segments: list[dict[str, Any]],
) -> str:
    raise NotImplementedError(
        "Implement extraction of source-audio clips for the selected training segments. "
        "Return a the path to the wav file which contains the selected training segments' audio"
    )


# This function takes the training audio link and uses it to clone the voice to be used for the next step
def clone_voice_once(
    *,
    src_blob: str,
    training_audio_refs: str,
) -> str:
    raise NotImplementedError(
        "Implement voice cloning here. "
        "Use training_audio_refs to create/retrieve one cloned voice profile for this speaker, "
        "then return its voice_profile_id."
    )


# This function takes in some text and a voice profile. 
# Generate audio using the profile and text and return the blob url of the wav file where it is stored. 
def synthesize_segment_audio(
    *,
    text: str,
    voice_profile_id: str,
) -> str:
    raise NotImplementedError(
        "Implement segment synthesis here. "
        "Use voice_profile_id to generate audio for this segment and return the generated blob/path reference."
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
        training_segments=training_segments,
    )

    voice_profile_id = clone_voice_once(
        src_blob=src_blob,
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
