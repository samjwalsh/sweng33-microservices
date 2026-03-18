import argparse
import time

import os
import logging
from dotenv import load_dotenv
from collections import defaultdict
from typing import Any
from azure.ai.translation.text import TextTranslationClient, TranslatorCredential
from azure.ai.translation.text.models import InputTextItem


from kafka_pipeline.db_helper import upsert_tts_placeholder, increment_translation_completed_tasks, increment_tts_total_tasks
from kafka_pipeline.microservice_template import KafkaMicroservice, MessageContext
from kafka_pipeline.payload_validation import PayloadValidationError, validate_translate_payload
from kafka_pipeline.topics import TOPIC_TEXT_TO_SPEECH, TOPIC_TRANSLATE_SEGMENTS, key_by_src_blob_and_speaker

logger = logging.getLogger(__name__)
load_dotenv() 

TRANSLATOR_KEY = os.getenv("TRANSLATOR_KEY")         
TRANSLATOR_ENDPOINT = os.getenv("TRANSLATOR_ENDPOINT") 
TRANSLATOR_REGION = os.getenv("TRANSLATOR_REGION")    

translator_client = None

try:   # ADDED
    if TRANSLATOR_KEY and TRANSLATOR_ENDPOINT and TRANSLATOR_REGION:
        credential = TranslatorCredential(TRANSLATOR_KEY, TRANSLATOR_REGION)
        translator_client = TextTranslationClient(
            endpoint=TRANSLATOR_ENDPOINT,
            credential=credential,
        )
        logger.info("Azure translation client loaded successfully.")
    else:
        logger.error(
            "Missing Azure Translator configuration. "
            "Set TRANSLATOR_KEY, TRANSLATOR_ENDPOINT, and TRANSLATOR_REGION."
        )
except Exception as e:
    logger.error(f"Failed to initialise Azure translation client: {e}")
    translator_client = None

def translate_segment_text(text: str, src_lang: str, dest_lang: str) -> str:
    if text is None:
        return ""

    text = text.strip()

    if text == "":
        return ""

    if src_lang == dest_lang:
        return text

    if translator_client is None:
        return text

    try:
        response = translator_client.translate(
            content=[InputTextItem(text=text)],
            to=[dest_lang],
            from_parameter=src_lang,
        )

        if response and response[0].translations:
            translated_text = response[0].translations[0].text
            if translated_text:
                return translated_text

    except Exception:
        pass

    return text


def handler(payload: dict[str, Any], context: MessageContext, service: KafkaMicroservice) -> None:
    # Filter out segments with empty or non-string `text` before validating.
    raw_segments = payload.get("segments", [])
    non_empty_segments = [
        s for s in raw_segments
        if isinstance(s.get("text"), str) and s.get("text").strip() != ""
    ]

    if not non_empty_segments:
        print(f"[{service.service_name}] No non-empty segments to process at offset={context.offset}; skipping")
        return

    # Validate using a payload copy that only contains non-empty segments
    payload_for_validation = {**payload, "segments": non_empty_segments}
    try:
        validate_translate_payload(payload_for_validation)
    except PayloadValidationError as error:
        print(f"[{service.service_name}] Invalid payload at offset={context.offset}: {error}")
        return

    src_blob = payload["src_blob"]
    src_lang = payload["src_lang"]
    dest_lang = payload["dest_lang"]
    segments = non_empty_segments

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
        increment_tts_total_tasks(src_blob=src_blob)

    increment_translation_completed_tasks(src_blob=src_blob)

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
