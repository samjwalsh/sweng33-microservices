from typing import Any


class PayloadValidationError(ValueError):
    pass


def _require_str(payload: dict[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise PayloadValidationError(f"{field} must be a non-empty string")
    return value


def _require_list(payload: dict[str, Any], field: str) -> list[Any]:
    value = payload.get(field)
    if not isinstance(value, list):
        raise PayloadValidationError(f"{field} must be a list")
    return value


def validate_ingest_payload(payload: dict[str, Any]) -> None:
    _require_str(payload, "src_blob")
    _require_str(payload, "dest_lang")

    src_lang = payload.get("src_lang")
    if src_lang is not None and not isinstance(src_lang, str):
        raise PayloadValidationError("src_lang must be null or string")


def validate_segment(segment: dict[str, Any]) -> None:
    if not isinstance(segment.get("segment_id"), int):
        raise PayloadValidationError("segment.segment_id must be int")
    _require_str(segment, "speaker_id")

    start = segment.get("start")
    end = segment.get("end")
    if not isinstance(start, (int, float)) or not isinstance(end, (int, float)):
        raise PayloadValidationError("segment.start and segment.end must be numbers")
    if float(end) < float(start):
        raise PayloadValidationError("segment.end must be >= segment.start")

    _require_str(segment, "text")


def validate_translate_payload(payload: dict[str, Any]) -> None:
    _require_str(payload, "src_blob")
    _require_str(payload, "src_lang")
    _require_str(payload, "dest_lang")

    segments = _require_list(payload, "segments")
    if not segments:
        raise PayloadValidationError("segments must contain at least one segment")
    for segment in segments:
        if not isinstance(segment, dict):
            raise PayloadValidationError("each segment must be an object")
        validate_segment(segment)


def validate_tts_payload(payload: dict[str, Any]) -> None:
    validate_translate_payload(payload)
    _require_str(payload, "speaker_id")

    segments = payload["segments"]
    for segment in segments:
        if segment.get("speaker_id") != payload["speaker_id"]:
            raise PayloadValidationError("all segments in a TTS message must share speaker_id")


def validate_reconstruct_payload(payload: dict[str, Any]) -> None:
    _require_str(payload, "src_blob")
