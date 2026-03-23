import pytest

from kafka_pipeline import payload_validation as pv


def valid_segment(**overrides):
    segment = {
        "segment_id": 1,
        "speaker_id": "speaker_0",
        "start": 0.0,
        "end": 1.5,
        "text": "hello world",
    }
    segment.update(overrides)
    return segment


def valid_translate_payload(**overrides):
    payload = {
        "src_blob": "blob-1",
        "src_lang": "en",
        "dest_lang": "fr",
        "segments": [valid_segment()],
    }
    payload.update(overrides)
    return payload


def valid_tts_payload(**overrides):
    payload = {
        "src_blob": "blob-1",
        "src_lang": "en",
        "dest_lang": "fr",
        "speaker_id": "speaker_0",
        "segments": [valid_segment(speaker_id="speaker_0")],
    }
    payload.update(overrides)
    return payload


def test_require_str_accepts_non_empty_string():
    payload = {"field": "value"}
    assert pv._require_str(payload, "field") == "value"


@pytest.mark.parametrize("value", [None, "", "   ", 123, [], {}])
def test_require_str_rejects_invalid_values(value):
    with pytest.raises(pv.PayloadValidationError, match="field must be a non-empty string"):
        pv._require_str({"field": value}, "field")


def test_require_list_accepts_list():
    payload = {"items": [1, 2, 3]}
    assert pv._require_list(payload, "items") == [1, 2, 3]


@pytest.mark.parametrize("value", [None, "", "abc", 123, {}])
def test_require_list_rejects_invalid_values(value):
    with pytest.raises(pv.PayloadValidationError, match="items must be a list"):
        pv._require_list({"items": value}, "items")


def test_validate_ingest_payload_accepts_valid_payload():
    pv.validate_ingest_payload(
        {
            "src_blob": "blob-1",
            "src_lang": "en",
            "dest_lang": "fr",
        }
    )


def test_validate_ingest_payload_accepts_none_src_lang():
    pv.validate_ingest_payload(
        {
            "src_blob": "blob-1",
            "src_lang": None,
            "dest_lang": "fr",
        }
    )


def test_validate_ingest_payload_rejects_non_string_src_lang():
    with pytest.raises(pv.PayloadValidationError, match="src_lang must be null or string"):
        pv.validate_ingest_payload(
            {
                "src_blob": "blob-1",
                "src_lang": 123,
                "dest_lang": "fr",
            }
        )


def test_validate_segment_accepts_valid_segment():
    pv.validate_segment(valid_segment())


def test_validate_segment_requires_int_segment_id():
    with pytest.raises(pv.PayloadValidationError, match="segment.segment_id must be int"):
        pv.validate_segment(valid_segment(segment_id="1"))


def test_validate_segment_requires_speaker_id():
    with pytest.raises(pv.PayloadValidationError, match="speaker_id must be a non-empty string"):
        pv.validate_segment(valid_segment(speaker_id=""))


@pytest.mark.parametrize(
    "start,end",
    [
        ("0", 1.0),
        (0.0, "1"),
        (None, 1.0),
        (0.0, None),
    ],
)
def test_validate_segment_requires_numeric_start_end(start, end):
    with pytest.raises(
        pv.PayloadValidationError,
        match="segment.start and segment.end must be numbers",
    ):
        pv.validate_segment(valid_segment(start=start, end=end))


def test_validate_segment_requires_end_not_before_start():
    with pytest.raises(pv.PayloadValidationError, match="segment.end must be >= segment.start"):
        pv.validate_segment(valid_segment(start=5.0, end=4.0))


def test_validate_segment_requires_text():
    with pytest.raises(pv.PayloadValidationError, match="text must be a non-empty string"):
        pv.validate_segment(valid_segment(text=""))


def test_validate_translate_payload_accepts_valid_payload():
    pv.validate_translate_payload(valid_translate_payload())


def test_validate_translate_payload_requires_nonempty_segments():
    with pytest.raises(
        pv.PayloadValidationError,
        match="segments must contain at least one segment",
    ):
        pv.validate_translate_payload(valid_translate_payload(segments=[]))


def test_validate_translate_payload_requires_segments_list():
    with pytest.raises(pv.PayloadValidationError, match="segments must be a list"):
        pv.validate_translate_payload(valid_translate_payload(segments="not-a-list"))


def test_validate_translate_payload_requires_each_segment_to_be_object():
    with pytest.raises(pv.PayloadValidationError, match="each segment must be an object"):
        pv.validate_translate_payload(valid_translate_payload(segments=["bad-segment"]))


def test_validate_translate_payload_validates_nested_segments():
    with pytest.raises(pv.PayloadValidationError, match="segment.segment_id must be int"):
        pv.validate_translate_payload(valid_translate_payload(segments=[valid_segment(segment_id="x")]))


def test_validate_tts_payload_accepts_valid_payload():
    pv.validate_tts_payload(valid_tts_payload())


def test_validate_tts_payload_requires_speaker_id():
    with pytest.raises(pv.PayloadValidationError, match="speaker_id must be a non-empty string"):
        pv.validate_tts_payload(valid_tts_payload(speaker_id=""))


def test_validate_tts_payload_requires_all_segments_match_speaker():
    payload = valid_tts_payload(
        segments=[
            valid_segment(speaker_id="speaker_0"),
            valid_segment(segment_id=2, speaker_id="speaker_1"),
        ]
    )
    with pytest.raises(
        pv.PayloadValidationError,
        match="all segments in a TTS message must share speaker_id",
    ):
        pv.validate_tts_payload(payload)


def test_validate_reconstruct_payload_accepts_valid_payload():
    pv.validate_reconstruct_payload({"src_blob": "blob-1"})


def test_validate_reconstruct_payload_requires_src_blob():
    with pytest.raises(pv.PayloadValidationError, match="src_blob must be a non-empty string"):
        pv.validate_reconstruct_payload({"src_blob": ""})