import pytest
from unittest.mock import Mock

from kafka_pipeline import microservices_job_creator as svc


# -------------------------
# build_segments
# -------------------------

def test_build_segments_basic():
    segments = svc.build_segments(
        speakers=2,
        segments_per_speaker=2,
        start_seconds=0.0,
        segment_duration=1.0,
        gap_seconds=0.5,
    )

    assert len(segments) == 4

    # Check first segment
    assert segments[0]["segment_id"] == 0
    assert segments[0]["speaker_id"] == "speaker_0"
    assert segments[0]["start"] == 0.0
    assert segments[0]["end"] == 1.0

    # Check ordering + continuity
    assert segments[1]["start"] > segments[0]["end"]


# -------------------------
# payload builders
# -------------------------

def test_build_ingest_payload_calls_validator(monkeypatch):
    mock = Mock()
    monkeypatch.setattr(svc, "validate_ingest_payload", mock)

    payload = svc.build_ingest_payload(
        src_blob="blob",
        src_lang="en",
        dest_lang="fr",
    )

    assert payload["src_blob"] == "blob"
    mock.assert_called_once_with(payload)


def test_build_translate_payload_calls_validator(monkeypatch):
    mock = Mock()
    monkeypatch.setattr(svc, "validate_translate_payload", mock)

    payload = svc.build_translate_payload(
        src_blob="blob",
        src_lang="en",
        dest_lang="fr",
        segments=[{"x": 1}],
    )

    assert "segments" in payload
    mock.assert_called_once()


def test_build_tts_payload_filters_by_speaker(monkeypatch):
    mock = Mock()
    monkeypatch.setattr(svc, "validate_tts_payload", mock)

    segments = [
        {"speaker_id": "speaker_0"},
        {"speaker_id": "speaker_1"},
    ]

    payload = svc.build_tts_payload(
        src_blob="blob",
        src_lang="en",
        dest_lang="fr",
        segments=segments,
        speaker_id="speaker_0",
    )

    assert len(payload["segments"]) == 1
    assert payload["segments"][0]["speaker_id"] == "speaker_0"
    mock.assert_called_once()


def test_build_reconstruct_payload_calls_validator(monkeypatch):
    mock = Mock()
    monkeypatch.setattr(svc, "validate_reconstruct_payload", mock)

    payload = svc.build_reconstruct_payload(src_blob="blob")

    assert payload == {"src_blob": "blob"}
    mock.assert_called_once()


# -------------------------
# publish
# -------------------------

def test_publish_calls_kafka(monkeypatch, capsys):
    mock = Mock(return_value="metadata-ok")
    monkeypatch.setattr(svc, "send_kafka_message", mock)

    svc.publish(
        topic="topic",
        key="key",
        payload={"x": 1},
        bootstrap_server="broker",
    )

    mock.assert_called_once_with(
        topic="topic",
        key="key",
        value={"x": 1},
        bootstrap_server="broker",
    )

    out = capsys.readouterr().out
    assert "Published topic: metadata-ok" in out


# -------------------------
# parser
# -------------------------

def test_build_parser_defaults(monkeypatch):
    monkeypatch.setattr(svc.time, "time", lambda: 123)

    parser = svc.build_parser()
    args = parser.parse_args([])

    assert args.target == "all"
    assert args.src_blob == "video-test-123"
    assert args.src_lang == "en"
    assert args.dest_lang == "fr"


# -------------------------
# main (light integration)
# -------------------------

def test_main_all_paths(monkeypatch):
    # mock dependencies
    monkeypatch.setattr(svc, "send_kafka_message", Mock(return_value="ok"))
    monkeypatch.setattr(svc, "validate_ingest_payload", lambda x: None)
    monkeypatch.setattr(svc, "validate_translate_payload", lambda x: None)
    monkeypatch.setattr(svc, "validate_tts_payload", lambda x: None)
    monkeypatch.setattr(svc, "validate_reconstruct_payload", lambda x: None)

    # mock topic + key funcs
    monkeypatch.setattr(svc, "TOPIC_INGEST", "ingest")
    monkeypatch.setattr(svc, "TOPIC_TRANSLATE_SEGMENTS", "translate")
    monkeypatch.setattr(svc, "TOPIC_TEXT_TO_SPEECH", "tts")
    monkeypatch.setattr(svc, "TOPIC_RECONSTRUCT_VIDEO", "reconstruct")

    monkeypatch.setattr(svc, "key_by_src_blob", lambda x: f"k-{x}")
    monkeypatch.setattr(svc, "key_by_src_blob_and_speaker", lambda x, y: f"k-{x}-{y}")

    # fake CLI args
    class Args:
        target = "all"
        src_blob = "blob"
        src_lang = "en"
        dest_lang = "fr"
        speakers = 1
        segments_per_speaker = 1
        tts_speaker_id = "speaker_0"
        start_seconds = 0.0
        segment_duration = 1.0
        gap_seconds = 0.1
        bootstrap_server = None

    monkeypatch.setattr(svc, "build_parser", lambda: Mock(parse_args=lambda: Args))

    svc.main()  # should not crash


def test_main_invalid_speakers(monkeypatch):
    class Args:
        speakers = 0
        segments_per_speaker = 1

    monkeypatch.setattr(
        svc,
        "build_parser",
        lambda: Mock(parse_args=lambda: Args),
    )

    with pytest.raises(ValueError):
        svc.main()


def test_main_invalid_tts_speaker(monkeypatch):
    monkeypatch.setattr(svc, "send_kafka_message", Mock())
    monkeypatch.setattr(svc, "validate_ingest_payload", lambda x: None)
    monkeypatch.setattr(svc, "validate_translate_payload", lambda x: None)
    monkeypatch.setattr(svc, "validate_tts_payload", lambda x: None)
    monkeypatch.setattr(svc, "validate_reconstruct_payload", lambda x: None)

    class Args:
        target = "tts"
        src_blob = "blob"
        src_lang = "en"
        dest_lang = "fr"
        speakers = 1
        segments_per_speaker = 1
        tts_speaker_id = "invalid"
        start_seconds = 0.0
        segment_duration = 1.0
        gap_seconds = 0.1
        bootstrap_server = None

    monkeypatch.setattr(svc, "build_parser", lambda: Mock(parse_args=lambda: Args))

    with pytest.raises(ValueError):
        svc.main()