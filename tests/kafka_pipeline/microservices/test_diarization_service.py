import subprocess
from types import SimpleNamespace

import pytest

from kafka_pipeline.microservices import diarization_service as ds


def test_text_for_time_range_matches_words_by_midpoint():
    words = [
        SimpleNamespace(word="hello", start=0.0, end=0.4),   # midpoint 0.2
        SimpleNamespace(word="world", start=0.6, end=1.0),   # midpoint 0.8
        SimpleNamespace(word="again", start=1.1, end=1.5),   # midpoint 1.3
    ]

    result = ds.text_for_time_range(words, start=0.0, end=1.0)

    assert result == "hello world"


def test_text_for_time_range_strips_whitespace():
    words = [
        SimpleNamespace(word=" hello ", start=0.0, end=0.4),
        SimpleNamespace(word=" world ", start=0.5, end=0.9),
    ]

    result = ds.text_for_time_range(words, start=0.0, end=1.0)

    assert result == "hello world"


def test_text_for_time_range_returns_empty_string_when_no_match():
    words = [
        SimpleNamespace(word="hello", start=2.0, end=2.4),
    ]

    result = ds.text_for_time_range(words, start=0.0, end=1.0)

    assert result == ""


def test_get_language_model_loads_once(monkeypatch):
    ds._LANGUAGE_MODEL = None
    loaded = []

    fake_model = object()

    class FakeWhisper:
        @staticmethod
        def load_model(name):
            loaded.append(name)
            return fake_model

    monkeypatch.setenv("LANGID_WHISPER_MODEL", "base")
    monkeypatch.setitem(__import__("sys").modules, "whisper", FakeWhisper)

    model1 = ds._get_language_model()
    model2 = ds._get_language_model()

    assert model1 is fake_model
    assert model2 is fake_model
    assert loaded == ["base"]


def test_detect_source_language_success(monkeypatch):
    ds._LANGUAGE_MODEL = None

    downloaded = []
    subprocess_calls = []
    unlinked = []

    class FakePathObj:
        def __init__(self, value):
            self.value = str(value)

        def __str__(self):
            return self.value

        def with_suffix(self, suffix):
            base = self.value.rsplit(".", 1)[0]
            return FakePathObj(base + suffix)

        def exists(self):
            return True

        def unlink(self):
            unlinked.append(self.value)

    class FakeWhisper:
        @staticmethod
        def load_audio(path):
            return f"audio:{path}"

        @staticmethod
        def pad_or_trim(audio):
            return f"trimmed:{audio}"

        @staticmethod
        def log_mel_spectrogram(audio):
            class FakeMel:
                def to(self, device):
                    return f"mel-on-{device}"
            return FakeMel()

    class FakeModel:
        device = "cpu"

        def detect_language(self, mel):
            return None, {"en": 0.2, "fr": 0.7, "de": 0.1}

    monkeypatch.setattr(ds, "Path", FakePathObj)
    monkeypatch.setattr(
        ds,
        "download_blob_to_file",
        lambda blob_location, output_path: downloaded.append((blob_location, str(output_path))),
    )

    def fake_run(cmd, check, capture_output):
        subprocess_calls.append(cmd)
        return None

    monkeypatch.setattr(ds.subprocess, "run", fake_run)
    monkeypatch.setitem(__import__("sys").modules, "whisper", FakeWhisper)
    monkeypatch.setattr(ds, "_get_language_model", lambda: FakeModel())

    result = ds.detect_source_language("blob://video.mp4")

    assert result == "fr"
    assert downloaded == [("blob://video.mp4", "/tmp/video_" + str(hash("blob://video.mp4")) + ".mp4")]
    assert subprocess_calls
    assert any(path.endswith(".mp4") for path in unlinked)
    assert any(path.endswith(".wav") for path in unlinked)


def test_detect_source_language_returns_und_on_error(monkeypatch):
    ds._LANGUAGE_MODEL = None

    class FakePathObj:
        def __init__(self, value):
            self.value = str(value)

        def __str__(self):
            return self.value

        def with_suffix(self, suffix):
            base = self.value.rsplit(".", 1)[0]
            return FakePathObj(base + suffix)

        def exists(self):
            return False

        def unlink(self):
            raise AssertionError("unlink should not be called")

    monkeypatch.setattr(ds, "Path", FakePathObj)
    monkeypatch.setattr(
        ds,
        "download_blob_to_file",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("download failed")),
    )

    result = ds.detect_source_language("blob://video.mp4")

    assert result == "und"


def test_diarize_and_transcribe_success(monkeypatch):
    downloaded = []
    subprocess_calls = []

    class FakeTempDir:
        def __enter__(self):
            return "/tmp/fake-dir"

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeTranscription:
        def __init__(self):
            self.words = [
                SimpleNamespace(word="hello", start=0.0, end=0.4),
                SimpleNamespace(word="there", start=0.5, end=0.9),
                SimpleNamespace(word="general", start=1.2, end=1.6),
                SimpleNamespace(word="kenobi", start=1.7, end=1.9),
            ]

    monkeypatch.setattr(ds, "TemporaryDirectory", FakeTempDir)
    monkeypatch.setattr(
        ds,
        "download_blob_to_file",
        lambda blob_location, output_path: downloaded.append((blob_location, str(output_path))),
    )

    def fake_run(cmd, check, capture_output, text):
        subprocess_calls.append(cmd)
        return None

    monkeypatch.setattr(ds.subprocess, "run", fake_run)
    monkeypatch.setattr(
        ds,
        "diarize",
        lambda wav_path: [
            {"speaker": "speaker_1", "start": 1.0, "end": 2.0},
            {"speaker": "speaker_0", "start": 0.0, "end": 1.0},
        ],
    )
    monkeypatch.setattr(ds, "transcribe_with_timestamps", lambda **kwargs: FakeTranscription())
    monkeypatch.setenv("TRANSCRIBER_MODEL", "tiny")
    monkeypatch.setenv("TRANSCRIBER_CHUNK_LENGTH_S", "30")
    monkeypatch.setenv("TRANSCRIBER_STRIDE_LENGTH_S", "5")

    segments = ds.diarize_and_transcribe(src_blob="movie.mp4", src_lang="en")

    assert downloaded == [("movie.mp4", "/tmp/fake-dir/input.mp4")]
    assert subprocess_calls
    assert segments == [
        {
            "segment_id": 0,
            "speaker_id": "speaker_0",
            "start": 0.0,
            "end": 1.0,
            "text": "hello there",
        },
        {
            "segment_id": 1,
            "speaker_id": "speaker_1",
            "start": 1.0,
            "end": 2.0,
            "text": "general kenobi",
        },
    ]


def test_diarize_and_transcribe_ffmpeg_failure(monkeypatch):
    class FakeTempDir:
        def __enter__(self):
            return "/tmp/fake-dir"

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(ds, "TemporaryDirectory", FakeTempDir)
    monkeypatch.setattr(ds, "download_blob_to_file", lambda **kwargs: None)

    def fake_run(*args, **kwargs):
        raise subprocess.CalledProcessError(
            returncode=1,
            cmd=["ffmpeg"],
            stderr="boom",
        )

    monkeypatch.setattr(ds.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="Failed to convert source media to WAV"):
        ds.diarize_and_transcribe(src_blob="movie.mp4", src_lang="en")


def test_handler_invalid_payload(capsys, monkeypatch):
    monkeypatch.setattr(
        ds,
        "validate_ingest_payload",
        lambda payload: (_ for _ in ()).throw(ds.PayloadValidationError("bad payload")),
    )

    service = SimpleNamespace(service_name="svc", publish=lambda **kwargs: None)
    context = SimpleNamespace(offset=7)

    ds.handler({"bad": "data"}, context, service)

    out = capsys.readouterr().out
    assert "Invalid payload at offset=7: bad payload" in out


def test_handler_detects_language_when_missing(monkeypatch, capsys):
    monkeypatch.setattr(ds, "validate_ingest_payload", lambda payload: None)
    monkeypatch.setattr(ds, "detect_source_language", lambda src_blob: "es")
    monkeypatch.setattr(
        ds,
        "diarize_and_transcribe",
        lambda src_blob, src_lang: [
            {"segment_id": 0, "speaker_id": "speaker_0", "start": 0.0, "end": 1.0, "text": "hola"}
        ],
    )

    diarization_calls = []
    translation_calls = []

    monkeypatch.setattr(
        ds,
        "increment_diarization_completed_tasks",
        lambda src_blob: diarization_calls.append(src_blob),
    )
    monkeypatch.setattr(
        ds,
        "increment_translation_total_tasks",
        lambda src_blob: translation_calls.append(src_blob),
    )
    monkeypatch.setattr(ds, "key_by_src_blob", lambda src_blob: f"key:{src_blob}")
    monkeypatch.setattr(ds, "TOPIC_TRANSLATE_SEGMENTS", "translate_segments")

    published = []

    service = SimpleNamespace(
        service_name="svc",
        publish=lambda **kwargs: published.append(kwargs),
    )
    context = SimpleNamespace(offset=3)

    ds.handler(
        {"src_blob": "blob1", "src_lang": None, "dest_lang": "fr"},
        context,
        service,
    )

    assert published == [
        {
            "topic": "translate_segments",
            "key": "key:blob1",
            "value": {
                "src_blob": "blob1",
                "src_lang": "es",
                "dest_lang": "fr",
                "segments": [
                    {
                        "segment_id": 0,
                        "speaker_id": "speaker_0",
                        "start": 0.0,
                        "end": 1.0,
                        "text": "hola",
                    }
                ],
            },
        }
    ]
    assert diarization_calls == ["blob1"]
    assert translation_calls == ["blob1"]

    out = capsys.readouterr().out
    assert "Published 1 segments for src_blob=blob1 from offset=3" in out


def test_handler_uses_existing_src_lang(monkeypatch):
    monkeypatch.setattr(ds, "validate_ingest_payload", lambda payload: None)

    detect_calls = []
    monkeypatch.setattr(ds, "detect_source_language", lambda src_blob: detect_calls.append(src_blob) or "es")
    monkeypatch.setattr(
        ds,
        "diarize_and_transcribe",
        lambda src_blob, src_lang: [
            {"segment_id": 0, "speaker_id": "speaker_0", "start": 0.0, "end": 1.0, "text": "hello"}
        ],
    )
    monkeypatch.setattr(ds, "increment_diarization_completed_tasks", lambda src_blob: None)
    monkeypatch.setattr(ds, "increment_translation_total_tasks", lambda src_blob: None)
    monkeypatch.setattr(ds, "key_by_src_blob", lambda src_blob: src_blob)
    monkeypatch.setattr(ds, "TOPIC_TRANSLATE_SEGMENTS", "translate_segments")

    published = []
    service = SimpleNamespace(service_name="svc", publish=lambda **kwargs: published.append(kwargs))
    context = SimpleNamespace(offset=5)

    ds.handler(
        {"src_blob": "blob2", "src_lang": "en", "dest_lang": "fr"},
        context,
        service,
    )

    assert detect_calls == []
    assert published[0]["value"]["src_lang"] == "en"


def test_handler_no_segments(capsys, monkeypatch):
    monkeypatch.setattr(ds, "validate_ingest_payload", lambda payload: None)
    monkeypatch.setattr(ds, "diarize_and_transcribe", lambda src_blob, src_lang: [])

    detect_calls = []
    monkeypatch.setattr(ds, "detect_source_language", lambda src_blob: detect_calls.append(src_blob) or "en")

    published = []
    service = SimpleNamespace(service_name="svc", publish=lambda **kwargs: published.append(kwargs))
    context = SimpleNamespace(offset=11)

    ds.handler(
        {"src_blob": "blob3", "src_lang": None, "dest_lang": "fr"},
        context,
        service,
    )

    assert published == []
    out = capsys.readouterr().out
    assert "No segments produced for src_blob=blob3" in out


def test_build_parser_defaults(monkeypatch):
    monkeypatch.setattr(ds.time, "time", lambda: 1234567890)

    parser = ds.build_parser()
    args = parser.parse_args([])

    assert args.group_id == "diarization-1234567890"
    assert args.bootstrap_server is None
    assert args.from_beginning is False


def test_build_parser_from_beginning_flag():
    parser = ds.build_parser()
    args = parser.parse_args(["--from-beginning"])

    assert args.from_beginning is True


def test_main_builds_service_and_runs(monkeypatch):
    class Args:
        group_id = "group-1"
        bootstrap_server = "broker:9092"
        from_beginning = True

    run_calls = []

    class FakeService:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def run(self, handler):
            run_calls.append((self.kwargs, handler))

    monkeypatch.setattr(ds, "build_parser", lambda: SimpleNamespace(parse_args=lambda: Args))
    monkeypatch.setattr(ds, "KafkaMicroservice", FakeService)
    monkeypatch.setattr(ds, "TOPIC_INGEST", "ingest")

    ds.main()

    assert run_calls
    kwargs, handler = run_calls[0]
    assert kwargs == {
        "service_name": "diarization-service",
        "input_topic": "ingest",
        "group_id": "group-1",
        "bootstrap_server": "broker:9092",
        "from_beginning": True,
    }
    assert handler is ds.handler