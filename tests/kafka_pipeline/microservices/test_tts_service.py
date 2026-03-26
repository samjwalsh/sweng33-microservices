import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from kafka_pipeline.microservices import tts_service as tts


def valid_segment(**overrides):
    segment = {
        "segment_id": 1,
        "speaker_id": "speaker_0",
        "start": 0.0,
        "end": 2.0,
        "text": "hello world",
    }
    segment.update(overrides)
    return segment


def valid_payload(**overrides):
    payload = {
        "src_blob": "video.mp4",
        "speaker_id": "speaker_0",
        "segments": [valid_segment()],
    }
    payload.update(overrides)
    return payload


def test_stable_id_is_deterministic():
    a = tts._stable_id("abc")
    b = tts._stable_id("abc")
    c = tts._stable_id("xyz")

    assert a == b
    assert a != c
    assert len(a) == 16


def test_stable_id_respects_length():
    result = tts._stable_id("abc", length=8)
    assert len(result) == 8


def test_select_voice_clone_training_segments_prefers_non_empty_and_longest():
    segments = [
        valid_segment(segment_id=1, start=0.0, end=1.0, text="hi"),
        valid_segment(segment_id=2, start=0.0, end=5.0, text="long"),
        valid_segment(segment_id=3, start=0.0, end=3.0, text="medium"),
        valid_segment(segment_id=4, start=0.0, end=10.0, text="   "),
    ]

    result = tts.select_voice_clone_training_segments(segments, max_training_segments=2)

    assert [segment["segment_id"] for segment in result] == [2, 3]


def test_select_voice_clone_training_segments_falls_back_to_all_segments_when_all_empty():
    segments = [
        valid_segment(segment_id=1, start=0.0, end=1.0, text=""),
        valid_segment(segment_id=2, start=0.0, end=3.0, text="   "),
        valid_segment(segment_id=3, start=0.0, end=2.0, text=""),
    ]

    result = tts.select_voice_clone_training_segments(segments, max_training_segments=2)

    assert [segment["segment_id"] for segment in result] == [2, 3]


def test_prepare_voice_clone_training_data_success(monkeypatch):
    downloaded = []
    uploaded = []
    subprocess_calls = []

    class FakePathObj:
        def __init__(self, value):
            self.value = str(value)

        def __str__(self):
            return self.value

        def exists(self):
            return True

        def unlink(self):
            return None

    monkeypatch.setattr(tts, "_stable_id", lambda value, length=16: "abc123def4567890")
    monkeypatch.setattr(tts, "Path", FakePathObj)
    monkeypatch.setattr(
        tts,
        "download_blob_to_file",
        lambda *, blob_location, output_path: downloaded.append((blob_location, str(output_path))),
    )

    def fake_run(cmd, check, capture_output):
        subprocess_calls.append(cmd)
        return None

    monkeypatch.setattr(tts.subprocess, "run", fake_run)
    monkeypatch.setattr(
        tts,
        "upload_file",
        lambda **kwargs: uploaded.append(kwargs) or "voice_cloning/training_abc123def4567890.wav",
    )

    result = tts.prepare_voice_clone_training_data(
        src_blob="video.mp4",
        speaker_id="speaker_0",
        training_segments=[
            {"start": 1.0, "end": 2.0},
            {"start": 4.0, "end": 6.0},
        ],
    )

    assert result == "voice_cloning/training_abc123def4567890.wav"
    assert downloaded == [("video.mp4", "/tmp/src_abc123def4567890.mp4")]
    assert subprocess_calls
    assert "aselect='between(t,1.0,2.0)+between(t,4.0,6.0)',asetpts=N/SR/TB" in subprocess_calls[0]
    assert uploaded == [
        {
            "local_path": "/tmp/training_abc123def4567890.wav",
            "folder": "voice_cloning",
            "blob_name": "training_abc123def4567890.wav",
            "overwrite": True,
        }
    ]


def test_prepare_voice_clone_training_data_raises_for_no_segments(monkeypatch):
    class FakePathObj:
        def __init__(self, value):
            self.value = str(value)

        def __str__(self):
            return self.value

        def exists(self):
            return False

        def unlink(self):
            return None

    monkeypatch.setattr(tts, "_stable_id", lambda value, length=16: "abc123")
    monkeypatch.setattr(tts, "Path", FakePathObj)
    monkeypatch.setattr(tts, "download_blob_to_file", lambda **kwargs: None)

    with pytest.raises(ValueError, match="No training segments provided"):
        tts.prepare_voice_clone_training_data(
            src_blob="video.mp4",
            speaker_id="speaker_0",
            training_segments=[],
        )


def test_prepare_voice_clone_training_data_reraises_ffmpeg_error(monkeypatch):
    class FakePathObj:
        def __init__(self, value):
            self.value = str(value)

        def __str__(self):
            return self.value

        def exists(self):
            return False

        def unlink(self):
            return None

    monkeypatch.setattr(tts, "_stable_id", lambda value, length=16: "abc123")
    monkeypatch.setattr(tts, "Path", FakePathObj)
    monkeypatch.setattr(tts, "download_blob_to_file", lambda **kwargs: None)

    def fake_run(*args, **kwargs):
        raise subprocess.CalledProcessError(returncode=1, cmd=["ffmpeg"], stderr=b"boom")

    monkeypatch.setattr(tts.subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError):
        tts.prepare_voice_clone_training_data(
            src_blob="video.mp4",
            speaker_id="speaker_0",
            training_segments=[{"start": 0.0, "end": 1.0}],
        )


def test_clone_voice_once_loads_local_file_when_present(monkeypatch, tmp_path):
    local_file = tmp_path / "train.wav"
    local_file.write_bytes(b"audio")

    captured = {}

    def fake_clone_voice_from_refs(voice_name, training_audio_refs, load_audio_bytes):
        captured["voice_name"] = voice_name
        captured["training_audio_refs"] = training_audio_refs
        captured["loaded"] = load_audio_bytes(str(local_file))
        return "voice-123"

    monkeypatch.setattr(tts, "clone_voice_from_refs", fake_clone_voice_from_refs)
    monkeypatch.setattr(tts, "_stable_id", lambda value, length=20: "stablevoiceid12345678")

    result = tts.clone_voice_once(
        src_blob="video.mp4",
        speaker_id="speaker_0",
        training_audio_refs=str(local_file),
    )

    assert result == "voice-123"
    assert captured["voice_name"] == "clone_stablevoiceid12345678"
    assert captured["training_audio_refs"] == [str(local_file)]
    assert captured["loaded"] == b"audio"


def test_clone_voice_once_downloads_blob_when_file_missing(monkeypatch):
    captured = {}

    def fake_clone_voice_from_refs(voice_name, training_audio_refs, load_audio_bytes):
        captured["loaded"] = load_audio_bytes("remote/blob.wav")
        return "voice-123"

    monkeypatch.setattr(tts, "clone_voice_from_refs", fake_clone_voice_from_refs)
    monkeypatch.setattr(tts, "download_blob", lambda ref: b"downloaded-audio")
    monkeypatch.setattr(tts, "_stable_id", lambda value, length=20: "stablevoiceid12345678")

    result = tts.clone_voice_once(
        src_blob="video.mp4",
        speaker_id="speaker_0",
        training_audio_refs="remote/blob.wav",
    )

    assert result == "voice-123"
    assert captured["loaded"] == b"downloaded-audio"


def test_synthesize_segment_audio(monkeypatch):
    class FakeTempDir:
        def __enter__(self):
            return "/tmp/fake-tts"

        def __exit__(self, exc_type, exc, tb):
            return False

    calls = {"generate": [], "save": [], "convert": [], "upload": []}

    monkeypatch.setattr(tts, "TemporaryDirectory", FakeTempDir)
    monkeypatch.setattr(
        tts,
        "generate_tts_audio",
        lambda **kwargs: calls["generate"].append(kwargs) or "audio-stream",
    )
    monkeypatch.setattr(
        tts,
        "save_audio_stream",
        lambda audio, path: calls["save"].append((audio, str(path))),
    )
    monkeypatch.setattr(
        tts,
        "convert_mp3_to_wav",
        lambda *, mp3_path, wav_path: calls["convert"].append((str(mp3_path), str(wav_path))),
    )
    monkeypatch.setattr(
        tts,
        "upload_file",
        lambda **kwargs: calls["upload"].append(kwargs) or "generated/segments/blob.wav",
    )

    result = tts.synthesize_segment_audio(
        text="hello",
        voice_profile_id="voice-1",
    )

    assert result == "generated/segments/blob.wav"
    assert calls["generate"] == [{"voice_id": "voice-1", "text": "hello"}]
    assert calls["save"] == [("audio-stream", "/tmp/fake-tts/segment.mp3")]
    assert calls["convert"] == [("/tmp/fake-tts/segment.mp3", "/tmp/fake-tts/segment.wav")]
    assert calls["upload"] == [
        {
            "local_path": Path("/tmp/fake-tts/segment.wav"),
            "folder": "generated/segments",
            "overwrite": True,
        }
    ]


def test_handler_invalid_payload(monkeypatch, capsys):
    monkeypatch.setattr(
        tts,
        "validate_tts_payload",
        lambda payload: (_ for _ in ()).throw(tts.PayloadValidationError("bad payload")),
    )

    service = SimpleNamespace(service_name="svc", publish=lambda **kwargs: None)
    context = SimpleNamespace(offset=7)

    tts.handler(valid_payload(), context, service)

    out = capsys.readouterr().out
    assert "Invalid payload at offset=7: bad payload" in out


def test_handler_processes_segments_and_publishes_reconstruct(monkeypatch, capsys):
    monkeypatch.setattr(tts, "validate_tts_payload", lambda payload: None)
    monkeypatch.setattr(
        tts,
        "select_voice_clone_training_segments",
        lambda segments: [segments[0]],
    )
    monkeypatch.setattr(
        tts,
        "prepare_voice_clone_training_data",
        lambda **kwargs: "training.wav",
    )
    monkeypatch.setattr(
        tts,
        "clone_voice_once",
        lambda **kwargs: "voice-123",
    )

    synth_calls = []
    monkeypatch.setattr(
        tts,
        "synthesize_segment_audio",
        lambda **kwargs: synth_calls.append(kwargs) or f"gen-{kwargs['text']}.wav",
    )

    set_calls = []
    monkeypatch.setattr(
        tts,
        "set_tts_generated_blob",
        lambda **kwargs: set_calls.append(kwargs),
    )

    delete_calls = []
    monkeypatch.setattr(
        tts,
        "delete_voice",
        lambda **kwargs: delete_calls.append(kwargs),
    )

    completed_calls = []
    monkeypatch.setattr(
        tts,
        "increment_tts_completed_tasks",
        lambda **kwargs: completed_calls.append(kwargs),
    )

    monkeypatch.setattr(tts, "are_all_segments_generated", lambda src_blob: True)
    monkeypatch.setattr(tts, "key_by_src_blob", lambda src_blob: f"key:{src_blob}")
    monkeypatch.setattr(tts, "TOPIC_RECONSTRUCT_VIDEO", "reconstruct_video")

    reconstruction_calls = []
    monkeypatch.setattr(
        tts,
        "increment_reconstruction_total_tasks",
        lambda **kwargs: reconstruction_calls.append(kwargs),
    )

    published = []
    service = SimpleNamespace(
        service_name="svc",
        publish=lambda **kwargs: published.append(kwargs),
    )
    context = SimpleNamespace(offset=8)

    payload = {
        "src_blob": "video.mp4",
        "speaker_id": "speaker_0",
        "segments": [
            valid_segment(segment_id=1, text="hello"),
            valid_segment(segment_id=2, text="world"),
        ],
    }

    tts.handler(payload, context, service)

    assert synth_calls == [
        {"text": "hello", "voice_profile_id": "voice-123"},
        {"text": "world", "voice_profile_id": "voice-123"},
    ]
    assert set_calls == [
        {"src_blob": "video.mp4", "segment_id": 1, "gen_blob": "gen-hello.wav"},
        {"src_blob": "video.mp4", "segment_id": 2, "gen_blob": "gen-world.wav"},
    ]
    assert delete_calls == [{"voice_id": "voice-123"}]
    assert completed_calls == [{"src_blob": "video.mp4"}]
    assert published == [
        {
            "topic": "reconstruct_video",
            "key": "key:video.mp4",
            "value": {"src_blob": "video.mp4"},
        }
    ]
    assert reconstruction_calls == [{"src_blob": "video.mp4"}]

    out = capsys.readouterr().out
    assert "Processed speaker=speaker_0 segments=2 src_blob=video.mp4" in out


def test_handler_does_not_publish_when_not_all_segments_generated(monkeypatch):
    monkeypatch.setattr(tts, "validate_tts_payload", lambda payload: None)
    monkeypatch.setattr(tts, "select_voice_clone_training_segments", lambda segments: segments[:1])
    monkeypatch.setattr(tts, "prepare_voice_clone_training_data", lambda **kwargs: "training.wav")
    monkeypatch.setattr(tts, "clone_voice_once", lambda **kwargs: "voice-123")
    monkeypatch.setattr(tts, "synthesize_segment_audio", lambda **kwargs: "gen.wav")
    monkeypatch.setattr(tts, "set_tts_generated_blob", lambda **kwargs: None)
    monkeypatch.setattr(tts, "delete_voice", lambda **kwargs: None)
    monkeypatch.setattr(tts, "increment_tts_completed_tasks", lambda **kwargs: None)
    monkeypatch.setattr(tts, "are_all_segments_generated", lambda src_blob: False)

    reconstruction_calls = []
    monkeypatch.setattr(
        tts,
        "increment_reconstruction_total_tasks",
        lambda **kwargs: reconstruction_calls.append(kwargs),
    )

    published = []
    service = SimpleNamespace(service_name="svc", publish=lambda **kwargs: published.append(kwargs))
    context = SimpleNamespace(offset=9)

    tts.handler(valid_payload(), context, service)

    assert published == []
    assert reconstruction_calls == []


def test_handler_deletes_voice_even_when_segment_synthesis_fails(monkeypatch):
    monkeypatch.setattr(tts, "validate_tts_payload", lambda payload: None)
    monkeypatch.setattr(tts, "select_voice_clone_training_segments", lambda segments: segments[:1])
    monkeypatch.setattr(tts, "prepare_voice_clone_training_data", lambda **kwargs: "training.wav")
    monkeypatch.setattr(tts, "clone_voice_once", lambda **kwargs: "voice-123")

    def bad_synthesize(**kwargs):
        raise RuntimeError("tts failed")

    monkeypatch.setattr(tts, "synthesize_segment_audio", bad_synthesize)

    delete_calls = []
    monkeypatch.setattr(tts, "delete_voice", lambda **kwargs: delete_calls.append(kwargs))

    service = SimpleNamespace(service_name="svc", publish=lambda **kwargs: None)
    context = SimpleNamespace(offset=10)

    with pytest.raises(RuntimeError, match="tts failed"):
        tts.handler(valid_payload(), context, service)

    assert delete_calls == [{"voice_id": "voice-123"}]


def test_handler_ignores_delete_voice_failure(monkeypatch, capsys):
    monkeypatch.setattr(tts, "validate_tts_payload", lambda payload: None)
    monkeypatch.setattr(tts, "select_voice_clone_training_segments", lambda segments: segments[:1])
    monkeypatch.setattr(tts, "prepare_voice_clone_training_data", lambda **kwargs: "training.wav")
    monkeypatch.setattr(tts, "clone_voice_once", lambda **kwargs: "voice-123")
    monkeypatch.setattr(tts, "synthesize_segment_audio", lambda **kwargs: "gen.wav")
    monkeypatch.setattr(tts, "set_tts_generated_blob", lambda **kwargs: None)
    monkeypatch.setattr(tts, "increment_tts_completed_tasks", lambda **kwargs: None)
    monkeypatch.setattr(tts, "are_all_segments_generated", lambda src_blob: False)

    def bad_delete(**kwargs):
        raise RuntimeError("delete failed")

    monkeypatch.setattr(tts, "delete_voice", bad_delete)

    service = SimpleNamespace(service_name="svc", publish=lambda **kwargs: None)
    context = SimpleNamespace(offset=11)

    tts.handler(valid_payload(), context, service)

    out = capsys.readouterr().out
    assert "Processed speaker=speaker_0 segments=1 src_blob=video.mp4" in out


def test_build_parser_defaults(monkeypatch):
    monkeypatch.setattr(tts.time, "time", lambda: 1234567890)

    parser = tts.build_parser()
    args = parser.parse_args([])

    assert args.group_id == "tts-1234567890"
    assert args.bootstrap_server is None
    assert args.from_beginning is False


def test_build_parser_from_beginning_flag():
    parser = tts.build_parser()
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

    monkeypatch.setattr(tts, "build_parser", lambda: SimpleNamespace(parse_args=lambda: Args))
    monkeypatch.setattr(tts, "KafkaMicroservice", FakeService)
    monkeypatch.setattr(tts, "TOPIC_TEXT_TO_SPEECH", "text_to_speech")

    tts.main()

    assert len(run_calls) == 1
    kwargs, handler = run_calls[0]
    assert kwargs == {
        "service_name": "tts-service",
        "input_topic": "text_to_speech",
        "group_id": "group-1",
        "bootstrap_server": "broker:9092",
        "from_beginning": True,
    }
    assert handler is tts.handlerpytest