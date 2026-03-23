import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from kafka_pipeline.microservices import reconstruction_service as rs


class FakeSegment:
    def __init__(self, segment_id, speaker_id, start, end, gen_blob):
        self.segment_id = segment_id
        self.speaker_id = speaker_id
        self.start = start
        self.end = end
        self.gen_blob = gen_blob


def test_fit_segment_audio_to_timing_raises_for_invalid_timing():
    segment = FakeSegment(1, "speaker_0", 5.0, 4.0, "blob")

    with pytest.raises(ValueError, match="Invalid segment timing"):
        rs.fit_segment_audio_to_timing(
            segment=segment,
            input_audio_path="in.wav",
            output_audio_path="out.wav",
        )


def test_fit_segment_audio_to_timing_returns_none_for_zero_duration():
    segment = FakeSegment(1, "speaker_0", 2.0, 2.0, "blob")

    result = rs.fit_segment_audio_to_timing(
        segment=segment,
        input_audio_path="in.wav",
        output_audio_path="out.wav",
    )

    assert result is None


def test_fit_segment_audio_to_timing_raises_when_input_duration_invalid(monkeypatch):
    segment = FakeSegment(1, "speaker_0", 0.0, 2.0, "blob")
    monkeypatch.setattr(rs, "get_audio_duration", lambda path: 0.0)

    with pytest.raises(ValueError, match="Could not determine duration"):
        rs.fit_segment_audio_to_timing(
            segment=segment,
            input_audio_path="in.wav",
            output_audio_path="out.wav",
        )


def test_fit_segment_audio_to_timing_success(monkeypatch):
    segment = FakeSegment(1, "speaker_0", 0.0, 4.0, "blob")
    calls = []

    monkeypatch.setattr(rs, "get_audio_duration", lambda path: 2.0)

    def fake_run(cmd, check):
        calls.append((cmd, check))
        return None

    monkeypatch.setattr(rs.subprocess, "run", fake_run)

    result = rs.fit_segment_audio_to_timing(
        segment=segment,
        input_audio_path="input.wav",
        output_audio_path="output.wav",
    )

    assert result == "output.wav"
    assert len(calls) == 1
    cmd, check = calls[0]
    assert cmd[0] == "rubberband"
    assert cmd[1] == "-t"
    assert cmd[2] == "2.00000000"
    assert cmd[3] == "input.wav"
    assert cmd[4] == "output.wav"
    assert check is True


def test_fit_segment_audio_to_timing_missing_rubberband(monkeypatch):
    segment = FakeSegment(1, "speaker_0", 0.0, 2.0, "blob")

    monkeypatch.setattr(rs, "get_audio_duration", lambda path: 1.0)

    def fake_run(*args, **kwargs):
        raise FileNotFoundError("missing")

    monkeypatch.setattr(rs.subprocess, "run", fake_run)

    with pytest.raises(FileNotFoundError, match="Could not find `rubberband` on PATH"):
        rs.fit_segment_audio_to_timing(
            segment=segment,
            input_audio_path="input.wav",
            output_audio_path="output.wav",
        )


def test_get_audio_duration_success(monkeypatch):
    class Result:
        stdout = json.dumps({"format": {"duration": "12.34"}})

    monkeypatch.setattr(rs.subprocess, "run", lambda *args, **kwargs: Result())

    duration = rs.get_audio_duration("audio.wav")

    assert duration == 12.34


def test_get_audio_duration_missing_ffprobe(monkeypatch):
    def fake_run(*args, **kwargs):
        raise FileNotFoundError("missing")

    monkeypatch.setattr(rs.subprocess, "run", fake_run)

    with pytest.raises(FileNotFoundError, match="Could not find `ffprobe` on PATH"):
        rs.get_audio_duration("audio.wav")


def test_reconstruct_video_success(monkeypatch):
    downloads = []
    fitted_calls = []
    compose_calls = []
    merge_calls = []
    upload_calls = []

    class FakeTempDir:
        def __enter__(self):
            return "/tmp/reconstruct"

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(rs.tempfile, "TemporaryDirectory", FakeTempDir)

    def fake_download_blob_to_file(blob_location, output_path):
        downloads.append((blob_location, str(output_path)))
        return Path(output_path)

    monkeypatch.setattr(rs, "download_blob_to_file", fake_download_blob_to_file)
    monkeypatch.setattr(rs, "extract_audio", lambda input_mp4, output_wav: None)

    def fake_fit_segment_audio_to_timing(segment, input_audio_path, output_audio_path):
        fitted_calls.append((segment.segment_id, str(input_audio_path), str(output_audio_path)))
        return str(output_audio_path)

    monkeypatch.setattr(rs, "fit_segment_audio_to_timing", fake_fit_segment_audio_to_timing)

    def fake_compose_audio_with_mute_and_overlay(base_wav_path, segments_info, output_path):
        compose_calls.append(
            {
                "base_wav_path": str(base_wav_path),
                "segments_info": segments_info,
                "output_path": str(output_path),
            }
        )

    monkeypatch.setattr(rs, "compose_audio_with_mute_and_overlay", fake_compose_audio_with_mute_and_overlay)

    def fake_merge_audio(input_wav, input_mp4, output_mp4):
        merge_calls.append((str(input_wav), str(input_mp4), str(output_mp4)))

    monkeypatch.setattr(rs, "merge_audio", fake_merge_audio)
    monkeypatch.setattr(rs, "build_reconstruction_blob_name", lambda src_blob, ext: f"rebuilt{ext}")

    def fake_upload_file(local_path, blob_name):
        upload_calls.append((str(local_path), blob_name))

    monkeypatch.setattr(rs, "upload_file", fake_upload_file)

    segments = [
        FakeSegment(2, "speaker_1", 5.0, 7.0, "gen-2"),
        FakeSegment(1, "speaker_0", 1.0, 3.0, "gen-1"),
    ]

    result = rs.reconstruct_video(src_blob="source.mp4", segments=segments)

    assert result == "rebuilt.mp4"
    assert downloads == [
        ("source.mp4", "/tmp/reconstruct/original.mp4"),
        ("gen-1", "/tmp/reconstruct/wavs/raw_1.wav"),
        ("gen-2", "/tmp/reconstruct/wavs/raw_2.wav"),
    ]
    assert fitted_calls == [
        (1, "/tmp/reconstruct/wavs/raw_1.wav", "/tmp/reconstruct/wavs/fitted_1.wav"),
        (2, "/tmp/reconstruct/wavs/raw_2.wav", "/tmp/reconstruct/wavs/fitted_2.wav"),
    ]
    assert len(compose_calls) == 1
    assert compose_calls[0]["base_wav_path"] == "/tmp/reconstruct/original.wav"
    assert compose_calls[0]["output_path"] == "/tmp/reconstruct/composed.wav"
    assert compose_calls[0]["segments_info"] == [
        {
            "path": "/tmp/reconstruct/wavs/fitted_1.wav",
            "start": 1.0,
            "end": 3.0,
        },
        {
            "path": "/tmp/reconstruct/wavs/fitted_2.wav",
            "start": 5.0,
            "end": 7.0,
        },
    ]
    assert merge_calls == [
        ("/tmp/reconstruct/composed.wav", "/tmp/reconstruct/original.mp4", "/tmp/reconstruct/output.mp4")
    ]
    assert upload_calls == [
        ("/tmp/reconstruct/output.mp4", "rebuilt.mp4")
    ]


def test_reconstruct_video_skips_failed_and_non_positive_segments(monkeypatch, capsys):
    class FakeTempDir:
        def __enter__(self):
            return "/tmp/reconstruct"

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(rs.tempfile, "TemporaryDirectory", FakeTempDir)
    monkeypatch.setattr(rs, "download_blob_to_file", lambda blob_location, output_path: Path(output_path))
    monkeypatch.setattr(rs, "extract_audio", lambda input_mp4, output_wav: None)

    def fake_fit_segment_audio_to_timing(segment, input_audio_path, output_audio_path):
        if segment.segment_id == 1:
            raise RuntimeError("bad audio")
        if segment.segment_id == 2:
            return None
        return str(output_audio_path)

    monkeypatch.setattr(rs, "fit_segment_audio_to_timing", fake_fit_segment_audio_to_timing)

    compose_args = []
    monkeypatch.setattr(
        rs,
        "compose_audio_with_mute_and_overlay",
        lambda base_wav_path, segments_info, output_path: compose_args.append(segments_info),
    )
    monkeypatch.setattr(rs, "merge_audio", lambda input_wav, input_mp4, output_mp4: None)
    monkeypatch.setattr(rs, "build_reconstruction_blob_name", lambda src_blob, ext: f"out{ext}")
    monkeypatch.setattr(rs, "upload_file", lambda local_path, blob_name: None)

    segments = [
        FakeSegment(1, "speaker_0", 0.0, 1.0, "gen-1"),
        FakeSegment(2, "speaker_1", 1.0, 1.0, "gen-2"),
        FakeSegment(3, "speaker_2", 2.0, 3.0, "gen-3"),
    ]

    result = rs.reconstruct_video(src_blob="source.mp4", segments=segments)

    assert result == "out.mp4"
    assert compose_args == [[
        {
            "path": "/tmp/reconstruct/wavs/fitted_3.wav",
            "start": 2.0,
            "end": 3.0,
        }
    ]]

    out = capsys.readouterr().out
    assert "Skipping segment segment_id=1 start=0.0 end=1.0 reason=bad audio" in out
    assert "Skipping segment segment_id=2 start=1.0 end=1.0 reason=non-positive_duration" in out


def test_handler_invalid_payload(monkeypatch, capsys):
    def fake_validate(payload):
        raise rs.PayloadValidationError("bad payload")

    monkeypatch.setattr(rs, "validate_reconstruct_payload", fake_validate)

    service = SimpleNamespace(service_name="svc")
    context = SimpleNamespace(offset=9)

    rs.handler({}, context, service)

    out = capsys.readouterr().out
    assert "Invalid payload at offset=9: bad payload" in out


def test_handler_no_segments(monkeypatch, capsys):
    monkeypatch.setattr(rs, "validate_reconstruct_payload", lambda payload: None)
    monkeypatch.setattr(rs, "get_segments_for_src_blob", lambda src_blob: [])

    service = SimpleNamespace(service_name="svc")
    context = SimpleNamespace(offset=10)

    rs.handler({"src_blob": "blob-a"}, context, service)

    out = capsys.readouterr().out
    assert "No segments found for src_blob=blob-a" in out


def test_handler_missing_generated_audio(monkeypatch, capsys):
    monkeypatch.setattr(rs, "validate_reconstruct_payload", lambda payload: None)
    monkeypatch.setattr(
        rs,
        "get_segments_for_src_blob",
        lambda src_blob: [
            FakeSegment(1, "speaker_0", 0.0, 1.0, "audio-1"),
            FakeSegment(2, "speaker_1", 1.0, 2.0, None),
        ],
    )

    service = SimpleNamespace(service_name="svc")
    context = SimpleNamespace(offset=11)

    rs.handler({"src_blob": "blob-b"}, context, service)

    out = capsys.readouterr().out
    assert "Missing generated audio for src_blob=blob-b segment_ids=[2]" in out


def test_handler_success_updates_db(monkeypatch, capsys):
    monkeypatch.setattr(rs, "validate_reconstruct_payload", lambda payload: None)
    monkeypatch.setattr(
        rs,
        "get_segments_for_src_blob",
        lambda src_blob: [FakeSegment(1, "speaker_0", 0.0, 1.0, "audio-1")],
    )
    monkeypatch.setattr(rs, "reconstruct_video", lambda src_blob, segments: "completed.mp4")

    set_calls = []
    increment_calls = []

    monkeypatch.setattr(
        rs,
        "set_video_completed_blob",
        lambda src_blob, completed_blob: set_calls.append((src_blob, completed_blob)) or {"ok": True},
    )
    monkeypatch.setattr(
        rs,
        "increment_reconstruction_completed_tasks",
        lambda src_blob: increment_calls.append(src_blob),
    )

    service = SimpleNamespace(service_name="svc")
    context = SimpleNamespace(offset=12)

    rs.handler({"src_blob": "blob-c"}, context, service)

    assert set_calls == [("blob-c", "completed.mp4")]
    assert increment_calls == ["blob-c"]

    out = capsys.readouterr().out
    assert "Reconstructed src_blob=blob-c with 1 segment(s) output=completed.mp4" in out


def test_handler_success_when_db_update_fails(monkeypatch, capsys):
    monkeypatch.setattr(rs, "validate_reconstruct_payload", lambda payload: None)
    monkeypatch.setattr(
        rs,
        "get_segments_for_src_blob",
        lambda src_blob: [FakeSegment(1, "speaker_0", 0.0, 1.0, "audio-1")],
    )
    monkeypatch.setattr(rs, "reconstruct_video", lambda src_blob, segments: "completed.mp4")
    monkeypatch.setattr(rs, "set_video_completed_blob", lambda src_blob, completed_blob: None)

    increment_calls = []
    monkeypatch.setattr(
        rs,
        "increment_reconstruction_completed_tasks",
        lambda src_blob: increment_calls.append(src_blob),
    )

    service = SimpleNamespace(service_name="svc")
    context = SimpleNamespace(offset=13)

    rs.handler({"src_blob": "blob-d"}, context, service)

    assert increment_calls == []

    out = capsys.readouterr().out
    assert "Could not update completed_blob for src_blob=blob-d" in out
    assert "Reconstructed src_blob=blob-d with 1 segment(s) output=completed.mp4" in out


def test_build_parser_defaults(monkeypatch):
    monkeypatch.setattr(rs.time, "time", lambda: 1234567890)

    parser = rs.build_parser()
    args = parser.parse_args([])

    assert args.group_id == "reconstruct-1234567890"
    assert args.bootstrap_server is None
    assert args.from_beginning is False


def test_build_parser_from_beginning_flag():
    parser = rs.build_parser()
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

    monkeypatch.setattr(rs, "build_parser", lambda: SimpleNamespace(parse_args=lambda: Args))
    monkeypatch.setattr(rs, "KafkaMicroservice", FakeService)
    monkeypatch.setattr(rs, "TOPIC_RECONSTRUCT_VIDEO", "reconstruct_video")

    rs.main()

    assert len(run_calls) == 1
    kwargs, handler = run_calls[0]
    assert kwargs == {
        "service_name": "reconstruction-service",
        "input_topic": "reconstruct_video",
        "group_id": "group-1",
        "bootstrap_server": "broker:9092",
        "from_beginning": True,
    }
    assert handler is rs.handler