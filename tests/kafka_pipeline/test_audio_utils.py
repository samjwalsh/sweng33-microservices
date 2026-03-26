import json
from pathlib import Path

import pytest

from kafka_pipeline import audio_utils as au


# -------------------------
# merge_audio / extract_audio
# -------------------------

def test_merge_audio_happy_path(tmp_path, monkeypatch):
    video = tmp_path / "in.mp4"
    audio = tmp_path / "in.wav"
    output = tmp_path / "out" / "out.mp4"

    video.write_bytes(b"x")
    audio.write_bytes(b"x")

    calls = []

    monkeypatch.setattr(au.subprocess, "run", lambda cmd, check, text: calls.append(cmd))

    au.merge_audio(video, audio, output)

    assert output.parent.exists()
    assert calls
    assert calls[0][0] == "ffmpeg"


def test_merge_audio_missing_video(tmp_path):
    audio = tmp_path / "in.wav"
    audio.write_bytes(b"x")

    with pytest.raises(FileNotFoundError):
        au.merge_audio("missing.mp4", audio, "out.mp4")


def test_merge_audio_missing_audio(tmp_path):
    video = tmp_path / "in.mp4"
    video.write_bytes(b"x")

    with pytest.raises(FileNotFoundError):
        au.merge_audio(video, "missing.wav", "out.mp4")


def test_extract_audio_happy_path(tmp_path, monkeypatch):
    video = tmp_path / "in.mp4"
    video.write_bytes(b"x")

    output = tmp_path / "out.wav"
    calls = []

    monkeypatch.setattr(au.subprocess, "run", lambda cmd, check, text: calls.append(cmd))

    au.extract_audio(video, output)

    assert output.parent.exists()
    assert calls[0][0] == "ffmpeg"


def test_extract_audio_missing_video():
    with pytest.raises(FileNotFoundError):
        au.extract_audio("missing.mp4", "out.wav")


# -------------------------
# stitch_audio_with_timestamps
# -------------------------

class FakeAudio:
    def __init__(self, duration=1000):
        self.duration = duration

    def __len__(self):
        return self.duration

    def __add__(self, other):
        return FakeAudio(self.duration + other.duration)

    def export(self, path, format):
        Path(path).write_bytes(b"audio")

    @staticmethod
    def silent(duration):
        return FakeAudio(duration)

    @staticmethod
    def from_file(path):
        return FakeAudio(500)


def test_stitch_audio_with_timestamps(monkeypatch, tmp_path):
    monkeypatch.setattr(au, "AudioSegment", FakeAudio)

    segments = [
        {"path": "a.wav", "start": 0.0},
        {"path": "b.wav", "start": 1.0},
    ]

    out = tmp_path / "out.wav"

    result = au.stitch_audio_with_timestamps(segments, out)

    assert result == out
    assert out.exists()


# -------------------------
# compose_audio_with_mute_and_overlay
# -------------------------

class FakeAudio2:
    def __init__(self, duration=1000):
        self.duration = duration
        self.frame_rate = 44100
        self.channels = 2
        self.sample_width = 2

    def __len__(self):
        return self.duration

    def __getitem__(self, item):
        return FakeAudio2(item.stop - item.start)

    def __add__(self, other):
        return FakeAudio2(self.duration + other.duration)

    def overlay(self, clip, position=0):
        return self

    def set_frame_rate(self, x):
        return self

    def set_channels(self, x):
        return self

    def set_sample_width(self, x):
        return self

    def export(self, path, format):
        Path(path).write_bytes(b"audio")

    @staticmethod
    def from_file(path):
        return FakeAudio2(1000)

    @staticmethod
    def silent(duration, frame_rate=None):
        return FakeAudio2(duration)


def test_compose_audio_happy_path(monkeypatch, tmp_path):
    monkeypatch.setattr(au, "AudioSegment", FakeAudio2)

    base = tmp_path / "base.wav"
    base.write_bytes(b"x")

    clip = tmp_path / "clip.wav"
    clip.write_bytes(b"x")

    output = tmp_path / "out.wav"

    segments = [{"path": clip, "start": 0.0, "end": 1.0}]

    result = au.compose_audio_with_mute_and_overlay(base, segments, output)

    assert result == str(output)
    assert output.exists()


def test_compose_audio_missing_base():
    with pytest.raises(FileNotFoundError):
        au.compose_audio_with_mute_and_overlay("missing.wav", [], "out.wav")


def test_compose_audio_skips_missing_clip(monkeypatch, tmp_path):
    monkeypatch.setattr(au, "AudioSegment", FakeAudio2)

    base = tmp_path / "base.wav"
    base.write_bytes(b"x")

    segments = [{"path": "missing.wav", "start": 0.0, "end": 1.0}]

    out = tmp_path / "out.wav"

    result = au.compose_audio_with_mute_and_overlay(base, segments, out)

    assert out.exists()
    assert result == str(out)


# -------------------------
# get_audio_snippet
# -------------------------

def test_get_audio_snippet(monkeypatch, tmp_path):
    monkeypatch.setattr(au, "AudioSegment", FakeAudio2)

    input_wav = tmp_path / "in.wav"
    input_wav.write_bytes(b"x")

    output = tmp_path / "out.wav"

    result = au.get_audio_snippet(input_wav, 0, 1, output)

    assert result == output
    assert output.exists()


# -------------------------
# stretch_audio_to_time
# -------------------------

def test_stretch_audio_success(monkeypatch, tmp_path):
    input_file = tmp_path / "in.wav"
    input_file.write_bytes(b"x")

    monkeypatch.setattr(au, "ffprobe_duration", lambda path: 2.0)

    calls = []

    monkeypatch.setattr(au.subprocess, "run", lambda cmd, check: calls.append(cmd))

    result = au.stretch_audio_to_time(input_file, 4.0)

    assert "rubberband" in calls[0][0]
    assert result.endswith(".wav")


def test_stretch_audio_invalid_target(tmp_path):
    with pytest.raises(ValueError):
        au.stretch_audio_to_time(tmp_path / "a.wav", 0)


def test_stretch_audio_invalid_duration(monkeypatch, tmp_path):
    monkeypatch.setattr(au, "ffprobe_duration", lambda path: 0)

    with pytest.raises(ValueError):
        au.stretch_audio_to_time(tmp_path / "a.wav", 5)


def test_stretch_audio_missing_rubberband(monkeypatch, tmp_path):
    monkeypatch.setattr(au, "ffprobe_duration", lambda path: 1)

    def fake_run(*args, **kwargs):
        raise FileNotFoundError()

    monkeypatch.setattr(au.subprocess, "run", fake_run)

    with pytest.raises(FileNotFoundError, match="Could not find `rubberband`"):
        au.stretch_audio_to_time(tmp_path / "a.wav", 5)


# -------------------------
# ffprobe_duration
# -------------------------

def test_ffprobe_duration_success(monkeypatch):
    class Result:
        stdout = json.dumps({"format": {"duration": "3.5"}})

    monkeypatch.setattr(au.subprocess, "run", lambda *a, **k: Result())

    assert au.ffprobe_duration("file.wav") == 3.5


def test_ffprobe_duration_missing_ffprobe(monkeypatch):
    def fake_run(*args, **kwargs):
        raise FileNotFoundError()

    monkeypatch.setattr(au.subprocess, "run", fake_run)

    with pytest.raises(FileNotFoundError, match="Could not find `ffprobe`"):
        au.ffprobe_duration("file.wav")