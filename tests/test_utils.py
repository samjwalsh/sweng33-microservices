from pathlib import Path
from unittest.mock import patch

import pytest

from src.utils import extract_audio, merge_audio


@patch("app.utils.subprocess.run")
def test_extract_audio_invokes_ffmpeg_with_expected_flags(mock_run, tmp_path: Path):
    input_mp4 = tmp_path / "input.mp4"
    output_wav = tmp_path / "output.wav"
    input_mp4.touch()

    extract_audio(input_mp4, output_wav)

    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]

    assert cmd[0] == "ffmpeg"
    assert "-i" in cmd and str(input_mp4) in cmd
    assert "-vn" in cmd
    assert "pcm_s16le" in cmd
    assert str(output_wav) in cmd


@patch("app.utils.subprocess.run")
def test_merge_audio_invokes_ffmpeg_with_expected_flags(mock_run, tmp_path: Path):
    input_mp4 = tmp_path / "input.mp4"
    input_wav = tmp_path / "input.wav"
    output_mp4 = tmp_path / "output.mp4"
    input_mp4.touch()
    input_wav.touch()

    merge_audio(input_mp4, input_wav, output_mp4)

    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]

    assert cmd[0] == "ffmpeg"
    assert str(input_mp4) in cmd
    assert str(input_wav) in cmd
    assert "-c:v" in cmd and "copy" in cmd
    assert "-c:a" in cmd and "aac" in cmd
    assert "-shortest" in cmd
    assert str(output_mp4) in cmd


def test_extract_audio_raises_when_video_is_missing(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        extract_audio(tmp_path / "missing.mp4", tmp_path / "out.wav")


def test_merge_audio_raises_when_video_is_missing(tmp_path: Path):
    input_wav = tmp_path / "audio.wav"
    input_wav.touch()

    with pytest.raises(FileNotFoundError):
        merge_audio(tmp_path / "missing.mp4", input_wav, tmp_path / "out.mp4")


def test_merge_audio_raises_when_audio_is_missing(tmp_path: Path):
    input_mp4 = tmp_path / "video.mp4"
    input_mp4.touch()

    with pytest.raises(FileNotFoundError):
        merge_audio(input_mp4, tmp_path / "missing.wav", tmp_path / "out.mp4")
