from pathlib import Path
import pytest

from src.transcriber import (
    format_timestamp,
    write_transcript_txt,
    transcribe_with_timestamps,
    TranscriptionResult,
    WordTimestamp,
)


def test_format_timestamp():
    assert format_timestamp(0.0) == "00:00.000"
    assert format_timestamp(61.234) == "01:01.234"


def test_write_transcript_txt_creates_file(tmp_path: Path):
    result = TranscriptionResult(
        text="hello world",
        words=[
            WordTimestamp("hello", 0.0, 0.5),
            WordTimestamp("world", 0.5, 1.0),
        ],
        model_name="test-model",
    )

    out_file = tmp_path / "transcript.txt"
    write_transcript_txt(result, out_file)

    assert out_file.exists()

    content = out_file.read_text(encoding="utf-8")
    assert "[00:00.000 - 00:00.500] hello" in content
    assert "[00:00.500 - 00:01.000] world" in content


def test_transcribe_rejects_missing_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        transcribe_with_timestamps(
            tmp_path / "missing.wav",
            model_name="openai/whisper-tiny",
            timestamp_level="chunk",
        )


def test_transcribe_rejects_non_wav(tmp_path: Path):
    fake_file = tmp_path / "audio.mp3"
    fake_file.write_bytes(b"fake data")

    with pytest.raises(ValueError):
        transcribe_with_timestamps(
            fake_file,
            model_name="openai/whisper-tiny",
            timestamp_level="chunk",
        )