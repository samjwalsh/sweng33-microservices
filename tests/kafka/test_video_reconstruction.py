import pytest
import subprocess
from unittest.mock import MagicMock
from pathlib import Path
import sys

kafka_dir = Path(__file__).resolve().parents[2] / "kafka"
sys.path.insert(0, str(kafka_dir))
sys.path.insert(0, str(kafka_dir / "microservices"))

def make_wav(path, duration_s=1.0, sample_rate=16000):
    subprocess.run(
        [
            "ffmpeg",
            "-f", "lavfi",
            "-i", f"anullsrc=r={sample_rate}:cl=mono",
            "-t", str(duration_s),
            "-y", str(path),
        ],
        check=True,
        capture_output=True,
    )

@pytest.fixture
def fake_segments():
    from db_helper import TTSSegment

    src_blob = "blob/original.mp4"
    return [
        TTSSegment(
            segment_id=1,
            speaker_id="s1",
            start=0.0,
            end=2.0,
            gen_blob="blob/seg1.wav",
            src_blob=src_blob,
        ),
        TTSSegment(
            segment_id=2,
            speaker_id="s1",
            start=2.0,
            end=4.5,
            gen_blob="blob/seg2.wav",
            src_blob=src_blob,
        ),
    ]

@pytest.fixture
def fake_blob_download(monkeypatch):
    def fake_download(blob_location, output_path):
        output_path = Path(output_path)
        if blob_location.endswith(".mp4"):
            subprocess.run(
                [
                    "ffmpeg",
                    "-f", "lavfi", "-i", "color=c=black:s=64x64:r=25",
                    "-f", "lavfi", "-i", "anullsrc",
                    "-t", "5",
                    "-y", str(output_path),
                ],
                check=True,
                capture_output=True,
            )
        else:
            make_wav(output_path, duration_s=1.5)
        return output_path

    monkeypatch.setattr("reconstruction_service.download_blob_to_file", fake_download)

@pytest.fixture
def mock_upload(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr("reconstruction_service.upload_file", mock)
    return mock


@pytest.fixture
def fake_blob_name(monkeypatch):
    monkeypatch.setattr(
        "reconstruction_service.build_reconstruction_blob_name",
        lambda *args, **kwargs: "output/final.mp4",
    )

@pytest.fixture
def reconstruction_env(fake_blob_download, mock_upload, fake_blob_name):
    pass

def test_reconstruct_video(tmp_path, fake_segments, reconstruction_env, mock_upload):
    from reconstruction_service import reconstruct_video # type: ignore
    src_blob = "blob/original.mp4"
    result = reconstruct_video(
        src_blob=src_blob,
        segments=fake_segments,
    )
    assert result == "output/final.mp4"
    mock_upload.assert_called_once()