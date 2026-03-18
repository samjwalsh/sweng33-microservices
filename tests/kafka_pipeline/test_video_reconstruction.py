import pytest
import subprocess
from unittest.mock import MagicMock
from pathlib import Path
import sys
from pydub import AudioSegment

kafka_dir = Path(__file__).resolve().parents[2] / "kafka_pipeline"
sys.path.insert(0, str(kafka_dir))
sys.path.insert(0, str(kafka_dir / "microservices"))

from db_helper import TTSSegment  # type: ignore
from reconstruction_service import reconstruct_video  # type: ignore

"""
.wav and .mp4 files are generated automatically for testing. 
Blob downloads and uploads are also mocked.
"""

def make_wav(path, duration_s=1.0, sample_rate=16000): 
    subprocess.run(
        ["ffmpeg", "-f", "lavfi", "-i", f"anullsrc=r={sample_rate}:cl=mono",
         "-t", str(duration_s), "-y", str(path)],
        check=True, capture_output=True,
    )

def make_mp4(path, duration_s=5.0):
    subprocess.run(
        ["ffmpeg",
         "-f", "lavfi", "-i", "color=c=black:s=64x64:r=25",
         "-f", "lavfi", "-i", "anullsrc",
         "-t", str(duration_s), "-y", str(path)],
        check=True, capture_output=True,
    )


def make_mp4_with_tone(path, duration_s=5.0):
    subprocess.run(
        [
            "ffmpeg",
            "-f",
            "lavfi",
            "-i",
            "color=c=black:s=64x64:r=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=16000",
            "-t",
            str(duration_s),
            "-y",
            str(path),
        ],
        check=True,
        capture_output=True,
    )


def make_tone_wav(path, duration_s=1.0, frequency=880, sample_rate=16000):
    subprocess.run(
        [
            "ffmpeg",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency={frequency}:sample_rate={sample_rate}",
            "-t",
            str(duration_s),
            "-y",
            str(path),
        ],
        check=True,
        capture_output=True,
    )


def make_silent_wav(path, duration_s=1.0, sample_rate=16000):
    subprocess.run(
        [
            "ffmpeg",
            "-f",
            "lavfi",
            "-i",
            f"anullsrc=r={sample_rate}:cl=mono",
            "-t",
            str(duration_s),
            "-y",
            str(path),
        ],
        check=True,
        capture_output=True,
    )


def extract_wav_from_mp4(input_mp4, output_wav):
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(input_mp4), "-vn", "-acodec", "pcm_s16le", str(output_wav)],
        check=True,
        capture_output=True,
    )


def slice_dbfs(audio, start_s, end_s):
    clip = audio[int(start_s * 1000):int(end_s * 1000)]
    return clip.dBFS

@pytest.fixture
def fake_segments():

    src_blob = "blob/original.mp4"
    return [
        TTSSegment(segment_id=1, speaker_id="s1", start=0.0, end=2.0, gen_blob="blob/seg1.wav", src_blob=src_blob),
        TTSSegment(segment_id=2, speaker_id="s1", start=2.0, end=4.5, gen_blob="blob/seg2.wav", src_blob=src_blob),
    ]

@pytest.fixture
def fake_blob_download(monkeypatch):
    def fake_download(blob_location, output_path):
        output_path = Path(output_path)
        if blob_location.endswith(".mp4"):
            make_mp4(output_path)
        else:
            make_wav(output_path, duration_s=1.5)
        return output_path
    monkeypatch.setattr("reconstruction_service.download_blob_to_file", fake_download)

@pytest.fixture
def mock_upload(monkeypatch):
    mock = MagicMock(return_value="output/final.mp4")
    monkeypatch.setattr("reconstruction_service.upload_file", mock)
    return mock

@pytest.fixture
def fake_blob_name(monkeypatch):
    monkeypatch.setattr("reconstruction_service.build_reconstruction_blob_name",lambda *args, **kwargs: "output/final.mp4",)

@pytest.fixture
def reconstruction_env(fake_blob_download, mock_upload, fake_blob_name):
    return None

def test_reconstruct_video_uploads_output(fake_segments, reconstruction_env, mock_upload):

    reconstruct_video(src_blob="blob/original.mp4", segments=fake_segments)
    mock_upload.assert_called_once()
    args, kwargs = mock_upload.call_args
    upload_path = Path(args[0] if args else next(iter(kwargs.values())))
    assert upload_path.suffix == ".mp4"

def test_reconstruct_video_download_failure_raises(fake_blob_name, mock_upload, monkeypatch):
    monkeypatch.setattr("reconstruction_service.download_blob_to_file", MagicMock(side_effect=RuntimeError("Storage unavailable")),)
    segments = [TTSSegment(segment_id=1, speaker_id="s1", start=0.0, end=2.0, gen_blob="blob/seg1.wav", src_blob="blob/original.mp4")]
    with pytest.raises(Exception):
        reconstruct_video(src_blob="blob/original.mp4", segments=segments)

def test_reconstruct_video_upload_failure_raises(fake_segments, fake_blob_download, fake_blob_name, monkeypatch):
    monkeypatch.setattr("reconstruction_service.upload_file",MagicMock(side_effect=IOError("Upload failed")),)
    with pytest.raises(Exception):
        reconstruct_video(src_blob="blob/original.mp4", segments=fake_segments)


def test_reconstruct_video_preserves_source_audio_in_gaps_and_mutes_translated_windows(monkeypatch, tmp_path):
    src_blob = "blob/original.mp4"
    seg1_blob = "blob/seg1.wav"
    seg2_blob = "blob/seg2.wav"

    segments = [
        TTSSegment(segment_id=1, speaker_id="s1", start=1.0, end=2.0, gen_blob=seg1_blob, src_blob=src_blob),
        TTSSegment(segment_id=2, speaker_id="s1", start=3.0, end=4.0, gen_blob=seg2_blob, src_blob=src_blob),
    ]

    source_mp4 = tmp_path / "source.mp4"
    seg1_wav = tmp_path / "seg1.wav"
    seg2_wav = tmp_path / "seg2.wav"
    output_mp4 = tmp_path / "output.mp4"
    output_wav = tmp_path / "output.wav"

    make_mp4_with_tone(source_mp4, duration_s=5.0)
    make_silent_wav(seg1_wav, duration_s=0.8)
    make_silent_wav(seg2_wav, duration_s=0.8)

    blob_map = {
        src_blob: source_mp4,
        seg1_blob: seg1_wav,
        seg2_blob: seg2_wav,
    }

    def fake_download(blob_location, output_path):
        output_path = Path(output_path)
        output_path.write_bytes(Path(blob_map[blob_location]).read_bytes())
        return output_path

    def fake_upload(local_path, blob_name):
        output_mp4.write_bytes(Path(local_path).read_bytes())

    monkeypatch.setattr("reconstruction_service.download_blob_to_file", fake_download)
    monkeypatch.setattr("reconstruction_service.upload_file", fake_upload)
    monkeypatch.setattr(
        "reconstruction_service.build_reconstruction_blob_name",
        lambda *args, **kwargs: "output/final.mp4",
    )

    reconstruct_video(src_blob=src_blob, segments=segments)
    extract_wav_from_mp4(output_mp4, output_wav)

    audio = AudioSegment.from_file(output_wav)

    gap_level = slice_dbfs(audio, 0.2, 0.8)
    seg1_level = slice_dbfs(audio, 1.2, 1.8)
    seg2_level = slice_dbfs(audio, 3.2, 3.8)

    assert gap_level > -35
    assert seg1_level < gap_level - 15
    assert seg2_level < gap_level - 15
