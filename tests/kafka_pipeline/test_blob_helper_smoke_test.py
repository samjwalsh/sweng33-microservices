from pathlib import Path
from types import SimpleNamespace

from kafka_pipeline import blob_helper_smoke_test as smoke


def test_run_smoke_test(monkeypatch, tmp_path, capsys):
    ensure_calls = []
    upload_blob_bytes_calls = []
    download_blob_calls = []
    download_blob_to_file_calls = []
    upload_file_calls = []
    tts_name_calls = []
    reconstruction_name_calls = []

    def fake_ensure_container_exists():
        ensure_calls.append(True)

    def fake_upload_blob_bytes(*, blob_bytes, original_filename, folder, overwrite):
        upload_blob_bytes_calls.append(
            {
                "blob_bytes": blob_bytes,
                "original_filename": original_filename,
                "folder": folder,
                "overwrite": overwrite,
            }
        )
        return f"{folder}/uploaded-from-bytes.txt"

    def fake_download_blob(blob_location):
        download_blob_calls.append(blob_location)
        if blob_location.endswith("uploaded-from-bytes.txt"):
            return b"blob-helper-smoke-test"
        if blob_location.endswith("uploaded-from-file.txt"):
            return b"local-file-upload"
        raise AssertionError(f"Unexpected blob_location: {blob_location}")

    def fake_download_blob_to_file(*, blob_location, output_path):
        download_blob_to_file_calls.append(
            {"blob_location": blob_location, "output_path": str(output_path)}
        )
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"blob-helper-smoke-test")
        return output_path

    def fake_upload_file(*, local_path, folder):
        upload_file_calls.append({"local_path": str(local_path), "folder": folder})
        return f"{folder}/uploaded-from-file.txt"

    def fake_build_tts_segment_blob_name(*, src_blob, speaker_id, segment_id, ext):
        tts_name_calls.append(
            {
                "src_blob": src_blob,
                "speaker_id": speaker_id,
                "segment_id": segment_id,
                "ext": ext,
            }
        )
        return "generated/segments/my_source_video.mp4/speaker_0/segment_12.wav"

    def fake_build_reconstruction_blob_name(*, src_blob, ext):
        reconstruction_name_calls.append({"src_blob": src_blob, "ext": ext})
        return "generated/reconstruction/my_source_video.mp4/final_abc123.mp4"

    monkeypatch.setattr(smoke, "ensure_container_exists", fake_ensure_container_exists)
    monkeypatch.setattr(smoke, "upload_blob_bytes", fake_upload_blob_bytes)
    monkeypatch.setattr(smoke, "download_blob", fake_download_blob)
    monkeypatch.setattr(smoke, "download_blob_to_file", fake_download_blob_to_file)
    monkeypatch.setattr(smoke, "upload_file", fake_upload_file)
    monkeypatch.setattr(smoke, "build_tts_segment_blob_name", fake_build_tts_segment_blob_name)
    monkeypatch.setattr(
        smoke,
        "build_reconstruction_blob_name",
        fake_build_reconstruction_blob_name,
    )

    smoke.run_smoke_test(folder="smoke/blob-helper")

    assert ensure_calls == [True]
    assert upload_blob_bytes_calls == [
        {
            "blob_bytes": b"blob-helper-smoke-test",
            "original_filename": "smoke.txt",
            "folder": "smoke/blob-helper",
            "overwrite": False,
        }
    ]
    assert download_blob_calls == [
        "smoke/blob-helper/uploaded-from-bytes.txt",
        "smoke/blob-helper/uploaded-from-file.txt",
    ]
    assert len(download_blob_to_file_calls) == 1
    assert download_blob_to_file_calls[0]["blob_location"] == "smoke/blob-helper/uploaded-from-bytes.txt"
    assert len(upload_file_calls) == 1
    assert upload_file_calls[0]["folder"] == "smoke/blob-helper"
    assert tts_name_calls == [
        {
            "src_blob": "my/source/video.mp4",
            "speaker_id": "speaker_0",
            "segment_id": 12,
            "ext": "wav",
        }
    ]
    assert reconstruction_name_calls == [
        {
            "src_blob": "my/source/video.mp4",
            "ext": "mp4",
        }
    ]

    out = capsys.readouterr().out
    assert "[1/7] ensure_container_exists" in out
    assert "[2/7] upload_blob_bytes" in out
    assert "[3/7] download_blob" in out
    assert "[4/7] download_blob_to_file" in out
    assert "[5/7] upload_file" in out
    assert "[6/7] build_tts_segment_blob_name" in out
    assert "[7/7] build_reconstruction_blob_name" in out
    assert "All blob_helper smoke checks passed." in out


def test_build_parser_defaults():
    parser = smoke.build_parser()
    args = parser.parse_args([])

    assert args.folder == "smoke/blob-helper"


def test_build_parser_custom_folder():
    parser = smoke.build_parser()
    args = parser.parse_args(["--folder", "custom/folder"])

    assert args.folder == "custom/folder"


def test_main_calls_run_smoke_test(monkeypatch):
    class Args:
        folder = "custom/folder"

    calls = []

    monkeypatch.setattr(smoke, "build_parser", lambda: SimpleNamespace(parse_args=lambda: Args))
    monkeypatch.setattr(smoke, "run_smoke_test", lambda *, folder: calls.append(folder))

    smoke.main()

    assert calls == ["custom/folder"]