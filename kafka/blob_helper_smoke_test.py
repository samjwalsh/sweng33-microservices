from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from blob_helper import (
    build_reconstruction_blob_name,
    build_tts_segment_blob_name,
    download_blob,
    download_blob_to_file,
    ensure_container_exists,
    upload_blob_bytes,
    upload_file,
)


def run_smoke_test(*, folder: str) -> None:
    print("[1/7] ensure_container_exists")
    ensure_container_exists()
    print("  OK")

    print("[2/7] upload_blob_bytes")
    payload = b"blob-helper-smoke-test"
    uploaded_from_bytes = upload_blob_bytes(
        blob_bytes=payload,
        original_filename="smoke.txt",
        folder=folder,
        overwrite=False,
    )
    print(f"  Uploaded blob: {uploaded_from_bytes}")

    print("[3/7] download_blob")
    downloaded = download_blob(uploaded_from_bytes)
    assert downloaded == payload, "download_blob() payload mismatch"
    print("  OK")

    print("[4/7] download_blob_to_file")
    with tempfile.TemporaryDirectory() as temp_dir:
        output_path = Path(temp_dir) / "downloaded_smoke.txt"
        saved_path = download_blob_to_file(
            blob_location=uploaded_from_bytes,
            output_path=output_path,
        )
        assert saved_path.exists(), "download_blob_to_file() did not create file"
        assert saved_path.read_bytes() == payload, "download_blob_to_file() content mismatch"
        print(f"  Saved file: {saved_path}")

    print("[5/7] upload_file")
    with tempfile.TemporaryDirectory() as temp_dir:
        local_file = Path(temp_dir) / "local_upload.txt"
        local_file.write_bytes(b"local-file-upload")
        uploaded_from_file = upload_file(local_path=local_file, folder=folder)
        downloaded_file_blob = download_blob(uploaded_from_file)
        assert downloaded_file_blob == b"local-file-upload", "upload_file() content mismatch"
        print(f"  Uploaded blob: {uploaded_from_file}")

    print("[6/7] build_tts_segment_blob_name")
    tts_name = build_tts_segment_blob_name(
        src_blob="my/source/video.mp4",
        speaker_id="speaker_0",
        segment_id=12,
        ext="wav",
    )
    assert tts_name.endswith("segment_12.wav"), "Unexpected TTS blob name format"
    print(f"  Generated name: {tts_name}")

    print("[7/7] build_reconstruction_blob_name")
    reconstruction_name = build_reconstruction_blob_name(
        src_blob="my/source/video.mp4",
        ext="mp4",
    )
    assert reconstruction_name.startswith("generated/reconstruction/"), "Unexpected reconstruction prefix"
    assert reconstruction_name.endswith(".mp4"), "Unexpected reconstruction extension"
    print(f"  Generated name: {reconstruction_name}")

    print("\nAll blob_helper smoke checks passed.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Smoke test for kafka/blob_helper.py")
    parser.add_argument(
        "--folder",
        default="smoke/blob-helper",
        help="Blob folder prefix used for test uploads",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_smoke_test(folder=args.folder)


if __name__ == "__main__":
    main()
