from __future__ import annotations

import os
import uuid
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


def _container_name() -> str:
    return os.getenv("AZURE_STORAGE_CONTAINER", "healthcheck")


def _blob_service_client():
    from azure.storage.blob import BlobServiceClient

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if connection_string:
        return BlobServiceClient.from_connection_string(connection_string)

    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    key = os.getenv("AZURE_STORAGE_KEY")
    if account and key:
        account_url = f"https://{account}.blob.core.windows.net"
        return BlobServiceClient(account_url=account_url, credential=key)

    raise RuntimeError(
        "Azure Blob credentials are not configured. Set either "
        "AZURE_STORAGE_CONNECTION_STRING or both AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY."
    )


def ensure_container_exists() -> None:
    container_client = _blob_service_client().get_container_client(_container_name())
    try:
        container_client.create_container()
    except Exception:
        pass


def download_blob(blob_location: str) -> bytes:
    blob_client = _blob_service_client().get_blob_client(
        container=_container_name(),
        blob=blob_location,
    )
    return blob_client.download_blob().readall()


def download_blob_to_file(*, blob_location: str, output_path: str | Path) -> Path:
    content = download_blob(blob_location)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(content)
    return output


def upload_blob_bytes(
    *,
    blob_bytes: bytes,
    original_filename: str,
    folder: str = "",
    overwrite: bool = False,
) -> str:
    ensure_container_exists()
    ext = Path(original_filename).suffix or ".bin"
    unique_filename = f"{uuid.uuid4().hex}{ext}"
    blob_location = f"{folder}/{unique_filename}" if folder else unique_filename

    blob_client = _blob_service_client().get_blob_client(
        container=_container_name(),
        blob=blob_location,
    )
    blob_client.upload_blob(blob_bytes, overwrite=overwrite)
    return blob_location


def upload_file(
    *,
    local_path: str | Path,
    folder: str = "",
    blob_name: str | None = None,
    overwrite: bool = True,
) -> str:
    ensure_container_exists()
    file_path = Path(local_path)
    final_blob_name = blob_name
    if not final_blob_name:
        unique_filename = f"{uuid.uuid4().hex}{file_path.suffix}"
        final_blob_name = f"{folder}/{unique_filename}" if folder else unique_filename

    blob_client = _blob_service_client().get_blob_client(
        container=_container_name(),
        blob=final_blob_name,
    )
    with file_path.open("rb") as handle:
        blob_client.upload_blob(handle, overwrite=overwrite)
    return final_blob_name


def build_tts_segment_blob_name(
    *,
    src_blob: str,
    speaker_id: str,
    segment_id: int,
    ext: str = ".wav",
) -> str:
    safe_src_blob = src_blob.replace("/", "_").replace("\\", "_")
    normalized_ext = ext if ext.startswith(".") else f".{ext}"
    return f"generated/segments/{safe_src_blob}/{speaker_id}/segment_{segment_id}{normalized_ext}"


def build_reconstruction_blob_name(*, src_blob: str, ext: str = ".mp4") -> str:
    safe_src_blob = src_blob.replace("/", "_").replace("\\", "_")
    normalized_ext = ext if ext.startswith(".") else f".{ext}"
    return f"generated/reconstruction/{safe_src_blob}/final_{uuid.uuid4().hex}{normalized_ext}"