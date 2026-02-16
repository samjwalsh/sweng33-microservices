from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BlobUploadResult:
    container: str
    blob_name: str
    url: str | None = None


class AzureBlobStorage:
    def __init__(self, container: str):
        self.container = container

    def upload_file(self, local_path: Path, blob_name: str) -> BlobUploadResult:
        # Lazy import so unit tests can mock without requiring azure libs everywhere
        from azure.storage.blob import BlobServiceClient

        conn_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        service = BlobServiceClient.from_connection_string(conn_str)

        container_client = service.get_container_client(self.container)
        try:
            container_client.create_container()
        except Exception:
            pass  # container may already exist

        blob_client = container_client.get_blob_client(blob_name)
        with open(local_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)

        # URL may require SAS/public access; keep as None unless you generate SAS
        return BlobUploadResult(container=self.container, blob_name=blob_name, url=None)

