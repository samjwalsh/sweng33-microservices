from pathlib import Path
from unittest.mock import patch
import os

from src.storage.azure_blob import AzureBlobStorage


def test_upload_file_calls_upload_blob(tmp_path):
    # Create a fake file
    f = tmp_path / "x.wav"
    f.write_bytes(b"RIFF....WAVE")  # minimal dummy content

    os.environ["AZURE_STORAGE_CONNECTION_STRING"] = "fake"

    with patch("azure.storage.blob.BlobServiceClient") as MockBSC:
        service = MockBSC.from_connection_string.return_value
        container_client = service.get_container_client.return_value
        blob_client = container_client.get_blob_client.return_value

        storage = AzureBlobStorage(container="dubbed-audio")
        res = storage.upload_file(f, "job1/x.wav")

        blob_client.upload_blob.assert_called()
        assert res.container == "dubbed-audio"
        assert res.blob_name == "job1/x.wav"

