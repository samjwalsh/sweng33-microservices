import os
import pytest
from src.storage.blob_health import BlobHealthCheck
from dotenv import load_dotenv

load_dotenv()
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

@pytest.mark.skipif(
    not os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
    reason="Requires Azure Blob Storage access"
)
def test_blob_health():
    assert BlobHealthCheck.run() is True
