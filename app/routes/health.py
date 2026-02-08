from fastapi import APIRouter
from azure.storage.blob import BlobServiceClient
import os

router = APIRouter()

@router.get("/blob-health")
def blob_health():
    """
    Check that models exist on Azure Blob Storage.
    Returns a dictionary per model indicating whether blobs are accessible.
    """
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        return {"status": "failed", "reason": "AZURE_STORAGE_CONNECTION_STRING not set"}

    results = {}
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    except Exception as e:
        return {"status": "failed", "reason": f"Failed to connect to Blob Storage: {str(e)}"}

    model_config = {
        "translation": {"container_name": "models", "blob_prefix": "translation/"},
        "transcription": {"container_name": "models", "blob_prefix": "transcription/"},
        "tts": {"container_name": "models", "blob_prefix": "tts/"},
    }

    for model_name, cfg in model_config.items():
        try:
            container_client = blob_service_client.get_container_client(cfg["container_name"])
            blobs = list(container_client.list_blobs(name_starts_with=cfg["blob_prefix"]))
            if blobs:
                results[model_name] = {"accessible": True, "file_count": len(blobs)}
            else:
                results[model_name] = {"accessible": False, "reason": "No blobs found with prefix"}
        except Exception as e:
            results[model_name] = {"accessible": False, "reason": str(e)}

    return results




