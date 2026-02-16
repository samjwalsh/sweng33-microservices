import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

class BlobHealthCheck:
    @staticmethod
    def run():
        load_dotenv()
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING not set")

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        model_config = {
            "translation": {"container_name": "models", "blob_prefix": "translation/"},
            "transcription": {"container_name": "models", "blob_prefix": "transcription/"},
            "tts": {"container_name": "models", "blob_prefix": "tts/"},
        }

        for model_name, cfg in model_config.items():
            container_client = blob_service_client.get_container_client(cfg["container_name"])
            blobs = list(container_client.list_blobs(name_starts_with=cfg["blob_prefix"]))
            if not blobs:
                raise RuntimeError(
                    f"No blobs found for model '{model_name}' with prefix '{cfg['blob_prefix']}'"
                )

        return True
