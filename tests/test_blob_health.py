# test_blob_health.py

import os
import unittest
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

class TestBlobHealth(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load .env
        load_dotenv()
        cls.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not cls.connection_string:
            raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING not set in environment")

        try:
            cls.blob_service_client = BlobServiceClient.from_connection_string(cls.connection_string)
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Azure Blob Storage: {e}")

        # Model configurations
        cls.model_config = {
            "translation": {"container_name": "models", "blob_prefix": "translation/"},
            "transcription": {"container_name": "models", "blob_prefix": "transcription/"},
            "tts": {"container_name": "models", "blob_prefix": "tts/"},
        }

    def test_models_accessible(self):
        """Check that each model has blobs in Azure Storage"""
        for model_name, cfg in self.model_config.items():
            with self.subTest(model=model_name):
                try:
                    container_client = self.blob_service_client.get_container_client(cfg["container_name"])
                    blobs = list(container_client.list_blobs(name_starts_with=cfg["blob_prefix"]))
                    self.assertTrue(
                        blobs,
                        msg=f"No blobs found for model '{model_name}' with prefix '{cfg['blob_prefix']}'"
                    )
                    print(f"{model_name}: ✅ Accessible, {len(blobs)} files found")
                except Exception as e:
                    self.fail(f"Failed to access container for model '{model_name}': {e}")

if __name__ == "__main__":
    unittest.main()
