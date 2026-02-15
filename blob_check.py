#debug script

import os
import sys
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError, ResourceNotFoundError
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

print("ACCOUNT_URL:", os.getenv("AZURE_STORAGE_ACCOUNT"))
cred = os.getenv("AZURE_STORAGE_KEY")
print("CREDENTIAL_SET:", bool(cred), "LEN:", len(cred) if cred else 0)


def build_client() -> BlobServiceClient:
    account_url = os.getenv("AZURE_STORAGE_ACCOUNT")
    credential = os.getenv("AZURE_STORAGE_KEY")

    if account_url and credential:
        return BlobServiceClient(account_url=account_url, credential=credential)

    raise ValueError(
        "Missing auth config. Set either:\n"
        "1) AZURE_STORAGE_CONNECTION_STRING\n"
        "or\n"
        "2) AZURE_STORAGE_ACCOUNT_URL + AZURE_STORAGE_CREDENTIAL"
    )


def main() -> int:
    try:
        client = build_client()
        container_name = os.getenv("AZURE_STORAGE_CONTAINER")

        if container_name:
            container_client = client.get_container_client(container_name)
            if container_client.exists():
                print(f"OK: Connected and container '{container_name}' exists.")
            else:
                print(f"FAIL: Connected, but container '{container_name}' does not exist.")
                return 2
        else:
            # Lightweight auth/connection test: list at most 1 container.
            next(client.list_containers(results_per_page=1).by_page(), None)
            print("OK: Connected to Azure Blob Storage.")

        return 0

    except ResourceNotFoundError as e:
        print(f"FAIL: Resource not found: {e}")
        return 3
    except AzureError as e:
        print(f"FAIL: Azure error: {e}")
        return 4
    except Exception as e:
        print(f"FAIL: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())