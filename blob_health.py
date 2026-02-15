import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

account = os.environ["AZURE_STORAGE_ACCOUNT"]
key = os.environ["AZURE_STORAGE_KEY"]
container_name = os.getenv("AZURE_STORAGE_CONTAINER", "healthcheck")



account = os.environ["AZURE_STORAGE_ACCOUNT"]
key = os.environ["AZURE_STORAGE_KEY"]
account_url = f"https://{account}.blob.core.windows.net"
service = BlobServiceClient(account_url=account_url, credential=key)

container = service.get_container_client(container_name)

#creating container if needed
try:
    container.create_container()
except Exception:
    pass    #if it passes, this means the container already exists


blob = container.get_blob_client("hello.txt")
blob.upload_blob(b"hello from python", overwrite=True)

data = blob.download_blob().readall()
print("Blob connected. Read back:", data.decode("utf-8"))
