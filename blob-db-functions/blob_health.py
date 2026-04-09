import os
import uuid
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

account = os.environ["AZURE_STORAGE_ACCOUNT"]
key = os.environ["AZURE_STORAGE_KEY"]
container_name = os.getenv("AZURE_STORAGE_CONTAINER", "healthcheck")
account_url = f"https://{account}.blob.core.windows.net"
service = BlobServiceClient(account_url=account_url, credential=key)
container = service.get_container_client(container_name)



def download_video(blob_location: str) -> bytes:
    """Fetches a video file from Azure Blob Storage"""

    blob_client = service.get_blob_client(container=container_name, blob=blob_location)
    return blob_client.download_blob().readall()



def upload_video(video_bytes: bytes, original_filename: str, folder: str) -> str:
    """Uploads a video file to Azure Blob Storage and returns the blob location"""

    #makes a unqiue filename
    ext = Path(original_filename).suffix or ".mp4"
    unique_filename = f"{uuid.uuid4().hex}{ext}"

    #builds blob locations and uploads video
    blob_location = f"{folder}/{unique_filename}" if folder else unique_filename
    blob_client = service.get_blob_client(container=container_name, blob=blob_location)
    blob_client.upload_blob(video_bytes, overwrite=False)

    return blob_location


if __name__ == "__main__":

    #creating container if needed
    try:
        container.create_container()
    except Exception:
        pass    #if it passes, this means the container already exists


    blob = container.get_blob_client("hello.txt")
    blob.upload_blob(b"hello from python", overwrite=True)

    data = blob.download_blob().readall()
    print("Blob connected. Read back:", data.decode("utf-8"))



    #Test to see if download_video() works
    data = download_video("j0mWWjjpuhRt5cWCjyidL0m5itR99RvB/1ce509ee-ceb7-43e8-bcd3-60a8ab685655.mp4")
    print("Downloaded bytes:", len(data))

    #Test to see if upload_video() works
    loc = upload_video(b"globe_test_file", "test_file_small.mp4", folder="queued")
    print("Uploaded to:", loc)
