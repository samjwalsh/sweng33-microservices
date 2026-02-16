import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

account = os.environ["AZURE_STORAGE_ACCOUNT"]
key = os.environ["AZURE_STORAGE_KEY"]
container_name = os.getenv("AZURE_STORAGE_CONTAINER", "healthcheck")
account_url = f"https://{account}.blob.core.windows.net"
service = BlobServiceClient(account_url=account_url, credential=key)
container = service.get_container_client(container_name)



#Write a function to take a blob location and fetch the file. Steps:
    #Connect to the blob storage and download the video file using the 'blob' property from the database entry
    #Return this file so that it can be used by the rest of the AI processing functions

def download_video(blob_location: str) -> bytes:
    blob_client = service.get_blob_client(container=container_name, blob=blob_location)
    return blob_client.download_blob().readall()



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
