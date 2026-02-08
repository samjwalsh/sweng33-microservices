from dotenv import load_dotenv
import os
from azure.storage.blob import BlobServiceClient

load_dotenv()
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

if not connection_string:
    print("No connection string found in .env file!")
    exit(1)

print("Testing Azure Blob Storage connection...\n")

try:
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    print("ALL CONTAINERS in this storage account:")
    containers = blob_service_client.list_containers()
    container_list = []
    
    for container in containers:
        container_list.append(container.name)
        print(f"{container.name}")
    
    if not container_list:
        print("No containers found!")
    
    print(f"\nTotal containers: {len(container_list)}")
    
    for container_name in container_list:
        print(f"Contents of: {container_name}")
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = list(container_client.list_blobs())

        if not blob_list:
            print("  (empty)")
        else:
            total_size = 0
            for blob in blob_list[:10]: 
                size_mb = blob.size / (1024**2)
                total_size += blob.size
                print(f"{blob.name} ({size_mb:.1f} MB)")
            
            if len(blob_list) > 10:
                print(f"and {len(blob_list) - 10} more files")
            
            print(f"\nTotal size: {total_size / (1024**3):.2f} GB")
            print(f"Total files: {len(blob_list)}")

except Exception as e:
    print(f"Connection failed: {e}")