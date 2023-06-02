from google.cloud import storage
from google.api_core.exceptions import NotFound
from typing import List

def delete_all_bucket_contents(bucket_name:str):
    # Create a storage client
    client = storage.Client()

    # Get the bucket reference
    bucket = client.get_bucket(bucket_name)

    # List all blobs in the bucket
    blobs = bucket.list_blobs()

    # Delete each blob in the bucket
    for blob in blobs:
        blob.delete()

    print(f"All contents in the bucket '{bucket_name}' have been cleared.")
    
    
def create_bucket(client: storage.Client, bucket_name:str) -> None:
    bucket = client.create_bucket(bucket_name)
    print(f"Bucket '{bucket.name}' created.")
    
def create_buckets_by_name(client: storage.Client, 
                           bucket_names:List[str]) -> None:
    for bucket_name in bucket_names:
        bucket = client.create_bucket(bucket_name)
        print(f"Bucket '{bucket.name}' created.")
    
def delete_bucket(client: storage.Client, bucket_name:str) -> None:
    bucket = client.get_bucket(bucket_name)
    bucket.delete()
    print(f"Bucket '{bucket.name}' deleted.")

def delete_buckets_by_name(client: storage.Client, 
                           bucket_names:List[str]) -> None:
    for bucket_name in bucket_names:
        try:
            bucket = client.get_bucket(bucket_name)
            bucket.delete()
            print(f"Bucket '{bucket.name}' deleted.")
        except NotFound as e:
            print(f"Bucket {bucket_name} not found.")
            
            
        
