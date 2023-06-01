from google.cloud import storage

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