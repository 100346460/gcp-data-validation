import os

def create_pubsub_topic(pubsub_topic: str) -> None:
    os.system(f"gcloud pubsub topics create {pubsub_topic}")
    
def delete_pubsub_topic(pubsub_topic: str) -> None:
    os.system(f"gcloud pubsub topics delete {pubsub_topic}")
    
def create_object_change_notification(pubsub_topic: str,
                                      bucket_name: str) -> None:
    '''creates a push notification when a new object is landed within a gcs bucket'''
    os.system(f"gsutil notification create -t {pubsub_topic} -f json -e OBJECT_FINALIZE gs://{bucket_name}")