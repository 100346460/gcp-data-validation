import os

def create_cloud_function_from_file_creation(
                            bucket_name: str,
                            function_name: str,
                            cloud_function_name: str,
                            function_folder_name: str) -> None:
    
    os.system(f"""gcloud functions deploy {cloud_function_name}\
              --runtime python37 \
              --trigger-resource {bucket_name} \
              --trigger-event google.storage.object.finalize \
              --entry-point {function_name} \
              --source {function_folder_name} \
              --region us-central1""")

    
def delete_cloud_function(
    cloud_function_name: str,
    project: str,
    region: str) -> None:
    os.system(f"""gcloud functions delete {cloud_function_name} --project {project} --region {region} --quiet""")
    print(f"deleted cloud function: {cloud_function_name}")
