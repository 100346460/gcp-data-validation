from fastavro import parse_schema, writer
from google.cloud import logging
import unittest
import os

PUBSUB_TOPIC = "my-topic"
BUCKET = "test_data_validation_bucket"
FUNCTION_FOLDER_NAME = "data-validation"
FUNCTION_NAME = "process_object" # folder name containing main.py
CLOUD_FUNCTION_NAME = "data_validation_func"


def read_cloud_function_logs(project_id:str, function_name:str):
    # Initialize the Logging client
    client = logging.Client(project=project_id)
    
    # Define the log filter to retrieve Cloud Function logs
    filter_str = f'resource.type="cloud_function" AND resource.labels.function_name="{function_name}"'
    
    # Retrieve the Cloud Function logs
    logs = client.list_entries(filter_=filter_str)
    
    # Process and print the log entries
    for log_entry in logs:
        print(log_entry)

def create_dummy_avro_file_locally(local_path: str,
                                    schema: dict,
                                    records: list) -> None:
    parsed_schema = parse_schema(schema)
    with open(local_path, "wb") as out:
        writer(out, parsed_schema, records)

def copy_file_to_bucket(local_path: str,
                        bucket: str,
                        gcs_file_path: str) -> None:
    os.system(f"gsutil cp {local_path} gs://{bucket}/{gcs_file_path}")


class TestPubSubPushCloudFunction(unittest.TestCase):
    def create_topic_and_cloud_function(self) -> None:
        os.system(f"gcloud pubsub topics create {PUBSUB_TOPIC}")

        os.system(f" gcloud functions deploy {CLOUD_FUNCTION_NAME} \
                    --runtime python310 \
                    --trigger-topic {PUBSUB_TOPIC} \
                    --entry-point {FUNCTION_NAME} \
                    --source {FUNCTION_FOLDER_NAME}")

        os.system(f"gsutil notification create -t {PUBSUB_TOPIC} -f json -e OBJECT_FINALIZE gs://{BUCKET}")


    def setUp(self):
        # create_topic_and_cloud_function()
        self.schema_avro = {
            'doc': 'a weather reading',
            'name': 'Weather',
            'namespace': 'test',
            'type': 'record',
            'fields': [
                {'name':'station', 'type':'string'},
                {'name':'time', 'type':'long'},
                {'name':'temp', 'type':'int'}
            ],
        }

        self.weather_records = [
            {u'station':u'011990-99999', u'temp': 0, u'time': 1433269388}
        ]

    def test_create_new_object_in_bucket(self):
        file_name = "weather8.avro"
        create_dummy_avro_file_locally(local_path=f'tests/{file_name}', 
                                        schema=self.schema_avro, 
                                        records=self.weather_records)
        
        copy_file_to_bucket('tests/{file_name}', BUCKET, file_name)

        read_cloud_function_logs(project_id="sacred-truck-387712",
                                 function_name=FUNCTION_NAME)