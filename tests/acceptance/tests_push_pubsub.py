from data_validation.repository.bigquery import full_table_query_validation, drop_table, Message, Warning 
from data_validation.repository.bigquery import create_log_table_for_Messages, insert_message_into_bigquery



from data_validation.repository.gcs import delete_all_bucket_contents, create_buckets_by_name, delete_buckets_by_name
from data_validation.services.pub_sub import create_pubsub_topic, delete_pubsub_topic, create_object_change_notification

from data_validation.services.cloud_functions import create_cloud_function_from_file_creation, delete_cloud_function

from fastavro import parse_schema, writer
from google.cloud import logging, bigquery
from google.api_core.exceptions import BadRequest
from google.cloud import storage
from typing import List
import unittest
import time
import random
import os

# Write the status to a BigQuery table
# have different alert levels notifying
# try visualizing in Looker

PROJECT_ID = "sacred-truck-387712"
PUBSUB_TOPIC = "my-topic"
BUCKET = "test_data_validation_bucket"
FUNCTION_FOLDER_NAME = "cf-data-validation"
FUNCTION_NAME = "process_object" # folder name containing main.py
CLOUD_FUNCTION_NAME = "data_validation_func"
GCS_BUCKET = "gs://test_data_validation_bucket"

def check_function_execution_status(project_id, function_name):
    # Load the service account credentials
    
    # Initialize the Cloud Functions client
    client = functions_v1.CloudFunctionsServiceClient()
    
    # Construct the parent resource name
    parent = f'projects/{project_id}/locations/us-central1'
    
    # Construct the Cloud Function name
    function = f'{parent}/functions/{function_name}'
    
    # Retrieve the Cloud Function's status
    response = client.get_function(name=function)
    
    # Check the status field to determine if the function is being executed
    while True:
        if response.status != functions_v1.FunctionStatus.RUNNING:
            print("Cloud Function is done executing")
            break
            
        else:
            print("Cloud Function is currently being executed.")

def create_external_table_from_avro_file(client: bigquery.Client,
                                          gcs_bucket:str) -> None:
    # example uses the weather#.avro file
    # Define the SQL statement
    sql_statement = f'''
    CREATE EXTERNAL TABLE `sacred-truck-387712.data_validation.weather`
    (
        station STRING,
        time TIMESTAMP,
        temp INT64
    )
    OPTIONS (
        format='AVRO',
        uris=['{GCS_BUCKET}/*']
    )
    '''

    # Execute the SQL statement
    job = client.query(sql_statement)
    job.result()  # Wait for the job to complete

    print("External table created successfully.")
    

def read_cloud_function_logs(project_id:str, function_name:str):
    # Initialize the Logging client
    client = logging.Client(project=project_id)

    # Define the log filter to retrieve Cloud Function logs
    filter_str = f'resource.type="cloud_function" AND resource.labels.function_name="{function_name}"'
    
    # Retrieve the Cloud Function logs
    logs = client.list_entries(filter_=filter_str)
    
    print(logs)
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
    print(f"Copying file {local_path} to bucket")


class TestPubSubPushCloudFunction(unittest.TestCase):
    def create_topic_and_cloud_function(self) -> None:
        os.system(f"gcloud pubsub topics create {PUBSUB_TOPIC}")

        os.system(f" gcloud functions deploy {CLOUD_FUNCTION_NAME} \
                    --runtime python310 \
                    --trigger-topic {PUBSUB_TOPIC} \
                    --entry-point {FUNCTION_NAME} \
                    --source {FUNCTION_FOLDER_NAME} \
                    --use-deprecated")

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
        self.schema_avro_namespace_issue = {
            'doc': 'a weather reading',
            'name': 'Weather',
            'namespace': ' ',
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
        delete_all_bucket_contents(BUCKET)
        client = bigquery.Client()
        drop_table("sacred-truck-387712:data_validation.weather")   
        file_name = "weather12.avro"
        local_path = f'tests/acceptance/data/{file_name}'
        
        create_dummy_avro_file_locally(local_path=local_path, 
                                        schema=self.schema_avro, 
                                        records=self.weather_records)
        
        copy_file_to_bucket(local_path, BUCKET, file_name)


        #check_function_execution_status(project_id="sacred-truck-387712",
        #                            function_name=CLOUD_FUNCTION_NAME)
                                                                        
        #read_cloud_function_logs(project_id="sacred-truck-387712",
        #                         function_name=CLOUD_FUNCTION_NAME)
        
        create_external_table_from_avro_file(client)
        full_table_query_validation(client, "sacred-truck-387712.data_validation.weather")
        file_name="weather_bad_namespace.avro"
        
        create_dummy_avro_file_locally(local_path=f'tests/acceptance/data/{file_name}', 
                                        schema=self.schema_avro_namespace_issue, 
                                        records=self.weather_records)
        
        copy_file_to_bucket(f'tests/acceptance/data/{file_name}', BUCKET, file_name)
        
        
        actual = full_table_query_validation(client, "sacred-truck-387712.data_validation.weather")
        expected = Message(Warning.NAMESPACE, "sacred-truck-387712.data_validation.weather", "some namespace error message")
        insert_message_into_bigquery(client, 
                                     DATASET_ID,
                                     "log_message",
                                     expected)
        self.assertEquals(expected.level, actual.level)
        self.assertEquals(expected.full_table_id, actual.full_table_id)
        
        
class TestCloudFunctions(unittest.TestCase):
    def setUp(self):
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
        self.schema_avro_namespace_issue = {
            'doc': 'a weather reading',
            'name': 'Weather',
            'namespace': ' ',
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
        
    def setUpBuckets(self, client: storage.Client, bucket_names: List[str]) -> None:
        for bucket_name in bucket_names:
            delete_all_bucket_contents(bucket_name)
        delete_buckets_by_name(client, bucket_names)
        create_buckets_by_name(client, bucket_names) 
        
        
    def tearDownBuckets(self, bucket_names: List[str]) -> None:
        # delete buckets
        for bucket_name in bucket_names:
            delete_all_bucket_contents(bucket_name)
        
        #delete_buckets_by_name(client, bucket_names)
        
    def create_cloud_functions(self, bucket_names: list[str]) -> None:
        
        for index, bucket_name in enumerate(bucket_names):
            cloud_function_name = f"{CLOUD_FUNCTION_NAME}_{bucket_name}"
            delete_cloud_function(cloud_function_name,
                                  project=PROJECT_ID,
                                  region="us-central1")
            create_cloud_function_from_file_creation(
                bucket_name=bucket_name,
                function_name=FUNCTION_NAME,
                cloud_function_name=cloud_function_name,
                function_folder_name="data_validation")
            
    def test_given_new_file_landed_in_bucket_generates_logs_to_bigquery_table(self):
        # create buckets
        client = storage.Client()
        bqclient = bigquery.Client()
        bucket_names = ["test_dv_1","test_dv_2"]
        log_table_id = "sacred-truck-387712.data_validation.message_log"

        # create buckets
        self.setUpBuckets(client, bucket_names)

        # create log table
        drop_table(log_table_id)
        create_log_table_for_Messages(
              bqclient,
             full_table_id = log_table_id)

        # create cloud functions
        self.create_cloud_functions(bucket_names)
            
        # update buckets with weather data
        for index,bucket_name in enumerate(bucket_names):
            #create file
            file_name = f"weather_{index}.avro"
            local_path = f'tests/acceptance/data/{file_name}'

            create_dummy_avro_file_locally(local_path=local_path, 
                                            schema=self.schema_avro, 
                                            records=self.weather_records)
            copy_file_to_bucket(local_path, bucket_name, file_name)
        #wait for cloud functions to finish executing
        time.sleep(30)
    
        rows = bqclient.query(f"SELECT * FROM `{log_table_id}`").result()
        for row in rows:
            print(row)
        self.assertEquals(rows.total_rows, 2)
        
        # remove buckets
        #self.tearDownbuckets(bucket_names)
        # remove cloud functions
        
        # delete log table
        drop_table(log_table_id)