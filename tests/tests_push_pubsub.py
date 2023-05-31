from fastavro import parse_schema, writer
from google.cloud import logging, bigquery
<<<<<<< HEAD
from google.cloud import storage
from google.api_core.exceptions import BadRequest
import unittest
import os

# Write the status to a BigQuery table
# have different alert levels notifying
# try visualizing in Looker


=======
import unittest
import os

>>>>>>> b6c94243f527df65d2a977dcd285c1dcc9b4f201
PROJECT_ID = "sacred-truck-387712"
PUBSUB_TOPIC = "my-topic"
BUCKET = "test_data_validation_bucket"
FUNCTION_FOLDER_NAME = "cf-data-validation"
FUNCTION_NAME = "process_object" # folder name containing main.py
CLOUD_FUNCTION_NAME = "data_validation_func"
GCS_BUCKET = "gs://test_data_validation_bucket"
<<<<<<< HEAD




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
=======
>>>>>>> b6c94243f527df65d2a977dcd285c1dcc9b4f201


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

def create_external_table_from_avro_file(client: bigquery.Client) -> None:
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
    
def full_table_query_validation(client: bigquery.Client,
                                full_table_id: str) -> None:
<<<<<<< HEAD
    try:
        sql_statement = f"SELECT * FROM `{full_table_id}`"
        job = client.query(sql_statement)
        job.result()
    except BadRequest as e:
        if "invalid namespace" in str(e).lower():
            print("Message some type of alert for namespace issue") 
        raise BadRequest(f"Error Message:{str(e)}")
=======
    sql_statement = f"SELECT * FROM `{full_table_id}`"
    job = client.query(sql_statement)
    job.result()
    print("Able to query the entire table")
>>>>>>> b6c94243f527df65d2a977dcd285c1dcc9b4f201

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

def drop_external_table(client:bigquery.Client, full_table_id:str) -> None:
    os.system(f"bq rm -f {full_table_id}")
    
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
<<<<<<< HEAD
        delete_all_bucket_contents(BUCKET)
        client = bigquery.Client()
        drop_external_table(client, "sacred-truck-387712:data_validation.weather")   
=======
        
        client = bigquery.Client()
        drop_external_table(client, "sacred-truck-387712:data_validation.weather")   
        
>>>>>>> b6c94243f527df65d2a977dcd285c1dcc9b4f201
        file_name = "weather12.avro"
        local_path = f'tests/data/{file_name}'
        
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
        
        create_dummy_avro_file_locally(local_path=f'tests/{file_name}', 
                                        schema=self.schema_avro_namespace_issue, 
                                        records=self.weather_records)
        
        copy_file_to_bucket(f'tests/{file_name}', BUCKET, file_name)
<<<<<<< HEAD
        with self.assertRaises(BadRequest):
            full_table_query_validation(client, "sacred-truck-387712.data_validation.weather")
=======
        full_table_query_validation(client, "sacred-truck-387712.data_validation.weather")
>>>>>>> b6c94243f527df65d2a977dcd285c1dcc9b4f201
        
              
        