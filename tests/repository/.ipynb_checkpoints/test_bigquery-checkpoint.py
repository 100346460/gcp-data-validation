from google.cloud import bigquery
from data_validation.repository.bigquery import create_log_table_for_Messages, drop_table, Message, Warning, insert_message_into_bigquery
from datetime import datetime
import unittest

PROJECT_ID = "sacred-truck-387712"
DATASET_ID = "data_validation"

def table_exists(client: bigquery.Client,
                 full_table_id: str) -> bool:
    try:
        table_ref = client.get_table(f"{full_table_id}")
        return True
    except:
        return False
    
    
def read_first_row_from_bigquery(client: bigquery.Client,
                                 project_id, 
                                 dataset_id, 
                                 table_id):
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    schema = table.schema
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}` LIMIT 1"
    query_job = client.query(query)
    results = query_job.result()
    results = [result for result in results]
    print("results:", results[0][0])
    return results

class TestWriteMessageToBigQuery(unittest.TestCase):
    def test_create_empty_Log_table_creates_table_based_on_Message_structure(self):
        client = bigquery.Client()
        full_table_id = f"{PROJECT_ID}.{DATASET_ID}.test_message_log_creation"
        drop_table(full_table_id)
        create_log_table_for_Messages(client, full_table_id)
        self.assertTrue(table_exists(client, full_table_id))
        
    
    def test_given_Message_writes_message_to_table_correctly(self):
        client = bigquery.Client()
        full_table_id = f"{PROJECT_ID}.{DATASET_ID}.test_message_log_creation"
        drop_table(full_table_id)
        message = Message(Warning.NAMESPACE, full_table_id,
                          message="namespace error message")
        insert_message_into_bigquery(client,
                                     DATASET_ID,
                                     "test_message_log_creation",
                                     message)
        
        result = read_first_row_from_bigquery(client,
                                     PROJECT_ID, 
                                     DATASET_ID, 
                                     "test_message_log_creation")
        print(result[0])