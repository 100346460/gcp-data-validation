from repository.bigquery import insert_message_into_bigquery, Message, Warning
from google.cloud import bigquery
import base64
import json

def process_object(data, context):   
    print(data['bucket'])
    print(data['name'])
    print(data['selfLink'])
    client = bigquery.Client()
    insert_message_into_bigquery(client,
                                 dataset_id='data_validation',
                                 table_id='message_log',
                           message = Message(Warning.INFO, data['name'],
                                             "This is a test message")
                                )
    