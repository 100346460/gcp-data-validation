from fastavro import parse_schema, writer
from google.cloud import pubsub_v1
import unittest 
import os
from pprint import pprint

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}")
    message.ack()

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


class TestPubSubMessage(unittest.TestCase):
    def test_given_file_landed_pubsub_message_sent(self):
        schema_avro = {
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

        weather_records = [
            {u'station':u'011990-99999', u'temp': 0, u'time': 1433269388}
        ]
    
        create_dummy_avro_file_locally(local_path='tests/weather4.avro', 
                                        schema=schema_avro, 
                                        records=weather_records)

        copy_file_to_bucket('tests/weather4.avro', 'test_data_validation', 'weather4.avro')
        print("Finished copying")

        subscriber = pubsub_v1.SubscriberClient()
        print("created subscriber client")
        subscription_path = subscriber.subscription_path('sacred-truck-387712', 'process_test_data_validation')
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        with subscriber:
            try:
                print("trying to streaming pull future")
                results = streaming_pull_future.result(timeout=5.0)
            except TimeoutError:
                streaming_pull_future.cancel()
                streaming_pull_future.result()
    




