from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

import os

class Warning(Enum):
    INFO = "INFO"
    ERROR = "ERROR"
    NAMESPACE = "NAMESPACE"

@dataclass
class Message:
    level: Warning
    full_table_id: str
    message: str
    
    def as_tuple(self) -> None:
        return (level, full_table_id, message)
    
    
def full_table_query_validation(client: bigquery.Client,
                                full_table_id: str) -> None:
    try:
        sql_statement = f"SELECT * FROM `{full_table_id}`"
        job = client.query(sql_statement)
        job.result()
    except BadRequest as e:
        if "invalid namespace" in str(e).lower():
            return Message(Warning.NAMESPACE, full_table_id, str(e))
        return Message(Warning.INFO, full_table_id, str(e))
    

    
def create_log_table_for_Messages(client: bigquery.Client,
                           full_table_id: str) -> None:
    sql_statement = f"""CREATE TABLE IF NOT EXISTS `{full_table_id}`
                    (level String, 
                    full_table_id String, 
                    message String, 
                    datetime Timestamp)"""
    job = client.query(sql_statement)
    job.result()
        

def insert_message_into_bigquery(client: bigquery.Client,
                                 dataset_id: str,
                                 table_id: str,
                                 message: Message) -> None:
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row_to_insert = [(message.level.value, message.full_table_id, 
                      message.message, current_datetime
                     )]
    errors = client.insert_rows(table, row_to_insert)
    if errors == []:
        print("Data inserted successfully.")
    else:
        print(f"Encountered errors while inserting data: {errors}")
        
        
def drop_table(full_table_id:str) -> None:
    os.system(f"bq rm -f {full_table_id}")