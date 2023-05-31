import logging

def process_object(event, context):
    try:
        # Extract the bucket and object information from the Pub/Sub message
        data = event['data']
        attributes = event['attributes']
        bucket = attributes['bucketId']
        object_name = attributes['objectId']
        
        # Process the object
        logging.info(f'Object created: gs://{bucket}/{object_name}')
        return "Success"

    except Exception as e:
        logging.info(f"Error process object: {str(e)}")
        return "Failure"