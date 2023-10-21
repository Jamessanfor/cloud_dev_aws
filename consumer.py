import boto3
import argparse
import time
import json
import logging
from datetime import datetime

#create logging file
log_file_name = datetime.now().strftime('widget_processor_%Y%m%d_%H%M%S.txt')
logging.basicConfig(filename=log_file_name, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#function for getting requests for bucket
def get_widget_request_from_s3(bucket_name):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
    if 'Contents' not in response:
        return None

    obj_key = response['Contents'][0]['Key']
    obj_data = s3.get_object(Bucket=bucket_name, Key=obj_key)
    raw_data = obj_data['Body'].read().decode('utf-8')
    widget_request = json.loads(raw_data)
    s3.delete_object(Bucket=bucket_name, Key=obj_key)
    
    logging.info(f"Origin: S3 bucket {bucket_name}\nRequest Content: {json.dumps(widget_request, indent=4)}\n")
    
    return widget_request
    
#function for processing requests for s3
def process_request_s3(request, bucket_name):
    widget_data = json.dumps(request)
    widget_key = f"widgets/{request['owner'].replace(' ', '-').lower()}/{request['widgetId']}"
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=widget_key, Body=widget_data)
    
    logging.info(f"Destination: S3 bucket {bucket_name}\nRequest Content: {json.dumps(request, indent=4)}\n")

#function for processing requests for dynamodb
def process_request_dynamodb(request, table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    widget_item = {
        'id': request['widgetId'], 
        'widgetId': request['widgetId'], 
        'owner': request['owner'], 
        'label': request['label'], 
        'description': request['description']
    }

    if 'otherAttributes' in request:
        for attr in request['otherAttributes']:
            widget_item[attr['name']] = attr['value']

    table.put_item(Item=widget_item)
    
    logging.info(f"Destination: DynamoDB table {table_name}\nRequest Content: {json.dumps(request, indent=4)}\n")

def main(storage_option):
    BUCKET_2 = 'usu-cs5250-blue-requests'  
    BUCKET_3 = 'usu-cs5250-blue-dist'  
    DYNAMODB_TABLE = 'widgets' 


    while True:
        widget_request = get_widget_request_from_s3(BUCKET_2)
        if widget_request:
            if widget_request['type'] == 'create':
                if storage_option == 's3':
                    process_request_s3(widget_request, BUCKET_3)
                elif storage_option == 'dynamodb':
                    process_request_dynamodb(widget_request, DYNAMODB_TABLE)
                print(f"Processed {widget_request['type']} request for widget {widget_request['widgetId']}")
            


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process widget requests.")
    parser.add_argument('--storage', choices=['s3', 'dynamodb'], required=True)
    args = parser.parse_args()
    main(args.storage)
