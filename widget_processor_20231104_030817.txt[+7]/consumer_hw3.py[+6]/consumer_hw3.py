import boto3
import argparse
import json
import logging
from datetime import datetime

# Create logging file
log_file_name = datetime.now().strftime('widget_processor_%Y%m%d_%H%M%S.txt')
logging.basicConfig(filename=log_file_name, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_widget_request_from_sqs(queue_url):
    sqs = boto3.client('sqs')
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=5)
    
    if 'Messages' not in response:
        return None

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    body = json.loads(message['Body'])
    
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    
    logging.info(f"Origin: SQS queue {queue_url}\nRequest Content: {json.dumps(body, indent=4)}\n")
    
    return body

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

def process_request_s3(request, bucket_name):
    widget_data = json.dumps(request)
    widget_key = f"widgets/{request['owner'].replace(' ', '-').lower()}/{request['widgetId']}"
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=widget_key, Body=widget_data)
    
    logging.info(f"Destination: S3 bucket {bucket_name}\nRequest Content: {json.dumps(request, indent=4)}\n")

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

def delete_widget(request, bucket_name, table_name):
    storage = request.get('storage')
    if storage == 's3':
        s3 = boto3.client('s3')
        widget_key = f"widgets/{request['owner'].replace(' ', '-').lower()}/{request['widgetId']}"
        s3.delete_object(Bucket=bucket_name, Key=widget_key)
    elif storage == 'dynamodb':
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        table.delete_item(Key={'id': request['widgetId']})
    
    if storage:
        logging.info(f"Deleted widget {request['widgetId']} from {storage}")

def update_widget(request, bucket_name, table_name):
    storage = request.get('storage')
    if storage == 's3':
        process_request_s3(request, bucket_name)
    elif storage == 'dynamodb':
        process_request_dynamodb(request, table_name)
    
    if storage:
        logging.info(f"Updated widget {request['widgetId']} in {storage}")

def process_request(request, storage_option, bucket_name, table_name):
    if request['type'] == 'create':
        if storage_option == 's3':
            process_request_s3(request, bucket_name)
        elif storage_option == 'dynamodb':
            process_request_dynamodb(request, table_name)
        print(f"Processed {request['type']} request for widget {request['widgetId']}")
    elif request['type'] == 'delete':
        delete_widget(request, bucket_name, table_name)
    elif request['type'] == 'update':
        update_widget(request, bucket_name, table_name)

def main(storage_option, queue_url):
    BUCKET_2 = 'usu-cs5250-blue-requests'
    BUCKET_3 = 'usu-cs5250-blue-dist'
    DYNAMODB_TABLE = 'widgets'

    while True:
        # Check SQS queue for new messages
        widget_request = get_widget_request_from_sqs(queue_url)
        if widget_request:
            process_request(widget_request, storage_option, BUCKET_3, DYNAMODB_TABLE)
            continue

        # Check S3 bucket for new requests
        widget_request = get_widget_request_from_s3(BUCKET_2)
        if widget_request:
            process_request(widget_request, storage_option, BUCKET_3, DYNAMODB_TABLE)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process widget requests.")
    parser.add_argument('--storage', choices=['s3', 'dynamodb'], required=True)
    parser.add_argument('--queue-url', type=str, required=True)
    args = parser.parse_args()
    main(args.storage, args.queue_url)
