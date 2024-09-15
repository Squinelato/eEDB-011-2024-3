import os
import csv
import time
import boto3
import random

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    csv_key = event['Records'][0]['s3']['object']['key']
    queue_url = os.environ['SQS_QUEUE_URL']

    response = s3.get_object(Bucket=bucket, Key=csv_key)
    lines = response['Body'].read().decode('utf-8').splitlines()
    
    reader = csv.reader(lines)
    for row in reader:
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=str(row)
        )
        time.sleep(random.randint(1, 5))
