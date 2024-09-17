import os
import csv
import boto3

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    try:
        print(event)
        bucket = event['Records'][0]['s3']['bucket']['name']
        csv_key = event['Records'][0]['s3']['object']['key']
        queue_url = os.environ['SQS_QUEUE_URL']
    
        response = s3.get_object(Bucket=bucket, Key=csv_key)
        lines = response['Body'].read().decode('ISO-8859-1').splitlines()
        
        reader = csv.reader(lines, delimiter=';')
        next(reader)
        for row in reader:
            print(f'row: {row}')
            message=';'.join(row)
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=message
            )
            print(f'message sent: {message}')
        print('End')

    except Exception as e:
        print(f'e: {e}')