import boto3
import json
import time
from datetime import datetime, timedelta, timezone
import os

def fetch_and_upload_logs(
    log_group_name,
    s3_bucket,
    s3_key,
    region_name='us-east-1',
    hours=24
):
    logs_client = boto3.client('logs', region_name=region_name)
    s3_client = boto3.client('s3', region_name=region_name)

    # Time range: last N hours
    end_time = int(time.time() * 1000)
    start_time = end_time - hours * 3600 * 1000

    next_token = None
    all_events = []

    while True:
        kwargs = {
            'logGroupName': log_group_name,
            'startTime': start_time,
            'endTime': end_time,
            'limit': 10000
        }
        if next_token:
            kwargs['nextToken'] = next_token

        response = logs_client.filter_log_events(**kwargs)
        events = response.get('events', [])
        all_events.extend(events)

        next_token = response.get('nextToken')
        if not next_token:
            break

    # Write logs to a .txt file
    local_txt = '/tmp/cloudwatch_logs.txt'
    with open(local_txt, 'w') as f:
        for event in all_events:
            timestamp = datetime.fromtimestamp(event['timestamp'] / 1000, tz=timezone.utc).isoformat()
            message = event['message'].rstrip('\n')
            f.write(f"[{timestamp}] {message}\n")

    # Upload to S3
    s3_client.upload_file(local_txt, s3_bucket, s3_key)
    os.remove(local_txt)
    return f"Uploaded logs to s3://{s3_bucket}/{s3_key}"

def lambda_handler(event, context):
    """
    AWS Lambda handler function
    
    Expected event structure:
    {
        "log_group_name": "/aws/lambda/site",
        "s3_bucket": "importinglogs",
        "s3_key": "exported-logs/cloudwatch_logs.txt",
        "region_name": "us-east-1",
        "hours": 24
    }
    """
    try:
        # Extract parameters from the event
        log_group_name = event.get('log_group_name', '/aws/lambda/site')
        s3_bucket = event.get('s3_bucket', 'importinglogs')
        s3_key = event.get('s3_key', 'exported-logs/cloudwatch_logs.txt')
        region_name = event.get('region_name', 'us-east-1')
        hours = event.get('hours', 24)
        
        # Call the main function
        result = fetch_and_upload_logs(
            log_group_name=log_group_name,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            region_name=region_name,
            hours=hours
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': result,
                'log_group_name': log_group_name,
                's3_bucket': s3_bucket,
                's3_key': s3_key,
                'hours': hours
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
