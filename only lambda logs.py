import boto3
import json
import time
from datetime import datetime, timezone
import os

def fetch_all_lambda_log_groups(region_name='us-east-1'):
    logs_client = boto3.client('logs', region_name=region_name)
    log_groups = []
    next_token = None

    while True:
        if next_token:
            response = logs_client.describe_log_groups(nextToken=next_token)
        else:
            response = logs_client.describe_log_groups()

        for group in response['logGroups']:
            log_group_name = group['logGroupName']
            if log_group_name.startswith('/aws/lambda/') and 'aws-logs-write-test' not in log_group_name:
                log_groups.append(log_group_name)

        next_token = response.get('nextToken')
        if not next_token:
            break

    return log_groups

def fetch_and_upload_logs(log_group_name, s3_bucket, s3_key, region_name='us-east-1', hours=24):
    logs_client = boto3.client('logs', region_name=region_name)
    s3_client = boto3.client('s3', region_name=region_name)

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

    # Format new log entries
    new_log_lines = []
    for event in all_events:
        timestamp = datetime.fromtimestamp(event['timestamp'] / 1000, tz=timezone.utc).isoformat()
        message = event['message'].rstrip('\n')
        new_log_lines.append(f"[{timestamp}] {message}")

    # Fetch existing logs from S3 if file exists
    existing_logs = ""
    try:
        obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        existing_logs = obj['Body'].read().decode('utf-8')
    except s3_client.exceptions.NoSuchKey:
        pass

    # Combine old + new logs
    all_logs = existing_logs + "\n" + "\n".join(new_log_lines)

    # Save to temp file and upload
    local_txt = '/tmp/cloudwatch_logs.txt'
    with open(local_txt, 'w') as f:
        f.write(all_logs)

    s3_client.upload_file(local_txt, s3_bucket, s3_key)
    os.remove(local_txt)

    return f"Uploaded logs for {log_group_name} to s3://{s3_bucket}/{s3_key}"

def lambda_handler(event, context):
    s3_bucket = "importinglogs"
    region_name = "us-east-1"
    hours = 24

    try:
        lambda_log_groups = fetch_all_lambda_log_groups(region_name)
        responses = []

        for log_group in lambda_log_groups:
            # Create file name based on Lambda function name
            function_name = log_group.split("/")[-1]
            s3_key = f"exported-logs/{function_name}.txt"

            result = fetch_and_upload_logs(
                log_group_name=log_group,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                region_name=region_name,
                hours=hours
            )
            responses.append(result)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'processed_groups': lambda_log_groups,
                'messages': responses
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
