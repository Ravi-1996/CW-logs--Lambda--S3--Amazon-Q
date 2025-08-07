import boto3
import json
import time
from datetime import datetime, timezone
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

    # Format new log content
    new_log_lines = []
    for event in all_events:
        timestamp = datetime.fromtimestamp(event['timestamp'] / 1000, tz=timezone.utc).isoformat()
        message = event['message'].rstrip('\n')
        new_log_lines.append(f"[{timestamp}] {message}")

    # Read existing file from S3 (if it exists) and append
    existing_logs = ""
    try:
        obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        existing_logs = obj['Body'].read().decode('utf-8')
        print(f"Existing log file found: {s3_key}")
    except s3_client.exceptions.NoSuchKey:
        print(f"No existing file. Creating new: {s3_key}")

    all_logs = existing_logs + "\n" + "\n".join(new_log_lines)

    # Write combined logs to local file
    local_txt = '/tmp/cloudwatch_logs.txt'
    with open(local_txt, 'w') as f:
        f.write(all_logs)

    s3_client.upload_file(local_txt, s3_bucket, s3_key)
    os.remove(local_txt)

    return f"Uploaded logs for {log_group_name} to s3://{s3_bucket}/{s3_key}"

def lambda_handler(event, context):
    log_group_names = [
        "/aws/lambda/site",
        "/aws/lambda/site2",
        "/aws/lambda/site3"
    ]
    s3_bucket = "importinglogs"
    region_name = "us-east-1"
    hours = 24

    try:
        responses = []
        for log_group in log_group_names:
            log_group_clean = log_group.strip('/').replace('/', '_')
            s3_key = f"exported-logs/{log_group_clean}.txt"

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
                'messages': responses,
                's3_bucket': s3_bucket,
                'hours': hours
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
