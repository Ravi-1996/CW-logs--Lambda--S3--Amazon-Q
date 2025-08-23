# CloudWatch Logs to Amazon Q Integration

## Overview
This project implements an automated pipeline to export AWS Lambda CloudWatch logs to S3 for Amazon Q natural language querying. The architecture follows: **Event Trigger → Lambda → S3 → Amazon Q**.

## Architecture

```
CloudWatch Logs → Lambda Function → S3 Bucket → Amazon Q
```

### Components
- **CloudWatch Logs**: Source of Lambda function logs
- **Lambda Function**: Processes and exports logs
- **S3 Bucket**: Storage for exported log files
- **Amazon Q**: Natural language querying interface

## Implementation Files

### Core Lambda Functions

#### 1. `exporting logs.py`
**Purpose**: Basic log export functionality for a single log group
- Fetches logs from specified CloudWatch log group
- Exports to S3 as text file
- Configurable time range (default: 24 hours)

**Key Features**:
- Parameterized via event payload
- Error handling with proper HTTP responses
- Timestamp formatting in ISO format

#### 2. `only lambda logs.py`
**Purpose**: Automatically discovers and exports all Lambda function logs
- Auto-discovers Lambda log groups (filters `/aws/lambda/` prefix)
- Creates separate S3 file per Lambda function
- Appends new logs to existing files

**Key Features**:
- Excludes test log groups (`aws-logs-write-test`)
- Incremental log appending
- Bulk processing of multiple Lambda functions

#### 3. `single text file per log group.py`
**Purpose**: Exports multiple predefined log groups to separate files
- Processes hardcoded list of log groups
- Creates individual S3 files per log group
- Appends to existing log files

## Configuration

### S3 Bucket Setup
- **Bucket Name**: `importinglogs`
- **Region**: `us-east-1`
- **File Path**: `exported-logs/`

### IAM Policies

#### S3 Bucket Policy (`Bucket policy.json`)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "logs.us-east-1.amazonaws.com"
      },
      "Action": "s3:GetBucketAcl",
      "Resource": "arn:aws:s3:::<Your-Bucket-Name>"
    },
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "logs.us-east-1.amazonaws.com"
      },
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::<Your-Bucket-Name>/*",
      "Condition": {
        "StringEquals": {
          "s3:x-amz-acl": "bucket-owner-full-control"
        }
      }
    }
  ]
}
```

#### Lambda Execution Role Policy (`Inline-policy for bucket.json`)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "*"
    }
  ]
}
```

## Usage

### Event Payload Structure
```json
{
  "log_group_name": "/aws/lambda/site",
  "s3_bucket": "importinglogs",
  "s3_key": "exported-logs/cloudwatch_logs.txt",
  "region_name": "us-east-1",
  "hours": 24
}
```

### Deployment Steps
1. Create S3 bucket with appropriate policies
2. Deploy Lambda function with required IAM permissions
3. Configure event trigger (CloudWatch Events, API Gateway, etc.)
4. Set up Amazon Q to access S3 bucket

## Required IAM Permissions

### Lambda Execution Role
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:DescribeLogGroups",
        "logs:FilterLogEvents"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::importinglogs",
        "arn:aws:s3:::importinglogs/*"
      ]
    }
  ]
}
```

## Output Format
Exported logs follow this format:
```
[2024-01-15T10:30:45.123Z] START RequestId: abc123 Version: $LATEST
[2024-01-15T10:30:45.456Z] [INFO] Processing request
[2024-01-15T10:30:45.789Z] END RequestId: abc123
```

## Amazon Q Integration
Once logs are in S3, Amazon Q can:
- Query logs using natural language
- Search for specific error patterns
- Analyze Lambda performance metrics
- Generate insights from log data

### Example Queries
- "Show me all errors from the last 24 hours"
- "What Lambda functions had the most invocations?"
- "Find timeout errors in my Lambda logs"

## Monitoring and Troubleshooting

### Common Issues
1. **Permission Errors**: Verify IAM policies and S3 bucket permissions
2. **Large Log Volumes**: Consider pagination and memory limits
3. **Network Timeouts**: Implement retry logic for S3 operations

### Best Practices
- Use appropriate log retention policies
- Monitor Lambda execution costs
- Implement error notifications
- Regular cleanup of old log files

## Cost Optimization
- Set CloudWatch log retention periods
- Use S3 lifecycle policies for log archival
- Monitor Lambda execution duration and memory usage