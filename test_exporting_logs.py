import unittest
from unittest.mock import patch, MagicMock, mock_open
import json
import time
from datetime import datetime, timezone
import os
import sys

# Add the current directory to the path to import the module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from exporting_logs import fetch_and_upload_logs, lambda_handler


class TestExportingLogs(unittest.TestCase):

    @patch('exporting_logs.boto3.client')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.remove')
    def test_fetch_and_upload_logs_success(self, mock_remove, mock_file, mock_boto3):
        # Mock AWS clients
        mock_logs_client = MagicMock()
        mock_s3_client = MagicMock()
        mock_boto3.side_effect = [mock_logs_client, mock_s3_client]
        
        # Mock CloudWatch logs response
        mock_logs_client.filter_log_events.return_value = {
            'events': [
                {'timestamp': 1640995200000, 'message': 'Test log message 1\n'},
                {'timestamp': 1640995260000, 'message': 'Test log message 2'}
            ]
        }
        
        result = fetch_and_upload_logs(
            log_group_name='/aws/lambda/test',
            s3_bucket='test-bucket',
            s3_key='test-key.txt'
        )
        
        # Assertions
        mock_logs_client.filter_log_events.assert_called_once()
        mock_s3_client.upload_file.assert_called_once_with(
            '/tmp/cloudwatch_logs.txt', 'test-bucket', 'test-key.txt'
        )
        mock_remove.assert_called_once_with('/tmp/cloudwatch_logs.txt')
        self.assertEqual(result, "Uploaded logs to s3://test-bucket/test-key.txt")

    @patch('exporting_logs.boto3.client')
    def test_fetch_and_upload_logs_pagination(self, mock_boto3):
        mock_logs_client = MagicMock()
        mock_s3_client = MagicMock()
        mock_boto3.side_effect = [mock_logs_client, mock_s3_client]
        
        # Mock paginated response
        mock_logs_client.filter_log_events.side_effect = [
            {
                'events': [{'timestamp': 1640995200000, 'message': 'Log 1'}],
                'nextToken': 'token123'
            },
            {
                'events': [{'timestamp': 1640995260000, 'message': 'Log 2'}]
            }
        ]
        
        with patch('builtins.open', mock_open()), patch('os.remove'):
            fetch_and_upload_logs('/aws/lambda/test', 'bucket', 'key')
        
        self.assertEqual(mock_logs_client.filter_log_events.call_count, 2)

    @patch('exporting_logs.fetch_and_upload_logs')
    def test_lambda_handler_success(self, mock_fetch):
        mock_fetch.return_value = "Uploaded logs to s3://test-bucket/test-key.txt"
        
        event = {
            'log_group_name': '/aws/lambda/test',
            's3_bucket': 'test-bucket',
            's3_key': 'test-key.txt',
            'hours': 12
        }
        
        result = lambda_handler(event, {})
        
        expected_response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Uploaded logs to s3://test-bucket/test-key.txt',
                'log_group_name': '/aws/lambda/test',
                's3_bucket': 'test-bucket',
                's3_key': 'test-key.txt',
                'hours': 12
            })
        }
        
        self.assertEqual(result, expected_response)
        mock_fetch.assert_called_once_with(
            log_group_name='/aws/lambda/test',
            s3_bucket='test-bucket',
            s3_key='test-key.txt',
            region_name='us-east-1',
            hours=12
        )

    def test_lambda_handler_default_values(self):
        with patch('exporting_logs.fetch_and_upload_logs') as mock_fetch:
            mock_fetch.return_value = "Success"
            
            result = lambda_handler({}, {})
            
            mock_fetch.assert_called_once_with(
                log_group_name='/aws/lambda/site',
                s3_bucket='importinglogs',
                s3_key='exported-logs/cloudwatch_logs.txt',
                region_name='us-east-1',
                hours=24
            )

    @patch('exporting_logs.fetch_and_upload_logs')
    def test_lambda_handler_error(self, mock_fetch):
        mock_fetch.side_effect = Exception("AWS Error")
        
        result = lambda_handler({}, {})
        
        expected_response = {
            'statusCode': 500,
            'body': json.dumps({'error': 'AWS Error'})
        }
        
        self.assertEqual(result, expected_response)

    @patch('exporting_logs.time.time')
    @patch('exporting_logs.boto3.client')
    def test_time_range_calculation(self, mock_boto3, mock_time):
        mock_time.return_value = 1640995200  # Fixed timestamp
        mock_logs_client = MagicMock()
        mock_s3_client = MagicMock()
        mock_boto3.side_effect = [mock_logs_client, mock_s3_client]
        
        mock_logs_client.filter_log_events.return_value = {'events': []}
        
        with patch('builtins.open', mock_open()), patch('os.remove'):
            fetch_and_upload_logs('/aws/lambda/test', 'bucket', 'key', hours=48)
        
        # Verify time range calculation
        call_args = mock_logs_client.filter_log_events.call_args[1]
        expected_end_time = 1640995200000  # mock_time * 1000
        expected_start_time = expected_end_time - (48 * 3600 * 1000)
        
        self.assertEqual(call_args['endTime'], expected_end_time)
        self.assertEqual(call_args['startTime'], expected_start_time)


    @patch('exporting_logs.boto3.client')
    def test_empty_log_events(self, mock_boto3):
        mock_logs_client = MagicMock()
        mock_s3_client = MagicMock()
        mock_boto3.side_effect = [mock_logs_client, mock_s3_client]
        
        # Mock empty response
        mock_logs_client.filter_log_events.return_value = {'events': []}
        
        with patch('builtins.open', mock_open()) as mock_file, patch('os.remove'):
            result = fetch_and_upload_logs('/aws/lambda/test', 'bucket', 'key')
            
        # Verify empty file is still created and uploaded
        mock_file.assert_called_once_with('/tmp/cloudwatch_logs.txt', 'w')
        mock_s3_client.upload_file.assert_called_once()
        self.assertEqual(result, "Uploaded logs to s3://bucket/key")

    @patch('exporting_logs.boto3.client')
    def test_s3_upload_failure(self, mock_boto3):
        mock_logs_client = MagicMock()
        mock_s3_client = MagicMock()
        mock_boto3.side_effect = [mock_logs_client, mock_s3_client]
        
        mock_logs_client.filter_log_events.return_value = {'events': []}
        mock_s3_client.upload_file.side_effect = Exception("S3 upload failed")
        
        with patch('builtins.open', mock_open()), patch('os.remove'):
            with self.assertRaises(Exception) as context:
                fetch_and_upload_logs('/aws/lambda/test', 'bucket', 'key')
            
        self.assertIn("S3 upload failed", str(context.exception))

    @patch('exporting_logs.boto3.client')
    def test_cloudwatch_api_error(self, mock_boto3):
        mock_logs_client = MagicMock()
        mock_s3_client = MagicMock()
        mock_boto3.side_effect = [mock_logs_client, mock_s3_client]
        
        mock_logs_client.filter_log_events.side_effect = Exception("Log group not found")
        
        with self.assertRaises(Exception) as context:
            fetch_and_upload_logs('/aws/lambda/test', 'bucket', 'key')
            
        self.assertIn("Log group not found", str(context.exception))

    def test_lambda_handler_invalid_hours(self):
        with patch('exporting_logs.fetch_and_upload_logs') as mock_fetch:
            mock_fetch.return_value = "Success"
            
            event = {'hours': 'invalid'}
            result = lambda_handler(event, {})
            
            # Should use string value as-is (Lambda will handle type conversion)
            mock_fetch.assert_called_once()
            call_args = mock_fetch.call_args[1]
            self.assertEqual(call_args['hours'], 'invalid')


if __name__ == '__main__':
    unittest.main()