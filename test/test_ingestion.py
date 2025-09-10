import os
import importlib
from unittest.mock import patch

import boto3
from moto import mock_s3

ingestion_lambda = importlib.import_module('lambda.ingestion_lambda')
handle_event = ingestion_lambda.handle_event


@mock_s3
@patch('lambda.ingestion_lambda.step.start_execution')
def test_ingestion_starts_execution(mock_start):
    os.environ['STATE_MACHINE_ARN'] = 'arn:aws:states:us-east-1:123:sm'
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='b')
    s3.put_object(Bucket='b', Key='f.pdf', Body=b'hi')
    event = {'Records': [{'s3': {'bucket': {'name': 'b'}, 'object': {'key': 'f.pdf'}}}]}
    result = handle_event(event, None)
    assert result['files'] == 1
    mock_start.assert_called_once()
