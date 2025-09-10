import importlib
import json
from unittest.mock import patch

import boto3
from moto import mock_dynamodb

comparison_lambda = importlib.import_module('lambda.comparison_lambda')
compare_data = comparison_lambda.compare_data


def setup_rates():
    ddb = boto3.resource('dynamodb', region_name='us-east-1')
    table = ddb.create_table(
        TableName='rates',
        KeySchema=[{'AttributeName': 'rate_type', 'KeyType': 'HASH'}, {'AttributeName': 'vendor', 'KeyType': 'RANGE'}],
        AttributeDefinitions=[{'AttributeName': 'rate_type', 'AttributeType': 'S'}, {'AttributeName': 'vendor', 'AttributeType': 'S'}],
        BillingMode='PAY_PER_REQUEST',
    )
    table.put_item(Item={'rate_type': 'RS', 'vendor': 'Manderville', 'value': '70'})


class BodyObj(bytes):
    def read(self):
        return self


@mock_dynamodb
@patch('lambda.comparison_lambda.sagemaker.invoke_endpoint', return_value={'Body': BodyObj(json.dumps({'is_anomaly': False}).encode())})
@patch('lambda.comparison_lambda.bedrock.invoke_model', return_value={})
def test_compare_data(_, __):
    setup_rates()
    event = {'labor': [{'name': 'Manderville', 'type': 'RS', 'rate': 77.0, 'hours': 1.0, 'total': 77.0}]}
    result = compare_data(event, None)
    assert result['flags']
