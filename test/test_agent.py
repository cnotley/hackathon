import os
import importlib
from unittest.mock import patch

import boto3
from moto import mock_dynamodb

agent_lambda = importlib.import_module('lambda.agent_lambda')
invoke_agent = agent_lambda.invoke_agent

def create_table():
    ddb = boto3.resource('dynamodb', region_name='us-east-1')
    ddb.create_table(
        TableName='sessions',
        KeySchema=[{'AttributeName': 'session_id', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'session_id', 'AttributeType': 'S'}],
        BillingMode='PAY_PER_REQUEST',
    )


@mock_dynamodb
@patch('lambda.agent_lambda.bedrock_agent.invoke_agent', return_value={'output': 'ok'})
def test_invoke_agent(mock_invoke):
    os.environ['SESSION_TABLE'] = 'sessions'
    os.environ['AGENT_ID'] = 'a1'
    create_table()
    result = invoke_agent({'query': 'hi'}, None)
    assert 'session_id' in result
    mock_invoke.assert_called_once()
