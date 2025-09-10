import json
import hashlib
import logging
import os
import time
import uuid
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

bedrock_agent = boto3.client('bedrock-agent-runtime')
dynamodb = boto3.resource('dynamodb')
step = boto3.client('stepfunctions')

SESSION_TABLE = os.environ.get('SESSION_TABLE', 'sessions')
AGENT_ID = os.environ.get('AGENT_ID', 'agent')

def invoke_agent(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Invoke Bedrock agent with caching and guardrails."""
    query = event.get('query', 'Audit invoice for discrepancies')
    session_key = hashlib.md5(query.encode()).hexdigest()
    table = dynamodb.Table(SESSION_TABLE)
    try:
        cached = table.get_item(Key={'session_id': session_key})
        if 'Item' in cached and cached['Item'].get('ttl', 0) > int(time.time()):
            return cached['Item']['response']
    except ClientError as exc:
        logger.error("DynamoDB get_item error: %s", exc)

    session_id = str(uuid.uuid4())
    try:
        resp = bedrock_agent.invoke_agent(
            agentId=AGENT_ID,
            sessionId=session_id,
            inputText=query,
            enableTrace=True,
        )
        result = {'session_id': session_id, 'response': resp}
    except ClientError as exc:
        if os.environ.get('LOCAL'):
            path = os.path.join(os.path.dirname(__file__), 'mock_agent.json')
            with open(path) as fh:
                result = json.load(fh)
        else:
            logger.error("invoke_agent failed: %s", exc)
            raise

    try:
        table.put_item(Item={'session_id': session_key, 'response': result, 'ttl': int(time.time()) + 3600})
    except ClientError as exc:
        logger.error("DynamoDB put_item error: %s", exc)
    return result
