import logging
import os
import time
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb')
RATE_TABLE = os.environ.get('RATE_TABLE', 'rates')

RATES = [
    {'rate_type': {'S': 'RS'}, 'vendor': {'S': 'default'}, 'value': {'N': '70'}},
    {'rate_type': {'S': 'GL'}, 'vendor': {'S': 'default'}, 'value': {'N': '40'}},
]


def seed_rates(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Seed DynamoDB table with default rates."""
    requests = [{'PutRequest': {'Item': item}} for item in RATES]
    backoff = 1
    for _ in range(5):
        try:
            dynamodb.batch_write_item(RequestItems={RATE_TABLE: requests})
            return {'status': 'ok'}
        except ClientError as exc:
            if exc.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
            else:
                logger.error('batch_write_item failed: %s', exc)
                raise
    raise RuntimeError('Failed to seed rates after retries')
