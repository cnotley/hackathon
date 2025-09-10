import json
import os
import logging
import time
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
step = boto3.client('stepfunctions')


def _validate_size(bucket: str, key: str) -> None:
    """Ensure the object is smaller than 5MB."""
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        size = head.get('ContentLength', 0)
        if size > 5 * 1024 * 1024:
            raise ValueError('File too large, split required')
    except ClientError as exc:
        logger.error("head_object failed: %s", exc)
        raise


def _start_execution(files: List[Dict[str, str]]) -> None:
    payload = json.dumps({'files': files})
    arn = os.environ['STATE_MACHINE_ARN']
    try:
        step.start_execution(stateMachineArn=arn, input=payload)
    except ClientError as exc:
        if exc.response['Error']['Code'] == 'ThrottlingException':
            backoff = 1
            for _ in range(5):
                time.sleep(backoff)
                try:
                    step.start_execution(stateMachineArn=arn, input=payload)
                    return
                except ClientError as err:
                    if err.response['Error']['Code'] != 'ThrottlingException':
                        raise
                    backoff = min(backoff * 2, 30)
            raise
        else:
            logger.error("start_execution failed: %s", exc)
            raise


def handle_event(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler for S3 events."""
    records = event.get('Records') or []
    if not records:
        raise ValueError('No records in event')

    batch: List[Dict[str, str]] = []
    for rec in records:
        s3info = rec['s3']
        bucket = s3info['bucket']['name']
        key = s3info['object']['key']
        logger.info("Processing %s", key)
        _validate_size(bucket, key)
        batch.append({'bucket': bucket, 'key': key})
        if len(batch) == 10:
            _start_execution(batch)
            batch = []
    if batch:
        _start_execution(batch)

    return {'status': 'started', 'files': len(records)}
