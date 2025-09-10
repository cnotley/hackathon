import json
import logging
import os
from typing import Any, Dict, List

import boto3
import numpy as np
from botocore.exceptions import ClientError
from statsmodels.stats.diagnostic import normal_ad

logger = logging.getLogger()
logger.setLevel(logging.INFO)

bedrock = boto3.client('bedrock-runtime')
dynamodb = boto3.resource('dynamodb')
sagemaker = boto3.client('sagemaker-runtime')

RATE_TABLE = os.environ.get('RATE_TABLE', 'rates')


def compare_data(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    labor: List[Dict[str, Any]] = event.get('labor', [])
    table = dynamodb.Table(RATE_TABLE)
    flags: List[Dict[str, Any]] = []
    savings_total = 0.0

    for item in labor:
        if item.get('total', 0) < 0:
            raise ValueError('Negative totals not allowed')
        try:
            resp = table.get_item(Key={'rate_type': item['type'], 'vendor': item.get('name', 'default')})
            expected = float(resp.get('Item', {}).get('value', 0))
        except ClientError as exc:
            logger.error('DynamoDB get_item error: %s', exc)
            expected = 0
        if expected:
            variance = (item['rate'] - expected) / expected * 100
            if variance > 5:
                savings = (item['rate'] - expected) * item.get('hours', 0)
                flags.append({'type': 'variance', 'details': f"{item['type']} rate {item['rate']} >{expected} by {variance:.1f}%", 'savings': savings})
        else:
            logger.warning('No expected rate for %s', item['type'])

    names = [i['name'] for i in labor]
    if len(names) != len(set(names)):
        flags.append({'type': 'duplicate', 'details': 'Duplicate labor entries detected', 'savings': 0})

    totals = [i['total'] for i in labor]
    try:
        stat, p = normal_ad(np.array(totals))
        if p < 0.05:
            flags.append({'type': 'distribution', 'details': 'Labor totals not normally distributed', 'savings': 0})
    except Exception as exc:
        logger.error('Distribution check failed: %s', exc)

    try:
        body = json.dumps({'totals': totals})
        resp = sagemaker.invoke_endpoint(EndpointName='anomaly-endpoint', Body=body)
        result = json.loads(resp['Body'].read())
        if result.get('is_anomaly'):
            flags.append({'type': 'anomaly', 'details': 'Anomaly detected', 'savings': 0})
    except ClientError as exc:
        logger.error('SageMaker invoke failed: %s', exc)

    try:
        prompt = f"Compare extracted JSON to rates and flag issues: {json.dumps(event)}"
        bedrock.invoke_model(modelId='claude', body=json.dumps({'prompt': prompt, 'max_tokens_to_sample': 2000}))
    except ClientError as exc:
        logger.error('Bedrock invoke_model error: %s', exc)

    for f in flags:
        savings_total += f.get('savings', 0.0)
    return {'flags': flags, 'savings_total': savings_total}
