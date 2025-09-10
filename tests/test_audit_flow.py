"""End-to-end smoke test for minimal pipeline."""

from __future__ import annotations

import json

import boto3
import pytest
from moto import mock_dynamodb, mock_s3, mock_stepfunctions

BUCKET = "ingestion"
REPORTS = "reports"
TABLE = "msa-rates"
STATE_MACHINE_ARN = "arn:aws:states:us-east-1:123:stateMachine:invoice"


@mock_s3
@mock_dynamodb
@mock_stepfunctions
@pytest.mark.integration
def test_e2e_smoke(monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    s3.create_bucket(Bucket=REPORTS)
    s3.put_object(Bucket=BUCKET, Key="uploads/demo.pdf", Body=b"pdf")

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName=TABLE,
        KeySchema=[{"AttributeName": "rate_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "rate_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    monkeypatch.setenv("INGESTION_BUCKET_NAME", BUCKET)
    monkeypatch.setenv("REPORTS_BUCKET_NAME", REPORTS)
    monkeypatch.setenv("STATE_MACHINE_ARN", STATE_MACHINE_ARN)
    monkeypatch.setenv("MSA_RATES_TABLE_NAME", TABLE)
    monkeypatch.setenv("MSA_DEFAULT_EFFECTIVE_DATE", "default")

    sf = boto3.client("stepfunctions", region_name="us-east-1")
    sf.create_state_machine(name="invoice", definition=json.dumps({"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}), roleArn="arn:aws:iam::123:role/demo")

    from lambda import seed_msa_rates

    seed_msa_rates.lambda_handler({}, None)

def test_placeholder():
    assert True
