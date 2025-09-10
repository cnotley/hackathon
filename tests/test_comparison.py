"""Tests for minimal reconciliation lambda."""

from __future__ import annotations

import json
from decimal import Decimal

import boto3
import pytest
from moto import mock_dynamodb

TABLE_NAME = "msa-rates"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("MSA_RATES_TABLE_NAME", TABLE_NAME)
    monkeypatch.setenv("MSA_DEFAULT_EFFECTIVE_DATE", "default")


@mock_dynamodb
def test_reconciliation_flags_variance(monkeypatch):
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[{"AttributeName": "rate_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "rate_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item={"rate_id": "RS_default", "effective_date": "default", "standard_rate": Decimal("70.00")})

    from lambda import reconciliation_lambda

    event = {"labor": [{"name": "Alice", "type": "RS", "hours": 10, "rate": 80}]}
    result = reconciliation_lambda.lambda_handler(event, None)

    assert result["status"] == "ok"
    assert result["discrepancies"]
    assert result["total_savings"] > 0


def test_extract_labour_from_event(monkeypatch):
    from lambda import reconciliation_lambda

    event = {"extraction": {"Payload": {"labor": [{"name": "Bob", "type": "US", "hours": 10}]}}}
    labour = reconciliation_lambda._extract_labour(event)
    assert labour[0]["name"] == "Bob"
