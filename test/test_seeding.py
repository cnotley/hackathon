import importlib
import os
import sys
import time
from decimal import Decimal

import boto3
import pytest

moto = pytest.importorskip("moto")
mock_aws = moto.mock_aws


def _load_module():
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault("MSA_RATES_TABLE", "msa-rates")
    os.environ.setdefault("CREATE_MSA_TABLE_IF_MISSING", "true")
    sys.modules.pop("lambda.seed_msa_rates", None)
    return importlib.import_module("lambda.seed_msa_rates")


def _setup(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("MSA_RATES_TABLE", "msa-rates")
    monkeypatch.setenv("CREATE_MSA_TABLE_IF_MISSING", "true")
    monkeypatch.setattr(time, "sleep", lambda x: None)


def _ensure_table_exists():
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    client = ddb.meta.client
    if "msa-rates" not in client.list_tables().get("TableNames", []):
        client.create_table(
            TableName="msa-rates",
            KeySchema=[{"AttributeName": "rate_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "rate_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        client.get_waiter("table_exists").wait(TableName="msa-rates")
    return ddb


@mock_aws()
def test_table_create(monkeypatch):
    _setup(monkeypatch)
    ddb = _ensure_table_exists()
    seed_module = _load_module()
    seed_module.lambda_handler({}, None)
    tables = list(ddb.tables.all())
    assert any(t.name == "msa-rates" for t in tables)


@mock_aws()
def test_currency(monkeypatch):
    _setup(monkeypatch)
    ddb = _ensure_table_exists()
    seed_module = _load_module()
    seed_module.lambda_handler({}, None)
    table = ddb.Table("msa-rates")
    items = table.scan(ConsistentRead=True)["Items"]
    items_by_id = {item["rate_id"]: item for item in items}
    assert "RS_default" in items_by_id
    assert items_by_id["RS_default"]["standard_rate"] == Decimal("70.00")


@mock_aws()
def test_ratios(monkeypatch):
    _setup(monkeypatch)
    ddb = _ensure_table_exists()
    seed_module = _load_module()
    seed_module.lambda_handler({}, None)
    table = ddb.Table("msa-rates")
    items = table.scan(ConsistentRead=True)["Items"]
    items_by_id = {item["rate_id"]: item for item in items}
    assert "ratio_rules_su_rs" in items_by_id
    assert items_by_id["ratio_rules_su_rs"]["max_ratio"] == Decimal("6.0")
