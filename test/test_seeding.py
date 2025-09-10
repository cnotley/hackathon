import importlib, os, boto3, time, pytest
from decimal import Decimal

moto = pytest.importorskip("moto")
mock_aws = moto.mock_aws

seed_rates = importlib.import_module("lambda.seeding").seed_rates


def _setup(monkeypatch):
    os.environ["MWO_TABLE_NAME"] = "test-table"
    monkeypatch.setattr(time, "sleep", lambda x: None)


@mock_aws
def test_table_create(monkeypatch):
    _setup(monkeypatch)
    seed_rates()
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    tables = list(ddb.tables.all())
    assert any(t.name == "test-table" for t in tables)


@mock_aws
def test_currency(monkeypatch):
    _setup(monkeypatch)
    seed_rates()
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    table = ddb.Table("test-table")
    item = table.get_item(Key={"code": "RS"})["Item"]
    assert item["rate_usd"] == Decimal("70")
    assert item["rate_eur"] == Decimal("65")


@mock_aws
def test_ratios(monkeypatch):
    _setup(monkeypatch)
    seed_rates()
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    table = ddb.Table("test-table")
    item = table.get_item(Key={"code": "PM:GL"})["Item"]
    assert item["ratio_max"] == Decimal("0.2")
