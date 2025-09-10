"""Tests for minimal report lambda."""

from __future__ import annotations

import json
from io import BytesIO

import boto3
import openpyxl
import pytest
from moto import mock_s3

BUCKET = "reports-bucket"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("REPORTS_BUCKET_NAME", BUCKET)


@mock_s3
def test_report_writes_excel(monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    from lambda import report_lambda

    event = {
        "discrepancies": [
            {"type": "rate_variance", "worker": "Alice", "labor_type": "RS", "hours": 10, "actual_rate": 80, "msa_rate": 70},
            {"type": "overtime", "worker": "Bob", "labor_type": "US", "hours": 50},
        ],
        "total_savings": 100.0,
        "report_id": "demo",
    }

    result = report_lambda.lambda_handler(event, None)
    assert result["status"] == "ok"
    key = result["key"]
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    wb = openpyxl.load_workbook(BytesIO(body))
    assert "Project Summary" in wb.sheetnames


def test_style_header():
    from lambda import report_lambda

    wb = openpyxl.Workbook()
    row = wb.active.append(["col"])
    report_lambda._style_header(wb.active[1])
