"""Tests for minimal extraction lambda."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_s3, mock_textract

BUCKET = "input-bucket"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@mock_s3
@mock_textract
def test_extraction_returns_labour_rows(monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    s3.put_object(Bucket=BUCKET, Key="invoice.pdf", Body=b"pdf")

    textract = boto3.client("textract", region_name="us-east-1")
    textract.create_lending_analysis_job = MagicMock()

    fake_blocks = [
        {"BlockType": "TABLE", "Id": "1", "Relationships": [{"Type": "CHILD", "Ids": ["2", "3"]}]},
        {"Id": "2", "BlockType": "CELL", "RowIndex": 1, "ColumnIndex": 1, "Relationships": [{"Type": "CHILD", "Ids": ["4"]}]},
        {"Id": "3", "BlockType": "CELL", "RowIndex": 1, "ColumnIndex": 2, "Relationships": [{"Type": "CHILD", "Ids": ["5"]}]},
        {"Id": "4", "BlockType": "WORD", "Text": "Worker"},
        {"Id": "5", "BlockType": "WORD", "Text": "Hours"},
    ]

    with patch("lambda.extraction_lambda.textract_client", MagicMock(analyze_document=MagicMock(return_value={"Blocks": fake_blocks}))):
        from lambda import extraction_lambda

        event = {"bucket": BUCKET, "key": "invoice.pdf"}
        result = extraction_lambda.lambda_handler(event, None)
        assert result["status"] == "ok"
        assert result["bucket"] == BUCKET


def test_extraction_missing_location():
    from lambda import extraction_lambda

    with pytest.raises(ValueError):
        extraction_lambda.lambda_handler({}, None)
