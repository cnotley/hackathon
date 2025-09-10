"""Integration-style test covering upload through fallback pipeline."""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_s3

BUCKET = "invoice-ingestion"


@mock_s3
def test_end_to_end_fallback(monkeypatch):
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.put_object(Bucket=BUCKET, Key="uploads/demo.pdf", Body=b"data")

    monkeypatch.setenv("INGESTION_BUCKET", BUCKET)
    monkeypatch.setenv("USE_SFN", "false")
    monkeypatch.setenv("DEFAULT_VENDOR_NAME", "SERVPRO")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    ingestion_module = importlib.import_module("lambda.ingestion_lambda")

    fake_responses = {
        ingestion_module.EXTRACTION_LAMBDA_NAME: {"status": "ok", "vendor": "SERVPRO", "labor": []},
        ingestion_module.RECONCILIATION_LAMBDA_NAME: {
            "status": "ok",
            "vendor": "SERVPRO",
            "discrepancies": [{"type": "rate_variance", "worker": "Robbins"}],
        },
        ingestion_module.REPORT_LAMBDA_NAME: {
            "status": "ok",
            "key": "reports/demo.xlsx",
            "vendor": "SERVPRO",
            "total_savings": 123.45,
            "discrepancies": [{"type": "rate_variance", "worker": "Robbins"}],
        },
    }

    def _fake_invoke(function_name, payload):
        return fake_responses[function_name]

    monkeypatch.setattr(ingestion_module, "_invoke_lambda", _fake_invoke)

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": BUCKET},
                    "object": {"key": "uploads/demo.pdf"},
                }
            }
        ]
    }

    response = ingestion_module.handle_s3_event(event, None)
    assert response["statusCode"] == 200
    details = response["body"]["results"][0]["details"]
    assert details["report"]["key"] == "reports/demo.xlsx"
    assert details["reconciliation"]["vendor"] == "SERVPRO"
    assert details["reconciliation"]["discrepancies"]
