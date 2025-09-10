"""Tests for ingestion workflow minimal integration."""

from __future__ import annotations

import importlib
import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import boto3
import pytest
import openpyxl
from moto import mock_aws

INGESTION_BUCKET = "invoice-ingestion"
STATE_MACHINE_ARN = "arn:aws:states:us-east-1:123:stateMachine:invoice"


def _load_ingestion_module(monkeypatch, use_sfn="true"):
    monkeypatch.setenv("INGESTION_BUCKET", INGESTION_BUCKET)
    monkeypatch.setenv("STATE_MACHINE_ARN", STATE_MACHINE_ARN)
    monkeypatch.setenv("USE_SFN", use_sfn)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("DEFAULT_VENDOR_NAME", "SERVPRO")

    module = importlib.import_module("lambda.ingestion_lambda")
    return importlib.reload(module)


@mock_aws
def test_handle_s3_event_starts_workflow_with_vendor(monkeypatch):
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=INGESTION_BUCKET)
    s3_client.put_object(
        Bucket=INGESTION_BUCKET,
        Key="uploads/sample.pdf",
        Body=b"data",
        Metadata={"vendor": "Servpro"},
    )

    ingestion_lambda = _load_ingestion_module(monkeypatch, use_sfn="true")

    start_mock = MagicMock(return_value="arn:aws:states:execution")
    monkeypatch.setattr(
        ingestion_lambda.WorkflowOrchestrator,
        "start_workflow",
        MagicMock(side_effect=start_mock),
    )

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": INGESTION_BUCKET},
                    "object": {"key": "uploads/sample.pdf"},
                }
            }
        ]
    }

    response = ingestion_lambda.handle_s3_event(event, None)
    assert response["statusCode"] == 200
    start_mock.assert_called_once()
    workflow_payload = start_mock.call_args.args[0]
    assert workflow_payload["vendor"] == "SERVPRO"
    assert workflow_payload["file_info"]["vendor"] == "SERVPRO"


@mock_aws
def test_handle_s3_event_fallback_includes_vendor(monkeypatch):
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=INGESTION_BUCKET)
    s3_client.put_object(Bucket=INGESTION_BUCKET, Key="uploads/sample.pdf", Body=b"data")

    ingestion_lambda = _load_ingestion_module(monkeypatch, use_sfn="false")

    fallback_mock = MagicMock(return_value={"status": "ok"})
    monkeypatch.setattr(ingestion_lambda, "_fallback_direct_processing", fallback_mock)

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": INGESTION_BUCKET},
                    "object": {"key": "uploads/sample.pdf"},
                }
            }
        ]
    }

    response = ingestion_lambda.handle_s3_event(event, None)
    assert response["statusCode"] == 200
    fallback_mock.assert_called_once()
    kwargs = fallback_mock.call_args.args
    assert kwargs[2]["vendor"] == "SERVPRO"


@mock_aws
def test_fallback_pipeline_returns_report_details(monkeypatch):
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=INGESTION_BUCKET)
    s3_client.put_object(Bucket=INGESTION_BUCKET, Key="uploads/sample.pdf", Body=b"data")

    ingestion_lambda = _load_ingestion_module(monkeypatch, use_sfn="false")

    def _fake_invoke(function_name, payload):
        if function_name == ingestion_lambda.EXTRACTION_LAMBDA_NAME:
            return {"status": "ok", "vendor": "SERVPRO", "labor": []}
        if function_name == ingestion_lambda.RECONCILIATION_LAMBDA_NAME:
            return {"status": "ok", "vendor": payload.get("vendor"), "discrepancies": [{"type": "rate_variance", "worker": "Robbins"}]}
        if function_name == ingestion_lambda.REPORT_LAMBDA_NAME:
            return {"status": "ok", "key": "reports/demo.xlsx", "vendor": payload.get("vendor"), "total_savings": 123.45}
        raise AssertionError("Unexpected function call")

    monkeypatch.setattr(ingestion_lambda, "_invoke_lambda", _fake_invoke)

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": INGESTION_BUCKET},
                    "object": {"key": "uploads/sample.pdf"},
                }
            }
        ]
    }

    response = ingestion_lambda.handle_s3_event(event, None)
    assert response["statusCode"] == 200
    results = response["body"]["results"]
    assert results[0]["status"] == "fallback_completed"
    details = results[0]["details"]
    assert details["report"]["key"] == "reports/demo.xlsx"
    assert details["reconciliation"]["discrepancies"]


@mock_aws
def test_fallback_pipeline_generates_excel_with_reconciliation(monkeypatch):
    reports_bucket = "reports-bucket"
    monkeypatch.setenv("REPORTS_BUCKET_NAME", reports_bucket)
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=INGESTION_BUCKET)
    s3_client.create_bucket(Bucket=reports_bucket)
    s3_client.put_object(
        Bucket=INGESTION_BUCKET,
        Key="uploads/test-invoice.pdf",
        Body=b"pdf",
        Metadata={"vendor": "Servpro Commercial, LLC"},
        ContentType="application/pdf",
    )

    ingestion_lambda = _load_ingestion_module(monkeypatch, use_sfn="false")

    extraction_payload = {
        "status": "ok",
        "vendor": "SERVPRO",
        "labor": [
            {
                "name": "Robbins, Dorian",
                "type": "RS",
                "reg_hours": 40.0,
                "ot_hours": 15.0,
                "hours": 55.0,
                "rate": 77.0,
                "total": 4812.5,
            }
        ]
        + [
            {
                "name": f"Tech {idx}",
                "type": "GL",
                "reg_hours": 8.5,
                "ot_hours": 0.0,
                "hours": 8.5,
                "rate": 42.0,
                "total": 357.0,
            }
            for idx in range(1, 44)
        ],
        "summaries": {
            "total_regular_hours": 458.75,
            "total_ot_hours": 661.0,
            "total_labor_charges": 77150.25,
        },
    }

    reconciliation_payload = {
        "status": "ok",
        "vendor": "SERVPRO",
        "discrepancies": [
            {
                "type": "rate_variance",
                "worker": "Robbins, Dorian",
                "labor_type": "RS",
                "hours": 55.0,
                "actual_rate": 77.0,
                "msa_rate": 70.0,
                "variance_multiplier": round(77.0 / 70.0, 2),
                "savings": 1732.50,
            },
            {
                "type": "overtime",
                "worker": "Robbins, Dorian",
                "labor_type": "RS",
                "hours": 55.0,
                "overtime_hours": 15.0,
            },
        ],
        "total_savings": 11568.00,
    }

    report_module = importlib.reload(importlib.import_module("lambda.report_lambda"))

    def _invoke(function_name, payload):
        if function_name == ingestion_lambda.EXTRACTION_LAMBDA_NAME:
            return extraction_payload
        if function_name == ingestion_lambda.RECONCILIATION_LAMBDA_NAME:
            assert "SERVPRO" in (payload.get("vendor") or "")
            return reconciliation_payload
        if function_name == ingestion_lambda.REPORT_LAMBDA_NAME:
            event = {
                "discrepancies": reconciliation_payload["discrepancies"],
                "total_savings": reconciliation_payload["total_savings"],
                "vendor": extraction_payload["vendor"],
                "report_id": "test-invoice",
            }
            return report_module.lambda_handler(event, None)
        raise AssertionError(f"unexpected function {function_name}")

    monkeypatch.setattr(ingestion_lambda, "_invoke_lambda", _invoke)

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": INGESTION_BUCKET},
                    "object": {"key": "uploads/test-invoice.pdf"},
                }
            }
        ]
    }

    response = ingestion_lambda.handle_s3_event(event, None)
    details = response["body"]["results"][0]["details"]

    assert details["extraction"]["vendor"] == "SERVPRO"
    assert details["extraction"]["summaries"]["total_ot_hours"] == pytest.approx(661.0)
    assert details["reconciliation"]["total_savings"] == pytest.approx(11568.0)
    assert len(details["reconciliation"]["discrepancies"]) == 2
    assert details["report"]["key"] == "reports/test-invoice.xlsx"

    report_obj = s3_client.get_object(Bucket=reports_bucket, Key="reports/test-invoice.xlsx")
    workbook = openpyxl.load_workbook(BytesIO(report_obj["Body"].read()))
    summary_sheet = workbook["Summary"]
    assert summary_sheet["B1"].value == "SERVPRO"
    assert summary_sheet["B2"].value == pytest.approx(11568.0)

    project_sheet = workbook["Project Summary"]
    data_rows = list(project_sheet.iter_rows(min_row=2, values_only=True))
    assert any(row[0] == "Robbins, Dorian" and row[2] == "rate_variance" for row in data_rows)
    assert any(row[2] == "overtime" for row in data_rows)
