import importlib
import json
import os
from pathlib import Path
from unittest.mock import Mock

import boto3
import pytest
from moto import mock_aws


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("MSA_RATES_TABLE", "msa-rates")
    monkeypatch.setenv("SAGEMAKER_ENDPOINT", "test-endpoint")
    monkeypatch.setenv("BEDROCK_AGENT_ID", "")  # Force fallback
    monkeypatch.setenv("BUCKET_NAME", "audit-bucket")


def _create_bucket():
    s3 = boto3.resource("s3", region_name="us-east-1")
    bucket = s3.Bucket("audit-bucket")
    bucket.create()
    return bucket


def _create_msa_table():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.create_table(
        TableName="msa-rates",
        KeySchema=[
            {"AttributeName": "labor_type", "KeyType": "HASH"},
            {"AttributeName": "location", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "labor_type", "AttributeType": "S"},
            {"AttributeName": "location", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()
    table.put_item(
        Item={"labor_type": "RS", "location": "default", "standard_rate": 70.0}
    )
    table.put_item(
        Item={"labor_type": "default", "location": "overtime_rules", "weekly_threshold": 40}
    )
    return table


def _mock_ingestion_response(module, monkeypatch):
    fake_sf = Mock()
    fake_sf.start_execution.return_value = {"executionArn": "arn:aws:states:us-east-1:123:execution"}
    monkeypatch.setattr(module, "stepfunctions_client", fake_sf)
    fake_s3 = Mock()
    fake_s3.head_object.return_value = {"ContentLength": 2048, "ContentType": "application/pdf"}
    monkeypatch.setattr(module, "s3_client", fake_s3)


def _mock_extraction(monkeypatch):
    extraction_result = {
        "extraction_status": "completed",
        "normalized_data": {
            "labor": [
                {"name": "Worker A", "type": "RS", "total_hours": 45, "unit_price": 77, "total_cost": 3465}
            ],
            "materials": [],
        },
        "processing_summary": {"processing_method": "mock"},
        "raw_extracted_data": {"page_count": 22},
    }
    monkeypatch.setattr("lambda.agent_lambda.call_extraction_lambda", lambda *args, **kwargs: extraction_result)


@mock_aws
def test_e2e_audit_happy_path(monkeypatch):
    _create_bucket()
    _create_msa_table()

    agent_module = importlib.import_module("lambda.agent_lambda")
    _mock_ingestion_response(agent_module, monkeypatch)
    _mock_extraction(monkeypatch)

    def fake_invoke_agent(query, session_id=None):
        return {"session_id": session_id or "session-1", "response": "Audit completed", "status": "success"}

    monkeypatch.setattr(agent_module.BedrockAgentManager, "invoke_agent", staticmethod(fake_invoke_agent))

    comparison_module = importlib.import_module("lambda.comparison_lambda")
    mock_sagemaker = Mock()
    mock_sagemaker.invoke_endpoint.return_value = {
        "Body": Mock(read=lambda: json.dumps({"predictions": [1.2]}).encode())
    }
    monkeypatch.setattr(comparison_module, "sagemaker_client", mock_sagemaker)

    report_module = importlib.import_module("lambda.report_lambda")
    monkeypatch.setattr(report_module.BedrockReportGenerator, "generate_markdown_report", lambda *a, **k: "# Report")
    monkeypatch.setattr(report_module.ExcelReportGenerator, "generate_excel_report", lambda *a, **k: b"excel")
    monkeypatch.setattr(report_module.ReportManager, "_upload_reports", lambda *a, **k: {"markdown": "s3://audit/report.md", "excel": "s3://audit/report.xlsx"})

    event = {
        "action": "audit",
        "bucket": "audit-bucket",
        "key": "invoices/test.pdf",
        "context": {"file_info": {"bucket": "audit-bucket", "key": "invoices/test.pdf"}},
    }

    result = agent_module.lambda_handler(event, None)

    assert result["status"] == "completed"
    assert result["audit_results"]["summary"]["rate_variances"] == 1
    assert result["recommendations"] == "Audit completed"
    assert result["extraction_summary"]["page_count"] == 22


@mock_aws
def test_audit_handles_no_labor(monkeypatch):
    _create_bucket()
    _create_msa_table()

    agent_module = importlib.import_module("lambda.agent_lambda")
    _mock_ingestion_response(agent_module, monkeypatch)
    empty_extraction = {
        "extraction_status": "completed",
        "normalized_data": {"labor": []},
        "processing_summary": {"processing_method": "mock"},
    }
    monkeypatch.setattr("lambda.agent_lambda.call_extraction_lambda", lambda *args, **kwargs: empty_extraction)

    def fake_invoke_agent(query, session_id=None):
        return {"session_id": session_id or "session-1", "response": "Audit completed", "status": "success"}

    monkeypatch.setattr(agent_module.BedrockAgentManager, "invoke_agent", staticmethod(fake_invoke_agent))

    result = agent_module.lambda_handler(
        {
            "action": "audit",
            "bucket": "audit-bucket",
            "key": "invoices/test.pdf",
            "context": {"file_info": {"bucket": "audit-bucket", "key": "invoices/test.pdf"}},
        },
        None,
    )

    assert result["status"] == "completed"
    assert result["audit_results"]["summary"]["total_discrepancies"] == 0
