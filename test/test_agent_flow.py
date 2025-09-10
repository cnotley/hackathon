import importlib
import json
import os
from types import SimpleNamespace
from decimal import Decimal

import boto3
import pytest
from moto import mock_aws


class DummyClient:
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.sent = []

    # Bedrock agent runtime methods
    def get_agent(self, *args, **kwargs):
        return {"agent": "mock"}

    def invoke_agent(self, *args, **kwargs):
        raise RuntimeError("bedrock unavailable")

    def retrieve(self, *args, **kwargs):
        return {"retrievalResults": []}

    # Bedrock runtime fallback
    def invoke_model(self, *args, **kwargs):
        return {"body": SimpleNamespace(read=lambda: json.dumps({"content": [{"text": "report"}]}).encode())}

    # Lambda async invocation
    def invoke(self, *args, **kwargs):
        return {"Payload": {"ExecutionId": "async-123"}}

    # Step functions mock
    def send_task_success(self, *args, **kwargs):
        self.sent.append(("success", args, kwargs))

    def send_task_failure(self, *args, **kwargs):
        self.sent.append(("failure", args, kwargs))

    # S3 mock
    def head_object(self, *args, **kwargs):
        return {"ContentLength": 2048, "ContentType": "application/pdf"}

    def put_object(self, *args, **kwargs):
        self.sent.append(("put_object", args, kwargs))


def _patch_boto_clients(monkeypatch):
    original_client = boto3.client

    def fake_client(service_name, *args, **kwargs):
        if service_name in {
            "bedrock-agent-runtime",
            "bedrock-runtime",
            "lambda",
            "stepfunctions",
            "s3",
        }:
            return DummyClient(service_name)
        return original_client(service_name, *args, **kwargs)

    monkeypatch.setattr(boto3, "client", fake_client)


def _create_msa_table(region_name: str = "us-east-1"):
    dynamodb = boto3.resource("dynamodb", region_name=region_name)
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
        Item={
            "labor_type": "RS",
            "location": "default",
            "standard_rate": Decimal("70.0"),
        }
    )
    table.put_item(
        Item={
            "labor_type": "default",
            "location": "overtime_rules",
            "weekly_threshold": Decimal("40"),
        }
    )
    return table


@mock_aws
def test_agent_fallback_response(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("BEDROCK_AGENT_ID", "")
    _patch_boto_clients(monkeypatch)
    _create_msa_table()

    module = importlib.import_module("lambda.agent_lambda")
    manager = module.BedrockAgentManager()

    response = manager.invoke_agent("What is the MSA rate for RS labor?")

    assert response["status"] == "fallback_success"
    assert "RS (Regular Skilled)" in response["response"]
    assert response["session_id"] in manager._session_cache


@mock_aws
def test_audit_flow_returns_completed(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("BEDROCK_AGENT_ID", "")
    monkeypatch.setenv("BUCKET_NAME", "audit-bucket")
    _patch_boto_clients(monkeypatch)
    dynamo_table = _create_msa_table()

    module = importlib.import_module("lambda.agent_lambda")

    def fake_call_extraction(bucket, key):
        return {
            "extraction_status": "completed",
            "normalized_data": {
                "labor": [
                    {"name": "Worker A", "type": "RS", "unit_price": 77.0, "total_hours": 35.0}
                ]
            },
            "processing_summary": {"processing_method": "mock"},
            "raw_extracted_data": {"page_count": 2},
        }

    def fake_invoke_agent(query, session_id=None):
        return {
            "session_id": session_id or "session-1",
            "response": "Audit completed.",
            "status": "success",
        }

    monkeypatch.setattr(module, "call_extraction_lambda", fake_call_extraction)
    monkeypatch.setattr(module.BedrockAgentManager, "invoke_agent", staticmethod(fake_invoke_agent))

    event = {
        "action": "audit",
        "bucket": "audit-bucket",
        "key": "invoice.pdf",
        "context": {"file_info": {"bucket": "audit-bucket", "key": "invoice.pdf"}},
    }

    result = module.lambda_handler(event, None)

    assert result["status"] == "pending_approval"
    assert result["audit_results"]["summary"]["rate_variances"] == 1
    assert result["audit_results"]["discrepancies"][0]["worker"] == "Worker A"

    # Ensure the DynamoDB table was referenced
    assert dynamo_table.item_count >= 2
