import importlib
import json
import os
from unittest.mock import Mock

import pytest


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("STATE_MACHINE_ARN", "arn:aws:states:us-east-1:123:stateMachine:ingestion")


@pytest.fixture
def module():
    return importlib.import_module("lambda.ingestion_lambda")


def _mock_stepfunctions(module, monkeypatch, failures=0):
    client = Mock()
    call_count = {"value": 0}

    def start_execution(*args, **kwargs):
        if call_count["value"] < failures:
            call_count["value"] += 1
            raise module.ClientError({"Error": {"Code": "ExecutionLimitExceeded"}}, "StartExecution")
        return {"executionArn": "arn:mock"}

    client.start_execution.side_effect = start_execution
    monkeypatch.setattr(module, "stepfunctions_client", client)
    return client


def _mock_s3_head(module, monkeypatch, size, content_type="application/pdf"):
    s3 = Mock()
    s3.head_object.return_value = {"ContentLength": size, "ContentType": content_type}
    s3.get_object_tagging.return_value = {"TagSet": []}
    monkeypatch.setattr(module, "s3_client", s3)
    return s3


def test_workflow_retry_success(monkeypatch, module):
    client = _mock_stepfunctions(module, monkeypatch, failures=1)
    _mock_s3_head(module, monkeypatch, 1024)

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "bucket"},
                    "object": {"key": "invoice.pdf"},
                }
            }
        ]
    }

    response = module.lambda_handler(event, None)

    assert response["statusCode"] == 200
    assert response["body"]["results"][0]["status"] == "workflow_started"
    assert client.start_execution.call_count == 2


def test_pdf_too_large(monkeypatch, module):
    _mock_stepfunctions(module, monkeypatch)
    _mock_s3_head(module, monkeypatch, 6 * 1024 * 1024)

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "bucket"},
                    "object": {"key": "large.pdf"},
                }
            }
        ]
    }

    response = module.lambda_handler(event, None)

    assert response["statusCode"] == 400
    assert response["error"] == "Invalid file type"
