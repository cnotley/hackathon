"""Tests for ingestion workflow minimal integration."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_s3, mock_stepfunctions

INGESTION_BUCKET = "invoice-ingestion"
STATE_MACHINE_ARN = "arn:aws:states:us-east-1:123:stateMachine:invoice"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("INGESTION_BUCKET_NAME", INGESTION_BUCKET)
    monkeypatch.setenv("STATE_MACHINE_ARN", STATE_MACHINE_ARN)

    
    @mock_s3
    @mock_stepfunctions
def test_ingestion_start_execution():
    bucket = boto3.client("s3", region_name="us-east-1")
    bucket.create_bucket(Bucket=INGESTION_BUCKET)
    bucket.put_object(Bucket=INGESTION_BUCKET, Key="uploads/sample.pdf", Body=b"data")

    sf = boto3.client("stepfunctions", region_name="us-east-1")
    sf.create_state_machine(name="invoice", definition=json.dumps({"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}), roleArn="arn:aws:iam::123:role/demo")

    from ui.app import _start_execution

    with patch("ui.app.stepfunctions_client", sf):
        arn = _start_execution("uploads/sample.pdf")
    assert arn.startswith("arn:aws:states")


def test_ingestion_start_execution_error(monkeypatch):
    mock_client = MagicMock()
    mock_client.start_execution.side_effect = Exception("boom")
    monkeypatch.setattr("ui.app.stepfunctions_client", mock_client)

    from ui.app import _start_execution

    assert _start_execution("key") is None
    mock_client.start_execution.assert_called_once()
