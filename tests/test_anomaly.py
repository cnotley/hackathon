import importlib
import json
import os
from unittest.mock import Mock

import boto3
import pandas as pd
import pytest
from moto import mock_dynamodb, mock_s3


@pytest.fixture
def anomaly_module(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("SAGEMAKER_ENDPOINT", "test-endpoint")
    module = importlib.import_module("lambda.comparison_lambda")
    return module


def _seed_msa_table():
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
        Item={
            "labor_type": "RS",
            "location": "default",
            "standard_rate": 70.0,
        }
    )
    return table


@mock_dynamodb
@mock_s3
def test_sagemaker_detector_returns_flag(monkeypatch, anomaly_module):
    _seed_msa_table()

    mock_sagemaker = Mock()
    mock_sagemaker.invoke_endpoint.return_value = {
        "Body": Mock(read=lambda: json.dumps({"predictions": [0.2, 1.4]}).encode())
    }

    monkeypatch.setattr(anomaly_module, "sagemaker_client", mock_sagemaker)

    detector = anomaly_module.AnomalyDetector()
    extracted = {
        "normalized_data": {
            "labor": [
                {"name": "Worker A", "type": "RS", "total_hours": 35, "unit_price": 77, "total_cost": 2695},
                {"name": "Worker B", "type": "RS", "total_hours": 60, "unit_price": 90, "total_cost": 5400},
            ]
        }
    }

    anomalies = detector.detect_anomalies(extracted)

    assert any(a["category"] == "sagemaker_isolation_forest" for a in anomalies)
    assert any(a["category"] == "overtime_spike" for a in anomalies)
    mock_sagemaker.invoke_endpoint.assert_called_once()


@mock_dynamodb
def test_anomaly_detector_fallback(monkeypatch, anomaly_module):
    table = _seed_msa_table()

    mock_sagemaker = Mock()
    mock_sagemaker.invoke_endpoint.side_effect = Exception("endpoint down")
    monkeypatch.setattr(anomaly_module, "sagemaker_client", mock_sagemaker)

    detector = anomaly_module.AnomalyDetector()
    extracted = {
        "normalized_data": {
            "labor": [
                {"name": "Worker A", "type": "RS", "total_hours": 35, "unit_price": 77, "total_cost": 2695},
                {"name": "Worker B", "type": "RS", "total_hours": 40, "unit_price": 70, "total_cost": 2800},
                {"name": "Worker C", "type": "RS", "total_hours": 41, "unit_price": 70, "total_cost": 2870},
            ]
        }
    }

    anomalies = detector.detect_anomalies(extracted)

    assert any(a["category"] == "statistical_outlier" for a in anomalies) or any(a["category"] == "overtime_spike" for a in anomalies)
    assert mock_sagemaker.invoke_endpoint.call_count >= 1
