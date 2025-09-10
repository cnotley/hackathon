"""
Test suite for comparison and discrepancy flagging component.

This module tests the comparison Lambda function that analyzes extracted invoice data
against MSA rates and flags discrepancies using Bedrock and SageMaker.
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
import boto3
from moto import mock_dynamodb, mock_s3
import pandas as pd
import numpy as np
import os
import importlib
from typing import Dict, Any

os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-1')
comparison_module = importlib.import_module('lambda.comparison_lambda')
MSARatesComparator = comparison_module.MSARatesComparator
BedrockAnalyzer = comparison_module.BedrockAnalyzer
AnomalyDetector = comparison_module.AnomalyDetector
DiscrepancyFlaggingEngine = comparison_module.DiscrepancyFlaggingEngine
lambda_handler = comparison_module.lambda_handler


class TestComparisonLambda:
    """Test cases for comparison Lambda function."""

    @pytest.fixture
    def sample_extraction_data(self):
        """Sample extracted invoice data for testing."""
        return {
            "invoice_number": "INV-2024-001",
            "vendor": "ABC Construction",
            "total_amount": 76160.00,
            "labor_entries": [
                {
                    "worker_name": "John Smith",
                    "labor_type": "RS",
                    "hours": 45.0,
                    "rate": 73.50,
                    "total": 3307.50,
                    "week": "2024-W01"
                },
                {
                    "worker_name": "Jane Doe", 
                    "labor_type": "SS",
                    "hours": 40.0,
                    "rate": 99.75,
                    "total": 3990.00,
                    "week": "2024-W01"
                },
                {
                    "worker_name": "Bob Wilson",
                    "labor_type": "EN",
                    "hours": 50.0,
                    "rate": 131.25,
                    "total": 6562.50,
                    "week": "2024-W01"
                }
            ],
            "material_entries": [
                {
                    "description": "Safety respirators",
                    "quantity": 10,
                    "unit_price": 631.30,
                    "total": 6313.00
                },
                {
                    "description": "Steel pipes",
                    "quantity": 100,
                    "unit_price": 45.50,
                    "total": 4550.00
                }
            ]
        }

    @pytest.fixture
    def sample_msa_rates(self):
        """Sample MSA rates for testing."""
        return [
            {"labor_type": "RS", "location": "default", "standard_rate": 70.00},
            {"labor_type": "SS", "location": "default", "standard_rate": 95.00},
            {"labor_type": "EN", "location": "default", "standard_rate": 125.00},
            {"labor_type": "default", "location": "overtime_rules", "weekly_threshold": 40.0}
        ]

    @mock_dynamodb
    @mock_s3
    def test_msa_rates_comparator(self, sample_msa_rates):
        """Test MSA rates comparison functionality."""
        # Setup mock DynamoDB
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.create_table(
            TableName='msa-rates',
            KeySchema=[
                {'AttributeName': 'labor_type', 'KeyType': 'HASH'},
                {'AttributeName': 'location', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'labor_type', 'AttributeType': 'S'},
                {'AttributeName': 'location', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Populate with sample data
        for rate in sample_msa_rates:
            table.put_item(Item=rate)

        # Import and test MSARatesComparator
        with patch.dict('os.environ', {'MSA_RATES_TABLE': 'msa-rates'}):
            
            comparator = MSARatesComparator()
            
            # Test rate lookup
            rs_rate = comparator.get_standard_rate('RS', 'default')
            assert rs_rate == 70.00
            
            # Test overtime threshold
            threshold = comparator.get_overtime_threshold('default')
            assert threshold == 40.0
            
            # Test rate variance calculation
            variance = comparator.calculate_rate_variance(73.50, 70.00)
            assert variance == 5.0

    @patch('lambda.comparison_lambda.boto3.client')
    def test_bedrock_analyzer(self, mock_boto_client):
        """Test Bedrock analysis functionality."""
        # Mock Bedrock client
        mock_bedrock = Mock()
        mock_boto_client.return_value = mock_bedrock
        
        mock_bedrock.invoke_model.return_value = {
            'body': Mock(read=lambda: json.dumps({
                'content': [{
                    'text': 'Analysis: RS rate overcharge detected. Standard rate $70.00, charged $73.50. Variance 5.0% exceeds threshold.'
                }]
            }).encode())
        }
        
        analyzer = BedrockAnalyzer()
        
        discrepancy_data = {
            "labor_type": "RS",
            "charged_rate": 73.50,
            "standard_rate": 70.00,
            "variance": 5.0
        }
        
        analysis = analyzer.analyze_discrepancy(discrepancy_data)
        
        assert "RS rate overcharge" in analysis
        assert "5.0%" in analysis
        mock_bedrock.invoke_model.assert_called_once()

    @patch('lambda.comparison_lambda.boto3.client')
    def test_sagemaker_anomaly_detection(self, mock_boto_client):
        """Test SageMaker anomaly detection."""
        # Mock SageMaker client
        mock_sagemaker = Mock()
        mock_boto_client.return_value = mock_sagemaker
        
        # Mock SageMaker response (anomaly scores)
        mock_sagemaker.invoke_endpoint.return_value = {
            'Body': Mock(read=lambda: json.dumps([0.1, 0.8, 0.2]).encode())
        }
        
        detector = AnomalyDetector()
        
        # Test data with one clear anomaly
        test_data = [
            {"amount": 1000.0, "description": "Normal item"},
            {"amount": 6313.0, "description": "Expensive respirators"},  # Anomaly
            {"amount": 1500.0, "description": "Another normal item"}
        ]
        
        anomalies = detector.detect_anomalies(test_data)
        
        assert len(anomalies) == 1
        assert anomalies[0]["description"] == "Expensive respirators"
        assert anomalies[0]["anomaly_score"] == 0.8
        mock_sagemaker.invoke_endpoint.assert_called_once()

    def test_statistical_anomaly_fallback(self):
        """Test statistical anomaly detection fallback."""
        
        detector = AnomalyDetector()
        
        # Test data with clear statistical anomaly
        amounts = [1000, 1100, 1200, 1050, 6313, 1150]  # 6313 is clear outlier
        
        anomalies = detector._statistical_anomaly_detection(amounts)
        
        assert len(anomalies) > 0
        assert 6313 in [a["amount"] for a in anomalies]
        assert all(a["z_score"] > 2.0 for a in anomalies)

    @patch('lambda.comparison_lambda.MSARatesComparator')
    @patch('lambda.comparison_lambda.BedrockAnalyzer')
    @patch('lambda.comparison_lambda.AnomalyDetector')
    def test_discrepancy_flagging_engine(self, mock_anomaly, mock_bedrock, mock_comparator, 
                                       sample_extraction_data):
        """Test complete discrepancy flagging engine."""
        # Setup mocks
        mock_comp_instance = Mock()
        mock_bedrock_instance = Mock()
        mock_anomaly_instance = Mock()
        
        mock_comparator.return_value = mock_comp_instance
        mock_bedrock.return_value = mock_bedrock_instance
        mock_anomaly.return_value = mock_anomaly_instance
        
        # Configure mock responses
        mock_comp_instance.get_standard_rate.side_effect = lambda labor_type, location: {
            'RS': 70.00, 'SS': 95.00, 'EN': 125.00
        }.get(labor_type, 0.0)
        
        mock_comp_instance.get_overtime_threshold.return_value = 40.0
        mock_comp_instance.calculate_rate_variance.side_effect = lambda charged, standard: (
            ((charged - standard) / standard) * 100
        )
        
        mock_bedrock_instance.analyze_discrepancy.return_value = "Rate variance analysis"
        
        mock_anomaly_instance.detect_anomalies.return_value = [
            {
                "item": "Safety respirators",
                "amount": 6313.00,
                "anomaly_score": 0.8,
                "z_score": 2.5
            }
        ]
        
        
        engine = DiscrepancyFlaggingEngine()
        result = engine.analyze_invoice(sample_extraction_data)
        
        # Verify structure
        assert "rate_variances" in result
        assert "overtime_violations" in result
        assert "anomalies" in result
        assert "total_savings" in result
        
        # Verify rate variances detected
        assert len(result["rate_variances"]) == 3
        
        # Verify overtime violations detected
        assert len(result["overtime_violations"]) == 2
        
        # Verify anomalies detected
        assert len(result["anomalies"]) == 1
        assert result["anomalies"][0]["item"] == "Safety respirators"

    @patch.dict('os.environ', {
        'MSA_RATES_TABLE': 'test-table',
        'SAGEMAKER_ENDPOINT': 'test-endpoint',
        'BEDROCK_MODEL_ID': 'test-model'
    })
    @patch('lambda.comparison_lambda.DiscrepancyFlaggingEngine')
    def test_lambda_handler(self, mock_engine, sample_extraction_data):
        """Test Lambda handler function."""
        # Setup mock engine
        mock_engine_instance = Mock()
        mock_engine.return_value = mock_engine_instance
        
        mock_engine_instance.analyze_invoice.return_value = {
            "rate_variances": [],
            "overtime_violations": [],
            "anomalies": [],
            "total_savings": 0.0,
            "summary": {"total_discrepancies": 0}
        }
        
        
        # Test event
        event = {
            "extraction_data": sample_extraction_data,
            "bucket": "test-bucket",
            "key": "test-invoice.pdf"
        }
        
        context = Mock()
        
        response = lambda_handler(event, context)
        
        assert response["statusCode"] == 200
        assert "discrepancy_analysis" in json.loads(response["body"])
        mock_engine_instance.analyze_invoice.assert_called_once_with(sample_extraction_data)

    def test_duplicate_detection(self, sample_extraction_data):
        """Test duplicate entry detection."""
        # Add duplicate entries to test data
        duplicate_data = sample_extraction_data.copy()
        duplicate_data["labor_entries"].append({
            "worker_name": "John Smith",  # Duplicate worker
            "labor_type": "RS",
            "hours": 40.0,
            "rate": 70.00,
            "total": 2800.00,
            "week": "2024-W01"
        })
        
        
        engine = DiscrepancyFlaggingEngine()
        duplicates = engine._detect_duplicates(duplicate_data)
        
        assert len(duplicates) > 0
        assert any("John Smith" in dup["description"] for dup in duplicates)

    def test_checklist_flag_generation(self):
        """Test checklist flag generation."""
        
        engine = DiscrepancyFlaggingEngine()
        
        # Test overtime flag
        overtime_data = {"worker": "John Smith", "hours": 45.0, "threshold": 40.0}
        flag = engine._generate_checklist_flag("overtime", overtime_data)
        
        assert "Overtime: support with time sheets" in flag
        
        # Test rate variance flag
        variance_data = {"labor_type": "RS", "variance": 7.5}
        flag = engine._generate_checklist_flag("rate_variance", variance_data)
        
        assert "Rate variance exceeds 5% threshold" in flag

    def test_savings_calculation(self, sample_extraction_data):
        """Test potential savings calculation."""
        
        engine = DiscrepancyFlaggingEngine()
        
        # Mock rate variances with savings
        rate_variances = [
            {"savings": 157.50},
            {"savings": 190.00},
            {"savings": 312.50}
        ]
        
        total_savings = engine._calculate_total_savings(rate_variances)
        assert total_savings == 660.00

    @patch('boto3.client')
    def test_error_handling(self, mock_boto_client):
        """Test error handling in comparison Lambda."""
        # Mock client that raises exception
        mock_boto_client.side_effect = Exception("AWS service error")
        
        
        event = {
            "extraction_data": {"invalid": "data"},
            "bucket": "test-bucket",
            "key": "test-invoice.pdf"
        }
        
        context = Mock()
        
        response = lambda_handler(event, context)
        
        assert response["statusCode"] == 500
        assert "error" in json.loads(response["body"])

    def test_chunked_context_handling(self):
        """Test handling of chunked context from Bedrock KB."""
        
        analyzer = BedrockAnalyzer()
        
        # Test chunked context processing
        chunked_context = [
            {"text": "MSA standard rates: RS $70.00", "metadata": {"page": 1}},
            {"text": "Overtime rules: >40 hours/week", "metadata": {"page": 2}},
            {"text": "Rate variance threshold: 5%", "metadata": {"page": 1}}
        ]
        
        processed_context = analyzer._process_chunked_context(chunked_context)
        
        assert "RS $70.00" in processed_context
        assert "40 hours/week" in processed_context
        assert "5%" in processed_context


if __name__ == "__main__":
    pytest.main([__file__])
