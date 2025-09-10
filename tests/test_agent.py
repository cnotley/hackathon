"""
Tests for the AI Agent Lambda function.

This module contains unit tests for the agent functionality including
MSA rate auditing, Bedrock agent interactions, and invoice processing.
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from moto import mock_dynamodb, mock_s3, mock_lambda
import boto3
from decimal import Decimal
from datetime import datetime

# Import the agent Lambda function
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lambda'))

from agent_lambda import (
    MSARatesManager,
    InvoiceAuditor,
    BedrockAgentManager,
    lambda_handler,
    handle_audit_request,
    call_extraction_lambda
)


class TestMSARatesManager:
    """Test cases for MSARatesManager class."""
    
    @mock_dynamodb
    def test_get_rate_for_labor_type(self):
        """Test retrieving MSA rates from DynamoDB."""
        # Setup DynamoDB table
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
        
        # Add sample data
        table.put_item(Item={
            'labor_type': 'RS',
            'location': 'default',
            'standard_rate': Decimal('70.00'),
            'description': 'Regular Skilled Labor'
        })
        
        # Test the manager
        with patch.dict(os.environ, {'MSA_RATES_TABLE': 'msa-rates'}):
            manager = MSARatesManager()
            rate = manager.get_rate_for_labor_type('RS')
            
            assert rate == 70.00
    
    @mock_dynamodb
    def test_get_overtime_threshold(self):
        """Test retrieving overtime thresholds."""
        # Setup DynamoDB table
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
        
        # Add overtime rules
        table.put_item(Item={
            'labor_type': 'default',
            'location': 'overtime_rules',
            'weekly_threshold': Decimal('40.0'),
            'description': 'Standard overtime threshold'
        })
        
        # Test the manager
        with patch.dict(os.environ, {'MSA_RATES_TABLE': 'msa-rates'}):
            manager = MSARatesManager()
            threshold = manager.get_overtime_threshold()
            
            assert threshold == 40.0


class TestInvoiceAuditor:
    """Test cases for InvoiceAuditor class."""
    
    def test_audit_extracted_data_rate_variance(self):
        """Test auditing with rate variance detection."""
        # Mock MSA rates manager
        mock_manager = Mock()
        mock_manager.get_rate_for_labor_type.return_value = 70.00  # MSA rate
        mock_manager.get_overtime_threshold.return_value = 40.0
        
        # Create auditor with mocked manager
        auditor = InvoiceAuditor()
        auditor.msa_manager = mock_manager
        
        # Sample extracted data with rate variance
        extracted_data = {
            'normalized_data': {
                'labor': [
                    {
                        'name': 'Smith, John',
                        'type': 'RS',
                        'unit_price': 77.00,  # 10% higher than MSA rate of $70
                        'total_hours': 35.0,
                        'total_cost': 2695.00
                    }
                ]
            }
        }
        
        # Perform audit
        result = auditor.audit_extracted_data(extracted_data)
        
        # Assertions
        assert result['summary']['total_discrepancies'] == 1
        assert result['summary']['rate_variances'] == 1
        assert result['summary']['overtime_violations'] == 0
        
        # Check discrepancy details
        discrepancy = result['discrepancies'][0]
        assert discrepancy['type'] == 'rate_variance'
        assert discrepancy['worker'] == 'Smith, John'
        assert discrepancy['actual_rate'] == 77.00
        assert discrepancy['msa_rate'] == 70.00
        assert discrepancy['variance_percentage'] == 10.0
    
    def test_audit_extracted_data_overtime_violation(self):
        """Test auditing with overtime violation detection."""
        # Mock MSA rates manager
        mock_manager = Mock()
        mock_manager.get_rate_for_labor_type.return_value = 70.00
        mock_manager.get_overtime_threshold.return_value = 40.0
        
        # Create auditor with mocked manager
        auditor = InvoiceAuditor()
        auditor.msa_manager = mock_manager
        
        # Sample extracted data with overtime violation
        extracted_data = {
            'normalized_data': {
                'labor': [
                    {
                        'name': 'Doe, Jane',
                        'type': 'RS',
                        'unit_price': 70.00,  # Matches MSA rate
                        'total_hours': 45.0,  # Exceeds 40-hour threshold
                        'total_cost': 3150.00
                    }
                ]
            }
        }
        
        # Perform audit
        result = auditor.audit_extracted_data(extracted_data)
        
        # Assertions
        assert result['summary']['total_discrepancies'] == 1
        assert result['summary']['rate_variances'] == 0
        assert result['summary']['overtime_violations'] == 1
        
        # Check discrepancy details
        discrepancy = result['discrepancies'][0]
        assert discrepancy['type'] == 'overtime_violation'
        assert discrepancy['worker'] == 'Doe, Jane'
        assert discrepancy['total_hours'] == 45.0
        assert discrepancy['overtime_hours'] == 5.0
        assert discrepancy['threshold'] == 40.0


class TestBedrockAgentManager:
    """Test cases for BedrockAgentManager class."""
    
    def test_invoke_agent_success(self):
        """Test successful Bedrock agent invocation."""
        # Mock Bedrock agent client
        mock_response = {
            'completion': [
                {
                    'chunk': {
                        'bytes': b'This invoice shows a rate variance for RS labor.'
                    }
                }
            ]
        }
        
        with patch.dict(os.environ, {
            'BEDROCK_AGENT_ID': 'test-agent-id',
            'BEDROCK_AGENT_ALIAS_ID': 'test-alias-id'
        }):
            manager = BedrockAgentManager()
            
            with patch.object(manager, 'bedrock_agent_client') as mock_client:
                mock_client.invoke_agent.return_value = mock_response
                
                result = manager.invoke_agent("Audit this invoice against MSA standards")
                
                assert result['status'] == 'success'
                assert 'rate variance' in result['response']
                assert 'session_id' in result
    
    def test_invoke_agent_error(self):
        """Test Bedrock agent invocation error handling."""
        with patch.dict(os.environ, {
            'BEDROCK_AGENT_ID': 'test-agent-id',
            'BEDROCK_AGENT_ALIAS_ID': 'test-alias-id'
        }):
            manager = BedrockAgentManager()
            
            with patch.object(manager, 'bedrock_agent_client') as mock_client:
                mock_client.invoke_agent.side_effect = Exception("Bedrock error")
                
                result = manager.invoke_agent("Test query")
                
                assert result['status'] == 'error'
                assert 'Bedrock error' in result['error']


class TestCallExtractionLambda:
    """Test cases for extraction Lambda integration."""
    
    @mock_s3
    @mock_lambda
    def test_call_extraction_lambda_success(self):
        """Test successful extraction Lambda invocation."""
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(
            Bucket='test-bucket',
            Key='test-invoice.pdf',
            Body=b'mock pdf content'
        )
        
        # Mock Lambda response
        mock_extraction_result = {
            'extraction_status': 'completed',
            'extracted_data': {
                'text_blocks': [
                    {'text': 'Total Labor: $76,160.00', 'confidence': 95.0}
                ]
            },
            'normalized_data': {
                'labor': [
                    {
                        'name': 'Smith, John',
                        'type': 'RS',
                        'unit_price': 77.00,
                        'total_hours': 55.0
                    }
                ]
            }
        }
        
        with patch('agent_lambda.lambda_client') as mock_lambda_client:
            mock_response = Mock()
            mock_response.get.return_value = 200
            mock_response.__getitem__.return_value = 200
            mock_payload = Mock()
            mock_payload.read.return_value = json.dumps(mock_extraction_result).encode()
            mock_response.__getitem__.return_value = mock_payload
            mock_lambda_client.invoke.return_value = {
                'StatusCode': 200,
                'Payload': mock_payload
            }
            
            result = call_extraction_lambda('test-bucket', 'test-invoice.pdf')
            
            assert result['extraction_status'] == 'completed'
            assert 'normalized_data' in result
            assert len(result['normalized_data']['labor']) == 1


class TestLambdaHandler:
    """Test cases for the main Lambda handler."""
    
    def test_lambda_handler_audit_action(self):
        """Test Lambda handler with audit action."""
        # Mock all dependencies
        mock_extraction_result = {
            'extraction_status': 'completed',
            'extracted_data': {
                'text_blocks': [{'text': 'RS Labor: $77/hour', 'confidence': 95.0}]
            },
            'normalized_data': {
                'labor': [
                    {
                        'name': 'Smith, John',
                        'type': 'RS',
                        'unit_price': 77.00,  # Higher than MSA rate of $70
                        'total_hours': 35.0,
                        'total_cost': 2695.00
                    }
                ]
            }
        }
        
        mock_agent_response = {
            'status': 'success',
            'response': 'Audit completed. Found rate variance: RS labor at $77 vs MSA standard $70.',
            'session_id': 'test-session'
        }
        
        event = {
            'action': 'audit',
            'bucket': 'test-bucket',
            'key': 'test-invoice.pdf',
            'query': 'Audit this invoice against MSA standards'
        }
        
        with patch('agent_lambda.call_extraction_lambda', return_value=mock_extraction_result), \
             patch('agent_lambda.InvoiceAuditor') as mock_auditor_class, \
             patch('agent_lambda.BedrockAgentManager') as mock_agent_class:
            
            # Setup mocks
            mock_auditor = Mock()
            mock_auditor.audit_extracted_data.return_value = {
                'audit_id': 'test-audit-123',
                'timestamp': '2025-01-09T20:50:00Z',
                'discrepancies': [
                    {
                        'type': 'rate_variance',
                        'severity': 'medium',
                        'worker': 'Smith, John',
                        'labor_type': 'RS',
                        'actual_rate': 77.00,
                        'msa_rate': 70.00,
                        'variance_percentage': 10.0
                    }
                ],
                'summary': {
                    'total_discrepancies': 1,
                    'rate_variances': 1,
                    'overtime_violations': 0
                }
            }
            mock_auditor_class.return_value = mock_auditor
            
            mock_agent = Mock()
            mock_agent.invoke_agent.return_value = mock_agent_response
            mock_agent_class.return_value = mock_agent
            
            # Execute handler
            result = lambda_handler(event, {})
            
            # Assertions
            assert result['status'] == 'completed'
            assert result['audit_id'] == 'test-audit-123'
            assert result['audit_results']['summary']['rate_variances'] == 1
            assert 'rate variance' in result['recommendations']
    
    def test_lambda_handler_query_action(self):
        """Test Lambda handler with direct query action."""
        event = {
            'action': 'query',
            'query': 'What are the standard MSA rates for RS labor?'
        }
        
        mock_agent_response = {
            'status': 'success',
            'response': 'Standard MSA rate for RS labor is $70.00 per hour.',
            'session_id': 'test-session'
        }
        
        with patch('agent_lambda.BedrockAgentManager') as mock_agent_class:
            mock_agent = Mock()
            mock_agent.invoke_agent.return_value = mock_agent_response
            mock_agent_class.return_value = mock_agent
            
            result = lambda_handler(event, {})
            
            assert result['status'] == 'success'
            assert '$70.00' in result['response']
    
    def test_lambda_handler_error_handling(self):
        """Test Lambda handler error handling."""
        event = {
            'action': 'audit',
            'bucket': 'test-bucket',
            'key': 'nonexistent-file.pdf'
        }
        
        with patch('agent_lambda.call_extraction_lambda', side_effect=Exception("File not found")):
            result = lambda_handler(event, {})
            
            assert result['statusCode'] == 500
            body = json.loads(result['body'])
            assert 'File not found' in body['error']


class TestIntegrationScenarios:
    """Integration test scenarios for complete audit workflows."""
    
    def test_complete_audit_workflow_with_discrepancies(self):
        """Test complete audit workflow that finds discrepancies."""
        # Sample invoice data with known discrepancies
        extracted_data = {
            'extraction_status': 'completed',
            'extracted_data': {
                'text_blocks': [
                    {'text': 'Invoice Total: $76,160.00', 'confidence': 95.0, 'page': 22}
                ],
                'page_count': 22
            },
            'normalized_data': {
                'labor': [
                    {
                        'name': 'Smith, John',
                        'type': 'RS',
                        'unit_price': 77.00,  # $7 higher than MSA standard of $70
                        'total_hours': 55.0,  # 15 hours overtime
                        'total_cost': 4235.00
                    },
                    {
                        'name': 'Doe, Jane',
                        'type': 'US',
                        'unit_price': 85.00,  # Matches MSA standard
                        'total_hours': 38.0,  # No overtime
                        'total_cost': 3230.00
                    }
                ]
            }
        }
        
        # Mock MSA rates
        mock_msa_manager = Mock()
        mock_msa_manager.get_rate_for_labor_type.side_effect = lambda labor_type, location='default': {
            'RS': 70.00,
            'US': 85.00
        }.get(labor_type)
        mock_msa_manager.get_overtime_threshold.return_value = 40.0
        
        # Create auditor and perform audit
        auditor = InvoiceAuditor()
        auditor.msa_manager = mock_msa_manager
        
        audit_results = auditor.audit_extracted_data(extracted_data)
        
        # Verify audit results
        assert audit_results['summary']['total_discrepancies'] == 2  # Rate variance + overtime
        assert audit_results['summary']['rate_variances'] == 1
        assert audit_results['summary']['overtime_violations'] == 1
        
        # Check specific discrepancies
        rate_discrepancy = next(d for d in audit_results['discrepancies'] if d['type'] == 'rate_variance')
        assert rate_discrepancy['worker'] == 'Smith, John'
        assert rate_discrepancy['actual_rate'] == 77.00
        assert rate_discrepancy['msa_rate'] == 70.00
        assert rate_discrepancy['variance_percentage'] == 10.0
        
        overtime_discrepancy = next(d for d in audit_results['discrepancies'] if d['type'] == 'overtime_violation')
        assert overtime_discrepancy['worker'] == 'Smith, John'
        assert overtime_discrepancy['total_hours'] == 55.0
        assert overtime_discrepancy['overtime_hours'] == 15.0
    
    def test_audit_workflow_no_discrepancies(self):
        """Test audit workflow with compliant invoice."""
        # Sample compliant invoice data
        extracted_data = {
            'extraction_status': 'completed',
            'normalized_data': {
                'labor': [
                    {
                        'name': 'Johnson, Bob',
                        'type': 'RS',
                        'unit_price': 70.00,  # Matches MSA standard
                        'total_hours': 35.0,  # No overtime
                        'total_cost': 2450.00
                    }
                ]
            }
        }
        
        # Mock MSA rates
        mock_msa_manager = Mock()
        mock_msa_manager.get_rate_for_labor_type.return_value = 70.00
        mock_msa_manager.get_overtime_threshold.return_value = 40.0
        
        # Create auditor and perform audit
        auditor = InvoiceAuditor()
        auditor.msa_manager = mock_msa_manager
        
        audit_results = auditor.audit_extracted_data(extracted_data)
        
        # Verify no discrepancies found
        assert audit_results['summary']['total_discrepancies'] == 0
        assert audit_results['summary']['rate_variances'] == 0
        assert audit_results['summary']['overtime_violations'] == 0
        assert len(audit_results['discrepancies']) == 0


if __name__ == '__main__':
    pytest.main([__file__])
