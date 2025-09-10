"""
Tests for MSA Invoice Auditing System UI

This module contains comprehensive tests for the Streamlit UI application,
including unit tests for the MSAInvoiceAuditor class and integration tests.
"""

import pytest
import boto3
import json
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from moto import mock_s3, mock_stepfunctions, mock_sts
from datetime import datetime
import io
import sys
import os

# Add the ui directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ui'))

# Import the UI module
from app import MSAInvoiceAuditor


class TestMSAInvoiceAuditor:
    """Test cases for the MSAInvoiceAuditor class."""
    
    @pytest.fixture
    def auditor(self):
        """Create a test instance of MSAInvoiceAuditor."""
        with patch('streamlit.secrets') as mock_secrets:
            mock_secrets.get.side_effect = lambda key, default=None: {
                'INGESTION_BUCKET': 'test-ingestion-bucket',
                'REPORTS_BUCKET': 'test-reports-bucket',
                'STEP_FUNCTION_ARN': 'arn:aws:states:us-east-1:123456789012:stateMachine:test-workflow',
                'BEDROCK_AGENT_ID': 'test-agent-id',
                'BEDROCK_AGENT_ALIAS_ID': 'TSTALIASID'
            }.get(key, default)
        with patch('boto3.client') as mock_boto3:
            # Mock AWS clients
            mock_s3 = Mock()
            mock_stepfunctions = Mock()
            mock_bedrock = Mock()
            
            mock_boto3.side_effect = lambda service: {
                's3': mock_s3,
                'stepfunctions': mock_stepfunctions,
                'bedrock-agent-runtime': mock_bedrock
            }[service]
            
            auditor = MSAInvoiceAuditor()
            auditor.s3_client = mock_s3
            auditor.stepfunctions_client = mock_stepfunctions
            auditor.bedrock_agent_client = mock_bedrock
            
            return auditor
    
    def test_init_success(self):
        """Test successful initialization of MSAInvoiceAuditor."""
        with patch('streamlit.secrets') as mock_secrets:
            mock_secrets.get.side_effect = lambda key, default=None: {
                'INGESTION_BUCKET': 'test-ingestion-bucket',
                'REPORTS_BUCKET': 'test-reports-bucket',
                'STEP_FUNCTION_ARN': 'arn:aws:states:us-east-1:123456789012:stateMachine:test-workflow',
                'BEDROCK_AGENT_ID': 'test-agent-id',
                'BEDROCK_AGENT_ALIAS_ID': 'TSTALIASID'
            }.get(key, default)
            with patch('boto3.client') as mock_boto3:
                mock_boto3.return_value = Mock()
                auditor = MSAInvoiceAuditor()
        assert auditor.ingestion_bucket == 'test-ingestion-bucket'
        assert auditor.reports_bucket == 'test-reports-bucket'
        assert auditor.step_function_arn == 'arn:aws:states:us-east-1:123456789012:stateMachine:test-workflow'
        assert auditor.bedrock_agent_id == 'test-agent-id'
    
    def test_get_content_type(self, auditor):
        """Test content type detection for PDF and unsupported types."""
        assert auditor._get_content_type('invoice.pdf') == 'application/pdf'
        for filename in ['data.xlsx', 'data.xls', 'image.jpg', 'image.png', 'unknown.txt']:
            assert auditor._get_content_type(filename) == 'application/octet-stream'
    
    def test_upload_file_to_s3_success(self, auditor):
        """Test successful file upload to S3."""
        # Mock S3 put_object
        auditor.s3_client.put_object.return_value = {}
        
        file_content = b'test file content'
        filename = 'test_invoice.pdf'
        
        with patch('datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = '20241201_143000'
            
            result = auditor.upload_file_to_s3(file_content, filename)
            
            assert result == 'uploads/20241201_143000_test_invoice.pdf'
            auditor.s3_client.put_object.assert_called_once_with(
                Bucket='test-ingestion-bucket',
                Key='uploads/20241201_143000_test_invoice.pdf',
                Body=file_content,
                ContentType='application/pdf'
            )
    
    def test_upload_file_to_s3_error(self, auditor):
        """Test S3 upload error handling."""
        from botocore.exceptions import ClientError
        
        # Mock S3 put_object to raise an error
        auditor.s3_client.put_object.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
            'PutObject'
        )
        
        with patch('streamlit.error') as mock_error:
            result = auditor.upload_file_to_s3(b'content', 'test.pdf')
            
            assert result is None
            mock_error.assert_called_once()
    
    def test_start_step_function_execution_success(self, auditor):
        """Test successful Step Functions execution start."""
        auditor.stepfunctions_client.start_execution.return_value = {
            'executionArn': 'arn:aws:states:us-east-1:123456789012:execution:test-workflow:test-execution'
        }
        
        s3_key = 'uploads/test_file.pdf'
        query = 'Compare to MSA'
        
        with patch('time.time', return_value=1234567890):
            result = auditor.start_step_function_execution(s3_key, query)
            
            assert result == 'arn:aws:states:us-east-1:123456789012:execution:test-workflow:test-execution'
            
            # Verify the call was made with correct parameters
            call_args = auditor.stepfunctions_client.start_execution.call_args
            assert call_args[1]['stateMachineArn'] == auditor.step_function_arn
            assert call_args[1]['name'] == 'audit-1234567890'
            
            # Parse and verify input
            input_data = json.loads(call_args[1]['input'])
            assert input_data['bucket'] == 'test-ingestion-bucket'
            assert input_data['key'] == s3_key
            assert input_data['query'] == query
    
    def test_get_execution_status_success(self, auditor):
        """Test successful execution status retrieval."""
        mock_response = {
            'status': 'SUCCEEDED',
            'startDate': datetime(2024, 12, 1, 14, 30, 0),
            'stopDate': datetime(2024, 12, 1, 14, 35, 0),
            'input': '{"bucket": "test-bucket", "key": "test-key"}'
        }
        
        auditor.stepfunctions_client.describe_execution.return_value = mock_response
        
        execution_arn = 'arn:aws:states:us-east-1:123456789012:execution:test-workflow:test-execution'
        result = auditor.get_execution_status(execution_arn)
        
        assert result == mock_response
        auditor.stepfunctions_client.describe_execution.assert_called_once_with(
            executionArn=execution_arn
        )
    
    def test_invoke_bedrock_agent_success(self, auditor):
        """Test successful Bedrock Agent invocation."""
        # Mock streaming response
        mock_response = {
            'completion': [
                {
                    'chunk': {
                        'bytes': b'This is the agent response about MSA rates.'
                    }
                }
            ]
        }
        
        auditor.bedrock_agent_client.invoke_agent.return_value = mock_response
        
        query = 'What are the standard labor rates for RS work?'
        session_id = 'test-session-123'
        
        result = auditor.invoke_bedrock_agent(query, session_id)
        
        assert result['response'] == 'This is the agent response about MSA rates.'
        assert result['session_id'] == session_id
        
        auditor.bedrock_agent_client.invoke_agent.assert_called_once_with(
            agentId='test-agent-id',
            agentAliasId='TSTALIASID',
            sessionId=session_id,
            inputText=query
        )
    
    def test_list_reports_success(self, auditor):
        """Test successful report listing."""
        mock_response = {
            'Contents': [
                {
                    'Key': 'reports/test_invoice/report.xlsx',
                    'Size': 1024,
                    'LastModified': datetime(2024, 12, 1, 14, 30, 0)
                },
                {
                    'Key': 'reports/test_invoice/report.pdf',
                    'Size': 2048,
                    'LastModified': datetime(2024, 12, 1, 14, 35, 0)
                }
            ]
        }
        
        auditor.s3_client.list_objects_v2.return_value = mock_response
        
        s3_key_prefix = 'uploads/20241201_143000_test_invoice.pdf'
        result = auditor.list_reports(s3_key_prefix)
        
        assert len(result) == 2
        assert result[0]['type'] == 'PDF Report'  # Sorted by last_modified desc
        assert result[1]['type'] == 'Excel Report'
        
        auditor.s3_client.list_objects_v2.assert_called_once_with(
            Bucket='test-reports-bucket',
            Prefix='reports/test_invoice'
        )
    
    def test_get_report_type(self, auditor):
        """Test report type detection."""
        assert auditor._get_report_type('report.xlsx') == 'Excel Report'
        assert auditor._get_report_type('report.pdf') == 'PDF Report'
        assert auditor._get_report_type('report.md') == 'Markdown Report'
        assert auditor._get_report_type('report.txt') == 'Unknown'
    
    def test_download_report_success(self, auditor):
        """Test successful report download."""
        mock_response = {
            'Body': io.BytesIO(b'report content')
        }
        
        auditor.s3_client.get_object.return_value = mock_response
        
        s3_key = 'reports/test_invoice/report.xlsx'
        result = auditor.download_report(s3_key)
        
        assert result == b'report content'
        auditor.s3_client.get_object.assert_called_once_with(
            Bucket='test-reports-bucket',
            Key=s3_key
        )


class TestUIIntegration:
    """Integration tests for the UI application."""
    
    @pytest.fixture
    def sample_flags_data(self):
        """Sample flags data for testing."""
        return [
            {
                "Type": "Overcharge",
                "Description": "RS Labor rate exceeds MSA",
                "Amount": "$375.00",
                "Line Item": "Labor - Restoration Services"
            },
            {
                "Type": "Duplicate",
                "Description": "Duplicate charge detected",
                "Item": "Safety Gloves",
                "Count": 2
            },
            {
                "Type": "Savings",
                "Description": "Potential savings identified",
                "Amount": "10% of $160,000",
                "Category": "Equipment Rental"
            }
        ]
    
    def test_flags_dataframe_creation(self, sample_flags_data):
        """Test creation of flags DataFrame for display."""
        df = pd.DataFrame(sample_flags_data)
        
        assert len(df) == 3
        assert 'Type' in df.columns
        assert 'Description' in df.columns
        
        # Test specific flag content
        overcharge_row = df[df['Type'] == 'Overcharge'].iloc[0]
        assert overcharge_row['Amount'] == '$375.00'
        assert 'RS Labor' in overcharge_row['Description']
        
        duplicate_row = df[df['Type'] == 'Duplicate'].iloc[0]
        assert duplicate_row['Item'] == 'Safety Gloves'
        assert duplicate_row['Count'] == 2
        
        savings_row = df[df['Type'] == 'Savings'].iloc[0]
        assert '10%' in savings_row['Amount']
        assert '$160,000' in savings_row['Amount']
    
    def test_report_flags_validation(self, sample_flags_data):
        """Test that report shows expected flags like 'Duplicate: Gloves'."""
        df = pd.DataFrame(sample_flags_data)
        
        # Check for duplicate gloves flag
        duplicate_flags = df[df['Type'] == 'Duplicate']
        gloves_flag = duplicate_flags[duplicate_flags['Item'].str.contains('Gloves', na=False)]
        
        assert len(gloves_flag) == 1
        assert gloves_flag.iloc[0]['Description'] == 'Duplicate charge detected'
        
        # Check for overcharge flag with specific amount
        overcharge_flags = df[df['Type'] == 'Overcharge']
        rs_labor_flag = overcharge_flags[overcharge_flags['Amount'] == '$375.00']
        
        assert len(rs_labor_flag) == 1
        assert 'RS Labor' in rs_labor_flag.iloc[0]['Description']
    
    @patch('streamlit.secrets')
    @patch('boto3.client')
    def test_ui_configuration_loading(self, mock_boto3, mock_secrets):
        """Test UI configuration loading from secrets."""
        # Mock secrets
        mock_secrets.get.side_effect = lambda key, default=None: {
            'INGESTION_BUCKET': 'prod-ingestion-bucket',
            'REPORTS_BUCKET': 'prod-reports-bucket',
            'STEP_FUNCTION_ARN': 'arn:aws:states:us-east-1:123456789012:stateMachine:prod-workflow',
            'BEDROCK_AGENT_ID': 'prod-agent-id',
            'BEDROCK_AGENT_ALIAS_ID': 'PRODALIASID'
        }.get(key, default)
        
        # Mock AWS clients
        mock_boto3.return_value = Mock()
        
        auditor = MSAInvoiceAuditor()
        
        assert auditor.ingestion_bucket == 'prod-ingestion-bucket'
        assert auditor.reports_bucket == 'prod-reports-bucket'
        assert auditor.step_function_arn == 'arn:aws:states:us-east-1:123456789012:stateMachine:prod-workflow'
        assert auditor.bedrock_agent_id == 'prod-agent-id'
        assert auditor.bedrock_agent_alias_id == 'PRODALIASID'


class TestUIErrorHandling:
    """Test error handling scenarios in the UI."""
    
    @pytest.fixture
    def auditor_with_errors(self):
        """Create auditor instance for error testing."""
        with patch('streamlit.secrets') as mock_secrets:
            mock_secrets.get.return_value = 'test-value'
            
            with patch('boto3.client') as mock_boto3:
                mock_boto3.return_value = Mock()
                return MSAInvoiceAuditor()
    
    def test_s3_upload_permission_error(self, auditor_with_errors):
        """Test handling of S3 permission errors."""
        from botocore.exceptions import ClientError
        
        auditor_with_errors.s3_client.put_object.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
            'PutObject'
        )
        
        with patch('streamlit.error') as mock_error:
            result = auditor_with_errors.upload_file_to_s3(b'content', 'test.pdf')
            
            assert result is None
            mock_error.assert_called_once()
            assert 'Error uploading file to S3' in str(mock_error.call_args)
    
    def test_step_functions_execution_error(self, auditor_with_errors):
        """Test handling of Step Functions execution errors."""
        from botocore.exceptions import ClientError
        
        auditor_with_errors.stepfunctions_client.start_execution.side_effect = ClientError(
            {'Error': {'Code': 'InvalidParameterValue', 'Message': 'Invalid parameter'}},
            'StartExecution'
        )
        
        with patch('streamlit.error') as mock_error:
            result = auditor_with_errors.start_step_function_execution('test-key', 'test-query')
            
            assert result is None
            mock_error.assert_called_once()
            assert 'Error starting Step Functions execution' in str(mock_error.call_args)
    
    def test_bedrock_agent_error(self, auditor_with_errors):
        """Test handling of Bedrock Agent errors."""
        from botocore.exceptions import ClientError
        
        auditor_with_errors.bedrock_agent_client.invoke_agent.side_effect = ClientError(
            {'Error': {'Code': 'AccessDeniedException', 'Message': 'Access denied'}},
            'InvokeAgent'
        )
        
        with patch('streamlit.error') as mock_error:
            result = auditor_with_errors.invoke_bedrock_agent('test query')
            
            assert result == {}
            mock_error.assert_called_once()
            assert 'Error invoking Bedrock Agent' in str(mock_error.call_args)


class TestUIWorkflowIntegration:
    """Test complete workflow integration through the UI."""
    
    @pytest.fixture
    def mock_complete_workflow(self):
        """Mock a complete successful workflow."""
        with patch('streamlit.secrets') as mock_secrets:
            mock_secrets.get.side_effect = lambda key, default=None: {
                'INGESTION_BUCKET': 'test-ingestion-bucket',
                'REPORTS_BUCKET': 'test-reports-bucket',
                'STEP_FUNCTION_ARN': 'arn:aws:states:us-east-1:123456789012:stateMachine:test-workflow',
                'BEDROCK_AGENT_ID': 'test-agent-id',
                'BEDROCK_AGENT_ALIAS_ID': 'TSTALIASID'
            }.get(key, default)
            with patch('boto3.client') as mock_boto3:
                mock_s3 = Mock()
                mock_stepfunctions = Mock()
                mock_bedrock = Mock()
                mock_s3.put_object.return_value = {}
                mock_stepfunctions.start_execution.return_value = {
                    'executionArn': 'arn:aws:states:us-east-1:123456789012:execution:test-workflow:test-execution'
                }
                mock_stepfunctions.describe_execution.return_value = {
                    'status': 'SUCCEEDED',
                    'startDate': datetime(2024, 12, 1, 14, 30, 0),
                    'stopDate': datetime(2024, 12, 1, 14, 35, 0),
                    'input': '{"bucket": "test-bucket", "key": "test-key"}'
                }
                mock_s3.list_objects_v2.return_value = {
                    'Contents': [
                        {
                            'Key': 'reports/test_invoice/audit_report.xlsx',
                            'Size': 15000,
                            'LastModified': datetime(2024, 12, 1, 14, 35, 0)
                        }
                    ]
                }
                mock_boto3.side_effect = lambda service: {
                    's3': mock_s3,
                    'stepfunctions': mock_stepfunctions,
                    'bedrock-agent-runtime': mock_bedrock
                }[service]
                yield {
                    's3': mock_s3,
                    'stepfunctions': mock_stepfunctions,
                    'bedrock': mock_bedrock
                }
    
    def test_complete_workflow_success(self, mock_complete_workflow):
        """Test complete workflow from upload to report generation."""
        auditor = MSAInvoiceAuditor()
        with patch('datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = '20241201_143000'
            s3_key = auditor.upload_file_to_s3(b'test invoice content', 'test_invoice.pdf')
            assert s3_key == 'uploads/20241201_143000_test_invoice.pdf'
        with patch('time.time', return_value=1234567890):
            execution_arn = auditor.start_step_function_execution(s3_key, 'Compare to MSA')
            assert execution_arn == 'arn:aws:states:us-east-1:123456789012:execution:test-workflow:test-execution'
        status = auditor.get_execution_status(execution_arn)
        assert status['status'] == 'SUCCEEDED'
        reports = auditor.list_reports(s3_key)
        assert len(reports) == 1
        assert reports[0]['key'] == 'reports/test_invoice/audit_report.xlsx'


if __name__ == '__main__':
    pytest.main([__file__])
