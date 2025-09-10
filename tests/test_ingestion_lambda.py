"""
Unit tests for Invoice Auditing File Ingestion Lambda

This module contains comprehensive unit tests for the Lambda function,
including S3 event handling, Step Functions integration, and file processing.
"""

import json
import os
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from moto import mock_s3, mock_stepfunctions
import boto3

# Import the Lambda handler and related functions
import sys
sys.path.append('../lambda/ingestion')
sys.path.append('../lambda/layers/common/python')

from handler import (
    lambda_handler,
    handle_s3_event,
    handle_step_functions_task,
    handle_validate_task,
    handle_extract_task,
    handle_process_task,
    FileProcessor,
    WorkflowOrchestrator
)
from common_utils import (
    FileTypeDetector
)


class TestLambdaHandler:
    """Test cases for the main Lambda handler."""
    
    def setup_method(self):
        """Set up test environment."""
        self.test_bucket = "test-audit-files-bucket"
        self.test_state_machine_arn = "arn:aws:states:us-east-1:123456789012:stateMachine:test-workflow"
        
        # Set environment variables
        os.environ['BUCKET_NAME'] = self.test_bucket
        os.environ['STATE_MACHINE_ARN'] = self.test_state_machine_arn
        os.environ['LOG_LEVEL'] = 'DEBUG'
    
    def test_lambda_handler_s3_event(self):
        """Test Lambda handler with S3 event."""
        s3_event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': self.test_bucket},
                        'object': {'key': 'test-invoice.pdf'}
                    }
                }
            ]
        }
        
        with patch('handler.handle_s3_event') as mock_handle_s3:
            mock_handle_s3.return_value = {'statusCode': 200}
            
            result = lambda_handler(s3_event, {})
            
            assert result['statusCode'] == 200
            mock_handle_s3.assert_called_once_with(s3_event, {})
    
    def test_lambda_handler_step_functions_task(self):
        """Test Lambda handler with Step Functions task."""
        step_functions_event = {
            'task': 'validate',
            'input': {
                'file_info': {'key': 'test-invoice.pdf'},
                'bucket': self.test_bucket
            }
        }
        
        with patch('handler.handle_step_functions_task') as mock_handle_sf:
            mock_handle_sf.return_value = {'validation_status': 'passed'}
            
            result = lambda_handler(step_functions_event, {})
            
            assert result['validation_status'] == 'passed'
            mock_handle_sf.assert_called_once_with(step_functions_event, {})
    
    def test_lambda_handler_unknown_event(self):
        """Test Lambda handler with unknown event type."""
        unknown_event = {'unknown': 'event'}
        
        with pytest.raises(ValueError, match="Unknown event type"):
            lambda_handler(unknown_event, {})


class TestS3EventHandling:
    """Test cases for S3 event handling."""
    
    def setup_method(self):
        """Set up test environment."""
        self.test_bucket = "test-audit-files-bucket"
        self.test_state_machine_arn = "arn:aws:states:us-east-1:123456789012:stateMachine:test-workflow"
        
        os.environ['BUCKET_NAME'] = self.test_bucket
        os.environ['STATE_MACHINE_ARN'] = self.test_state_machine_arn
    
    @mock_s3
    @mock_stepfunctions
    def test_handle_s3_event_success(self):
        """Test successful S3 event handling."""
        # Create mock S3 bucket and object
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=self.test_bucket)
        s3_client.put_object(
            Bucket=self.test_bucket,
            Key='test-invoice.pdf',
            Body=b'test content',
            ContentType='application/pdf'
        )
        
        # Create mock Step Functions state machine
        sf_client = boto3.client('stepfunctions', region_name='us-east-1')
        sf_client.create_state_machine(
            name='test-workflow',
            definition=json.dumps({
                "Comment": "Test workflow",
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "End": True}}
            }),
            roleArn='arn:aws:iam::123456789012:role/test-role'
        )
        
        s3_event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': self.test_bucket},
                        'object': {'key': 'test-invoice.pdf'}
                    }
                }
            ]
        }
        
        result = handle_s3_event(s3_event, {})
        
        assert result['statusCode'] == 200
        assert len(result['body']['results']) == 1
        assert result['body']['results'][0]['status'] == 'workflow_started'
        assert 'execution_arn' in result['body']['results'][0]
    
    def test_handle_s3_event_error(self):
        """Test S3 event handling with error."""
        s3_event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'nonexistent-bucket'},
                        'object': {'key': 'test-invoice.pdf'}
                    }
                }
            ]
        }
        
        result = handle_s3_event(s3_event, {})
        
        assert result['statusCode'] == 200
        assert len(result['body']['results']) == 1
        assert result['body']['results'][0]['status'] == 'error'
        assert 'error' in result['body']['results'][0]


class TestStepFunctionsTasks:
    """Test cases for Step Functions task handling."""
    
    def setup_method(self):
        """Set up test environment."""
        self.test_bucket = "test-audit-files-bucket"
        os.environ['BUCKET_NAME'] = self.test_bucket
    
    @mock_s3
    def test_handle_validate_task_success(self):
        """Test successful file validation task."""
        # Create mock S3 bucket and object
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=self.test_bucket)
        s3_client.put_object(
            Bucket=self.test_bucket,
            Key='test-invoice.pdf',
            Body=b'test content',
            ContentType='application/pdf'
        )
        
        input_data = {
            'file_info': {
                'key': 'test-invoice.pdf',
                'size': 1024,
                'extension': '.pdf',
                'is_supported': True
            },
            'bucket': self.test_bucket
        }
        
        result = handle_validate_task(input_data)
        
        assert result['validation_status'] == 'passed'
        assert 'validation_details' in result
        assert 'timestamp' in result
    
    def test_handle_validate_task_failure(self):
        """Test file validation task failure."""
        input_data = {
            'file_info': {
                'key': 'test-file.txt',
                'size': 0,
                'extension': '.txt',
                'is_supported': False
            },
            'bucket': self.test_bucket
        }
        
        with pytest.raises(ValueError, match="File validation failed"):
            handle_validate_task(input_data)
    
    @mock_s3
    def test_handle_extract_task_success(self):
        """Test successful metadata extraction task."""
        input_data = {
            'file_info': {
                'key': 'test-invoice.pdf',
                'size': 1024,
                'extension': '.pdf',
                'content_type': 'application/pdf',
                'etag': 'test-etag',
                'metadata': {}
            },
            'bucket': self.test_bucket
        }
        
        result = handle_extract_task(input_data)
        
        assert result['extraction_status'] == 'completed'
        assert 'metadata' in result
        assert result['metadata']['document_type'] == 'pdf'
        assert result['metadata']['processing_priority'] == 'high'
    
    @mock_s3
    def test_handle_process_task_success(self):
        """Test successful file processing task."""
        # Create mock S3 bucket and object
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=self.test_bucket)
        s3_client.put_object(
            Bucket=self.test_bucket,
            Key='test-invoice.pdf',
            Body=b'test content'
        )
        
        input_data = {
            'file_info': {'key': 'test-invoice.pdf'},
            'bucket': self.test_bucket,
            'extraction_result': {
                'Payload': {
                    'metadata': {
                        'document_type': 'pdf',
                        'processing_priority': 'high'
                    }
                }
            }
        }
        
        result = handle_process_task(input_data)
        
        assert result['processing_status'] == 'completed'
        assert result['processed_file'] == 'test-invoice.pdf'


class TestFileProcessor:
    """Test cases for FileProcessor class."""
    
    def setup_method(self):
        """Set up test environment."""
        self.test_bucket = "test-bucket"
        self.processor = FileProcessor(self.test_bucket)
    
    @mock_s3
    def test_get_file_info_success(self):
        """Test successful file info retrieval."""
        # Create mock S3 bucket and object
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=self.test_bucket)
        s3_client.put_object(
            Bucket=self.test_bucket,
            Key='test-file.pdf',
            Body=b'test content',
            ContentType='application/pdf'
        )
        
        file_info = self.processor.get_file_info('test-file.pdf')
        
        assert file_info['key'] == 'test-file.pdf'
        assert file_info['size'] > 0
        assert file_info['extension'] == '.pdf'
        assert file_info['is_supported'] is True
        assert file_info['content_type'] == 'application/pdf'
    
    def test_validate_file_supported_type(self):
        """Test file validation for supported file type."""
        file_info = {
            'extension': '.pdf',
            'is_supported': True,
            'size': 1024
        }
        
        result = self.processor.validate_file(file_info)
        
        assert result['is_valid'] is True
        assert len(result['errors']) == 0
    
    def test_validate_file_unsupported_type(self):
        """Test file validation for unsupported file type."""
        file_info = {
            'extension': '.txt',
            'is_supported': False,
            'size': 1024
        }
        
        result = self.processor.validate_file(file_info)
        
        assert result['is_valid'] is False
        assert len(result['errors']) > 0
        assert 'Unsupported file type' in result['errors'][0]
    
    def test_validate_file_too_large(self):
        """Test file validation for oversized file."""
        file_info = {
            'extension': '.pdf',
            'is_supported': True,
            'size': 200 * 1024 * 1024  # 200MB
        }
        
        result = self.processor.validate_file(file_info)
        
        assert result['is_valid'] is False
        assert any('exceeds maximum' in error for error in result['errors'])
    
    def test_validate_file_empty(self):
        """Test file validation for empty file."""
        file_info = {
            'extension': '.pdf',
            'is_supported': True,
            'size': 0
        }
        
        result = self.processor.validate_file(file_info)
        
        assert result['is_valid'] is False
        assert 'File is empty' in result['errors']
    
    def test_extract_metadata_pdf(self):
        """Test metadata extraction for PDF file."""
        file_info = {
            'key': 'invoices/test-invoice.pdf',
            'size': 1024,
            'extension': '.pdf',
            'content_type': 'application/pdf',
            'etag': 'test-etag',
            'metadata': {'custom': 'value'}
        }
        
        metadata = self.processor.extract_metadata(file_info)
        
        assert metadata['file_name'] == 'test-invoice.pdf'
        assert metadata['file_path'] == 'invoices/test-invoice.pdf'
        assert metadata['document_type'] == 'pdf'
        assert metadata['processing_priority'] == 'high'
        assert metadata['custom'] == 'value'
    
    def test_extract_metadata_raises_for_non_pdf(self):
        """Non-PDF metadata extraction should be rejected."""
        for extension, key in [('.xlsx', 'spreadsheets/data.xlsx'), ('.png', 'images/receipt.png')]:
            file_info = {
                'key': key,
                'size': 1024,
                'extension': extension,
                'content_type': 'application/octet-stream',
                'etag': 'test-etag',
                'metadata': {}
            }
            with pytest.raises(ValueError):
                self.processor.extract_metadata(file_info)


class TestWorkflowOrchestrator:
    """Test cases for WorkflowOrchestrator class."""
    
    def setup_method(self):
        """Set up test environment."""
        self.test_state_machine_arn = "arn:aws:states:us-east-1:123456789012:stateMachine:test"
        self.orchestrator = WorkflowOrchestrator(self.test_state_machine_arn)
    
    @mock_stepfunctions
    def test_start_workflow_success(self):
        """Test successful workflow start."""
        # Create mock Step Functions state machine
        sf_client = boto3.client('stepfunctions', region_name='us-east-1')
        sf_client.create_state_machine(
            name='test',
            definition=json.dumps({
                "Comment": "Test workflow",
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "End": True}}
            }),
            roleArn='arn:aws:iam::123456789012:role/test-role'
        )
        
        input_data = {
            'file_info': {'key': 'test-file.pdf'},
            'bucket': 'test-bucket'
        }
        
        execution_arn = self.orchestrator.start_workflow(input_data)
        
        assert execution_arn.startswith('arn:aws:states:')
        assert 'execution' in execution_arn


class TestCommonUtils:
    """Test cases for common utilities."""
    
    def test_file_metadata_dataclass(self):
        """Test FileMetadata dataclass."""
        metadata = FileMetadata(
            file_name='test.pdf',
            file_path='path/test.pdf',
            file_size=1024,
            file_type='.pdf',
            content_type='application/pdf',
            upload_timestamp='2023-01-01T00:00:00Z',
            etag='test-etag',
            document_type='pdf',
            processing_priority='high'
        )
        
        metadata_dict = metadata.to_dict()
        
        assert metadata_dict['file_name'] == 'test.pdf'
        assert metadata_dict['document_type'] == 'pdf'
    
    def test_validation_result_dataclass(self):
        """Test ValidationResult dataclass."""
        result = ValidationResult(
            is_valid=False,
            errors=['Error 1', 'Error 2'],
            warnings=['Warning 1']
        )
        
        result_dict = result.to_dict()
        
        assert result_dict['is_valid'] is False
        assert len(result_dict['errors']) == 2
        assert len(result_dict['warnings']) == 1
    
    def test_audit_logger(self):
        """Test AuditLogger functionality."""
        logger = AuditLogger('test_logger')
        
        # Test that logger is created without errors
        assert logger.logger is not None
        
        # Test logging methods (these should not raise exceptions)
        logger.log_file_event('TEST_EVENT', 'test-file.pdf', {'size': 1024})
        logger.log_workflow_event('test-workflow', 'validate', 'success')
        logger.log_error(Exception('Test error'), {'context': 'test'})
    
    def test_file_type_detector(self):
        """Test FileTypeDetector utility."""
        # Test supported file type
        file_info = FileTypeDetector.get_file_type_info('test.pdf', 'application/pdf')
        
        assert file_info['extension'] == '.pdf'
        assert file_info['is_supported'] is True
        assert file_info['content_type_match'] is True
        assert file_info['file_category'] == 'document'
        
        # Test unsupported file type
        file_info = FileTypeDetector.get_file_type_info('test.txt', 'text/plain')
        
        assert file_info['extension'] == '.txt'
        assert file_info['is_supported'] is False
        assert file_info['file_category'] == 'unknown'
    
    def test_file_type_validation(self):
        """Test file type validation."""
        # Valid file type
        result = FileTypeDetector.validate_file_type('test.pdf', 'application/pdf')
        
        assert result.is_valid is True
        assert len(result.errors) == 0
        
        # Invalid file type
        result = FileTypeDetector.validate_file_type('test.txt', 'text/plain')
        
        assert result.is_valid is False
        assert len(result.errors) > 0


class TestIntegrationScenarios:
    """Integration test scenarios."""
    
    def setup_method(self):
        """Set up test environment."""
        self.test_bucket = "test-audit-files-bucket"
        self.test_state_machine_arn = "arn:aws:states:us-east-1:123456789012:stateMachine:test-workflow"
        
        os.environ['BUCKET_NAME'] = self.test_bucket
        os.environ['STATE_MACHINE_ARN'] = self.test_state_machine_arn
    
    @mock_s3
    @mock_stepfunctions
    def test_end_to_end_pdf_processing(self):
        """Test end-to-end PDF file processing."""
        # Setup AWS resources
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=self.test_bucket)
        s3_client.put_object(
            Bucket=self.test_bucket,
            Key='invoices/invoice-001.pdf',
            Body=b'PDF content here',
            ContentType='application/pdf'
        )
        
        sf_client = boto3.client('stepfunctions', region_name='us-east-1')
        sf_client.create_state_machine(
            name='test-workflow',
            definition=json.dumps({
                "Comment": "Test workflow",
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "End": True}}
            }),
            roleArn='arn:aws:iam::123456789012:role/test-role'
        )
        
        # Test S3 event processing
        s3_event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': self.test_bucket},
                        'object': {'key': 'invoices/invoice-001.pdf'}
                    }
                }
            ]
        }
        
        # Process S3 event
        s3_result = handle_s3_event(s3_event, {})
        assert s3_result['statusCode'] == 200
        
        # Test validation task
        validation_input = {
            'file_info': {
                'key': 'invoices/invoice-001.pdf',
                'size': 16,
                'extension': '.pdf',
                'is_supported': True
            },
            'bucket': self.test_bucket
        }
        
        validation_result = handle_validate_task(validation_input)
        assert validation_result['validation_status'] == 'passed'
        
        # Test extraction task
        extraction_result = handle_extract_task(validation_input)
        assert extraction_result['extraction_status'] == 'completed'
        assert extraction_result['metadata']['document_type'] == 'pdf'
        
        # Test processing task
        processing_input = {
            'file_info': {'key': 'invoices/invoice-001.pdf'},
            'bucket': self.test_bucket,
            'extraction_result': {
                'Payload': extraction_result
            }
        }
        
        processing_result = handle_process_task(processing_input)
        assert processing_result['processing_status'] == 'completed'
    
    def test_error_handling_scenarios(self):
        """Test various error handling scenarios."""
        # Test with missing file
        with pytest.raises(Exception):
            processor = FileProcessor('nonexistent-bucket')
            processor.get_file_info('nonexistent-file.pdf')
        
        # Test with invalid Step Functions ARN
        with pytest.raises(Exception):
            orchestrator = WorkflowOrchestrator('invalid-arn')
            orchestrator.start_workflow({'test': 'data'})


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
