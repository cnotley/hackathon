"""
Invoice Auditing File Ingestion Lambda Handler

This module handles S3 events and orchestrates the file processing workflow
through Step Functions. Only PDF invoices are supported.
"""

import json
import logging
import os
import time
import urllib.parse
from datetime import datetime
from typing import Dict, Any, List, Optional

import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Initialize AWS clients
s3_client = boto3.client('s3')
stepfunctions_client = boto3.client('stepfunctions')

# Configuration
BUCKET_NAME = os.environ.get('BUCKET_NAME')
STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN')

# Supported file types (PDF only)
SUPPORTED_EXTENSIONS = {
    '.pdf': 'application/pdf'
}

# File size limits (in bytes)
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB for better processing performance


class FileProcessor:
    """Handles file processing operations."""
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = s3_client
    
    def get_file_info(self, key: str) -> Dict[str, Any]:
        """Get file information from S3."""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            
            file_info = {
                'key': key,
                'size': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified').isoformat() if response.get('LastModified') else None,
                'content_type': response.get('ContentType', ''),
                'etag': response.get('ETag', '').strip('"'),
                'metadata': response.get('Metadata', {}),
                'tags': self._get_object_tags(key)
            }
            
            # Extract file extension and validate
            file_extension = os.path.splitext(key)[1].lower()
            file_info['extension'] = file_extension
            file_info['is_supported'] = file_extension in SUPPORTED_EXTENSIONS
            
            return file_info
            
        except ClientError as e:
            logger.error(f"Error getting file info for {key}: {e}")
            raise
    
    def _get_object_tags(self, key: str) -> Dict[str, str]:
        """Get object tags from S3."""
        try:
            response = self.s3_client.get_object_tagging(Bucket=self.bucket_name, Key=key)
            return {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
        except ClientError as e:
            logger.warning(f"Could not get tags for {key}: {e}")
            return {}
    
    def validate_file(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Validate file type and size."""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check file extension
        if not file_info['is_supported']:
            validation_result['is_valid'] = False
            validation_result['errors'].append(
                f"Unsupported file type: {file_info['extension']}"
            )
        
        # Check file size with proper validation
        if file_info['size'] > MAX_FILE_SIZE:
            validation_result['is_valid'] = False
            validation_result['errors'].append(
                f"File size {file_info['size']:,} bytes exceeds maximum {MAX_FILE_SIZE:,} bytes ({MAX_FILE_SIZE/1024/1024:.1f}MB)"
            )
            raise ValueError(f"File size {file_info['size']} exceeds maximum {MAX_FILE_SIZE}")
        
        # Check if file is empty
        if file_info['size'] == 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("File is empty")
        
        return validation_result
    
    def extract_metadata(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata based on file type."""
        if file_info.get('extension') != '.pdf':
            raise ValueError("Only PDF files are supported")
        
        metadata = {
            'file_name': os.path.basename(file_info['key']),
            'file_path': file_info['key'],
            'file_size': file_info['size'],
            'file_type': file_info['extension'],
            'content_type': file_info['content_type'],
            'upload_timestamp': datetime.utcnow().isoformat(),
            'etag': file_info['etag']
        }
        
        # Only PDF metadata is allowed
        metadata['document_type'] = 'pdf'
        metadata['processing_priority'] = 'high'
        
        # Add existing S3 metadata
        metadata.update(file_info.get('metadata', {}))
        
        return metadata
    
    def apply_processing_tags(self, key: str, metadata: Dict[str, Any]) -> None:
        """Apply processing tags to S3 object with graceful error handling."""
        tags = {
            'ProcessingStatus': 'processed',
            'DocumentType': metadata.get('document_type', 'unknown'),
            'ProcessingPriority': metadata.get('processing_priority', 'low'),
            'ProcessedTimestamp': datetime.utcnow().isoformat()
        }
        
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
                self.s3_client.put_object_tagging(
                    Bucket=self.bucket_name,
                    Key=key,
                    Tagging={'TagSet': tag_set}
                )
                logger.info(f"Applied processing tags to {key}")
                return
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['NoSuchKey', 'AccessDenied'] or attempt == max_retries - 1:
                    logger.error(f"Failed to apply tags to {key} after {attempt + 1} attempts: {e}")
                    # Don't raise - tagging failure shouldn't stop processing
                    return
                else:
                    logger.warning(f"Tagging attempt {attempt + 1} failed for {key}, retrying: {e}")
                    time.sleep(retry_delay * (2 ** attempt))


class WorkflowOrchestrator:
    """Handles Step Functions workflow orchestration."""
    
    def __init__(self, state_machine_arn: str):
        self.state_machine_arn = state_machine_arn
        self.stepfunctions_client = stepfunctions_client
    
    def start_workflow(self, input_data: Dict[str, Any]) -> str:
        """Start Step Functions workflow execution with retry logic."""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                execution_name = f"ingestion-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{input_data.get('file_info', {}).get('etag', 'unknown')[:8]}"
                
                response = self.stepfunctions_client.start_execution(
                    stateMachineArn=self.state_machine_arn,
                    name=execution_name,
                    input=json.dumps(input_data, default=str)
                )
                
                execution_arn = response['executionArn']
                logger.info(f"Started workflow execution: {execution_arn}")
                return execution_arn
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['ExecutionLimitExceeded', 'InvalidParameterValue'] or attempt == max_retries - 1:
                    logger.error(f"Failed to start workflow after {attempt + 1} attempts: {e}")
                    raise
                else:
                    logger.warning(f"Workflow start attempt {attempt + 1} failed, retrying: {e}")
                    time.sleep(retry_delay * (2 ** attempt))
        
        raise Exception("Failed to start workflow after all retry attempts")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for processing S3 events and Step Functions tasks.
    
    Handles two types of invocations:
    1. S3 event notifications (file uploads)
    2. Step Functions task executions
    """
    logger.info(f"Received event: {json.dumps(event, default=str)}")
    
    try:
        # Check if this is a Step Functions task
        if 'task' in event:
            return handle_step_functions_task(event, context)
        
        # Handle S3 event notification
        if 'Records' in event:
            return handle_s3_event(event, context)
        
        # Unknown event type
        logger.error(f"Unknown event type: {event}")
        raise ValueError("Unknown event type")
        
    except ValueError as error:
        logger.warning(f"Rejected event due to invalid file type: {error}")
        return {
            'statusCode': 400,
            'error': 'Invalid file type'
        }
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        raise


def handle_s3_event(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle S3 event notifications with batch processing support."""
    results = []
    batch_files = []
    
    # Group files for potential batch processing
    for record in event['Records']:
        try:
            # Extract S3 information
            bucket = record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
            
            logger.info(f"Processing file: s3://{bucket}/{key}")

            file_extension = os.path.splitext(key)[1].lower()
            if file_extension != '.pdf':
                logger.warning(f"Rejected non-PDF file: {key}")
                raise ValueError("Only PDF files are supported")
            
            # Initialize file processor
            file_processor = FileProcessor(bucket)
            
            # Route MSA documents to kb-msas/ prefix for Knowledge Base ingestion
            if 'msa' in key.lower() and not key.lower().startswith('kb-msas/'):
                new_key = f"kb-msas/{os.path.basename(key)}"
                try:
                    s3_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=new_key)
                    s3_client.delete_object(Bucket=bucket, Key=key)
                    key = new_key
                    logger.info(f"Routed MSA document to {key}")
                except ClientError as e:
                    logger.warning(f"Failed routing MSA document, proceeding with original key: {e}")

            # Get file information
            file_info = file_processor.get_file_info(key)
            logger.info(f"File info: {file_info}")
            
            batch_files.append({
                'file_info': file_info,
                'bucket': bucket,
                'key': key,
                'record': record
            })
            
        except ValueError:
            # Re-raise to allow lambda_handler to return a 400 response
            raise
        except Exception as e:
            logger.error(f"Error processing S3 record: {e}")
            results.append({
                'file': key if 'key' in locals() else 'unknown',
                'status': 'error',
                'error': str(e)
            })
    
    # Only PDF files are processed; batch handling reduces to single workflow per record
    for file_data in batch_files:
        try:
            workflow_input = {
                'file_info': file_data['file_info'],
                'bucket': file_data['bucket'],
                'key': file_data['key'],
                'event_time': datetime.utcnow().isoformat(),
                'source': 's3_event',
                'batch_mode': False
            }

            orchestrator = WorkflowOrchestrator(STATE_MACHINE_ARN)
            execution_arn = orchestrator.start_workflow(workflow_input)

            results.append({
                'file': file_data['key'],
                'status': 'workflow_started',
                'execution_arn': execution_arn
            })

        except Exception as e:
            logger.error(f"Error processing file {file_data['key']}: {e}")
            results.append({
                'file': file_data['key'],
                'status': 'error',
                'error': str(e)
            })
    
    return {
        'statusCode': 200,
        'body': {
            'message': 'S3 event processed',
            'results': results,
            'batch_processed': len(batch_files) > 1,
            'total_files': len(batch_files)
        }
    }


def handle_batch_processing(batch_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Retained for compatibility; PDF-only ingestion bypasses batch logic."""
    return []


def handle_step_functions_task(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle Step Functions task executions."""
    task = event.get('task')
    input_data = event.get('input', {})
    
    logger.info(f"Processing Step Functions task: {task}")
    
    try:
        if task == 'validate':
            return handle_validate_task(input_data)
        elif task == 'extract':
            return handle_extract_task(input_data)
        elif task == 'process':
            return handle_process_task(input_data)
        else:
            raise ValueError(f"Unknown task: {task}")
            
    except Exception as e:
        logger.error(f"Error processing task {task}: {e}")
        raise


def handle_validate_task(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Handle file validation task."""
    file_info = input_data.get('file_info', {})
    bucket = input_data.get('bucket')
    
    logger.info(f"Validating file: {file_info.get('key')}")
    
    # Initialize file processor
    file_processor = FileProcessor(bucket)
    
    # Validate file
    validation_result = file_processor.validate_file(file_info)
    
    logger.info(f"Validation result: {validation_result}")
    
    if not validation_result['is_valid']:
        raise ValueError(f"File validation failed: {validation_result['errors']}")
    
    return {
        'validation_status': 'passed',
        'validation_details': validation_result,
        'timestamp': datetime.utcnow().isoformat()
    }


def handle_extract_task(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Handle metadata extraction task."""
    file_info = input_data.get('file_info', {})
    bucket = input_data.get('bucket')
    
    logger.info(f"Extracting metadata for file: {file_info.get('key')}")
    
    # Initialize file processor
    file_processor = FileProcessor(bucket)
    
    # Extract metadata
    metadata = file_processor.extract_metadata(file_info)
    
    logger.info(f"Extracted metadata: {metadata}")
    
    return {
        'extraction_status': 'completed',
        'metadata': metadata,
        'timestamp': datetime.utcnow().isoformat()
    }


def handle_process_task(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Handle file processing task."""
    file_info = input_data.get('file_info', {})
    bucket = input_data.get('bucket')
    key = file_info.get('key')
    
    # Get metadata from extraction result
    extraction_result = input_data.get('extraction_result', {})
    metadata = extraction_result.get('Payload', {}).get('metadata', {})
    
    logger.info(f"Processing file: {key}")
    
    # Initialize file processor
    file_processor = FileProcessor(bucket)
    
    # Apply processing tags
    file_processor.apply_processing_tags(key, metadata)
    
    logger.info(f"File processing completed for: {key}")
    
    return {
        'processing_status': 'completed',
        'processed_file': key,
        'processing_metadata': metadata,
        'timestamp': datetime.utcnow().isoformat()
    }
