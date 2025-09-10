"""
Common utilities for Invoice Auditing File Ingestion Module

This module provides shared utilities and helper classes for file processing,
logging, S3 operations, and Step Functions integration.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, asdict

import boto3
from botocore.exceptions import ClientError


@dataclass
class FileMetadata:
    """Data class for file metadata."""
    file_name: str
    file_path: str
    file_size: int
    file_type: str
    content_type: str
    upload_timestamp: str
    etag: str
    document_type: Optional[str] = None
    processing_priority: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class ValidationResult:
    """Data class for validation results."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class AuditLogger:
    """Enhanced logging for audit trail."""
    
    def __init__(self, logger_name: str = __name__):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))
        
        # Add structured logging format if not already configured
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def log_file_event(self, event_type: str, file_key: str, 
                      metadata: Optional[Dict[str, Any]] = None,
                      level: str = 'INFO') -> None:
        """Log file processing events with structured data."""
        log_data = {
            'event_type': event_type,
            'file_key': file_key,
            'timestamp': datetime.utcnow().isoformat(),
            'metadata': metadata or {}
        }
        
        message = f"{event_type}: {file_key}"
        if metadata:
            message += f" | {json.dumps(metadata, default=str)}"
        
        getattr(self.logger, level.lower())(message)
    
    def log_workflow_event(self, workflow_id: str, step: str, 
                          status: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Log workflow execution events."""
        log_data = {
            'workflow_id': workflow_id,
            'step': step,
            'status': status,
            'timestamp': datetime.utcnow().isoformat(),
            'details': details or {}
        }
        
        message = f"Workflow {workflow_id} - {step}: {status}"
        if details:
            message += f" | {json.dumps(details, default=str)}"
        
        self.logger.info(message)
    
    def log_error(self, error: Exception, context: Optional[Dict[str, Any]] = None) -> None:
        """Log errors with context."""
        error_data = {
            'error_type': type(error).__name__,
            'error_message': str(error),
            'timestamp': datetime.utcnow().isoformat(),
            'context': context or {}
        }
        
        message = f"Error: {type(error).__name__} - {str(error)}"
        if context:
            message += f" | Context: {json.dumps(context, default=str)}"
        
        self.logger.error(message)


class S3Helper:
    """Helper class for S3 operations."""
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        self.logger = AuditLogger(f"{__name__}.S3Helper")
    
    def get_object_info(self, key: str) -> Dict[str, Any]:
        """Get comprehensive object information."""
        try:
            # Get object metadata
            head_response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            
            # Get object tags
            tags = self._get_object_tags(key)
            
            # Get object ACL (if accessible)
            acl = self._get_object_acl(key)
            
            object_info = {
                'key': key,
                'bucket': self.bucket_name,
                'size': head_response.get('ContentLength', 0),
                'last_modified': head_response.get('LastModified'),
                'content_type': head_response.get('ContentType', ''),
                'etag': head_response.get('ETag', '').strip('"'),
                'metadata': head_response.get('Metadata', {}),
                'tags': tags,
                'acl': acl,
                'storage_class': head_response.get('StorageClass', 'STANDARD'),
                'server_side_encryption': head_response.get('ServerSideEncryption'),
                'version_id': head_response.get('VersionId')
            }
            
            self.logger.log_file_event('S3_OBJECT_INFO_RETRIEVED', key, 
                                     {'size': object_info['size']})
            
            return object_info
            
        except ClientError as e:
            self.logger.log_error(e, {'operation': 'get_object_info', 'key': key})
            raise
    
    def _get_object_tags(self, key: str) -> Dict[str, str]:
        """Get object tags."""
        try:
            response = self.s3_client.get_object_tagging(Bucket=self.bucket_name, Key=key)
            return {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
        except ClientError as e:
            self.logger.logger.warning(f"Could not get tags for {key}: {e}")
            return {}
    
    def _get_object_acl(self, key: str) -> Optional[Dict[str, Any]]:
        """Get object ACL."""
        try:
            response = self.s3_client.get_object_acl(Bucket=self.bucket_name, Key=key)
            return {
                'owner': response.get('Owner'),
                'grants': response.get('Grants', [])
            }
        except ClientError as e:
            self.logger.logger.warning(f"Could not get ACL for {key}: {e}")
            return None
    
    def put_object_tags(self, key: str, tags: Dict[str, str]) -> None:
        """Put object tags."""
        try:
            tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
            self.s3_client.put_object_tagging(
                Bucket=self.bucket_name,
                Key=key,
                Tagging={'TagSet': tag_set}
            )
            
            self.logger.log_file_event('S3_TAGS_APPLIED', key, {'tags': tags})
            
        except ClientError as e:
            self.logger.log_error(e, {'operation': 'put_object_tags', 'key': key})
            raise
    
    def copy_object(self, source_key: str, dest_key: str, 
                   metadata: Optional[Dict[str, str]] = None) -> None:
        """Copy object within the same bucket."""
        try:
            copy_source = {'Bucket': self.bucket_name, 'Key': source_key}
            
            copy_args = {
                'CopySource': copy_source,
                'Bucket': self.bucket_name,
                'Key': dest_key
            }
            
            if metadata:
                copy_args['Metadata'] = metadata
                copy_args['MetadataDirective'] = 'REPLACE'
            
            self.s3_client.copy_object(**copy_args)
            
            self.logger.log_file_event('S3_OBJECT_COPIED', source_key, 
                                     {'destination': dest_key})
            
        except ClientError as e:
            self.logger.log_error(e, {'operation': 'copy_object', 
                                    'source': source_key, 'dest': dest_key})
            raise
    
    def generate_presigned_url(self, key: str, expiration: int = 3600) -> str:
        """Generate presigned URL for object access."""
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': key},
                ExpiresIn=expiration
            )
            
            self.logger.log_file_event('S3_PRESIGNED_URL_GENERATED', key, 
                                     {'expiration': expiration})
            
            return url
            
        except ClientError as e:
            self.logger.log_error(e, {'operation': 'generate_presigned_url', 'key': key})
            raise


class StepFunctionsHelper:
    """Helper class for Step Functions operations."""
    
    def __init__(self):
        self.stepfunctions_client = boto3.client('stepfunctions')
        self.logger = AuditLogger(f"{__name__}.StepFunctionsHelper")
    
    def start_execution(self, state_machine_arn: str, input_data: Dict[str, Any], 
                       name: Optional[str] = None) -> str:
        """Start Step Functions execution."""
        try:
            if not name:
                name = f"execution-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
            
            response = self.stepfunctions_client.start_execution(
                stateMachineArn=state_machine_arn,
                name=name,
                input=json.dumps(input_data, default=str)
            )
            
            execution_arn = response['executionArn']
            
            self.logger.log_workflow_event(execution_arn, 'START', 'SUCCESS', 
                                         {'state_machine': state_machine_arn})
            
            return execution_arn
            
        except ClientError as e:
            self.logger.log_error(e, {'operation': 'start_execution', 
                                    'state_machine': state_machine_arn})
            raise
    
    def describe_execution(self, execution_arn: str) -> Dict[str, Any]:
        """Describe Step Functions execution."""
        try:
            response = self.stepfunctions_client.describe_execution(
                executionArn=execution_arn
            )
            
            execution_info = {
                'execution_arn': execution_arn,
                'status': response.get('status'),
                'start_date': response.get('startDate'),
                'stop_date': response.get('stopDate'),
                'input': response.get('input'),
                'output': response.get('output'),
                'error': response.get('error'),
                'cause': response.get('cause')
            }
            
            return execution_info
            
        except ClientError as e:
            self.logger.log_error(e, {'operation': 'describe_execution', 
                                    'execution_arn': execution_arn})
            raise
    
    def send_task_success(self, task_token: str, output: Dict[str, Any]) -> None:
        """Send task success to Step Functions."""
        try:
            self.stepfunctions_client.send_task_success(
                taskToken=task_token,
                output=json.dumps(output, default=str)
            )
            
            self.logger.log_workflow_event(task_token, 'TASK_SUCCESS', 'SENT')
            
        except ClientError as e:
            self.logger.log_error(e, {'operation': 'send_task_success', 
                                    'task_token': task_token})
            raise
    
    def send_task_failure(self, task_token: str, error: str, cause: str) -> None:
        """Send task failure to Step Functions."""
        try:
            self.stepfunctions_client.send_task_failure(
                taskToken=task_token,
                error=error,
                cause=cause
            )
            
            self.logger.log_workflow_event(task_token, 'TASK_FAILURE', 'SENT', 
                                         {'error': error, 'cause': cause})
            
        except ClientError as e:
            self.logger.log_error(e, {'operation': 'send_task_failure', 
                                    'task_token': task_token})
            raise


class FileTypeDetector:
    """Utility for detecting and validating file types."""
    
    MIME_TYPE_MAPPING = {
        'application/pdf': ['.pdf']
    }
    
    EXTENSION_MAPPING = {
        '.pdf': 'application/pdf'
    }
    
    @classmethod
    def get_file_type_info(cls, filename: str, content_type: str = '') -> Dict[str, Any]:
        """Get comprehensive file type information."""
        file_extension = os.path.splitext(filename)[1].lower()
        
        # Determine expected content type from extension
        expected_content_type = cls.EXTENSION_MAPPING.get(file_extension)
        
        # Check if content type matches extension
        content_type_match = (
            content_type == expected_content_type or
            content_type in cls.MIME_TYPE_MAPPING and 
            file_extension in cls.MIME_TYPE_MAPPING[content_type]
        )
        
        return {
            'filename': filename,
            'extension': file_extension,
            'content_type': content_type,
            'expected_content_type': expected_content_type,
            'is_supported': file_extension in cls.EXTENSION_MAPPING,
            'content_type_match': content_type_match,
            'file_category': cls._get_file_category(file_extension)
        }
    
    @classmethod
    def _get_file_category(cls, extension: str) -> str:
        """Get file category based on extension."""
        if extension == '.pdf':
            return 'document'
        return 'unknown'
    
    @classmethod
    def validate_file_type(cls, filename: str, content_type: str = '') -> ValidationResult:
        """Validate file type and return detailed results."""
        file_info = cls.get_file_type_info(filename, content_type)
        
        errors = []
        warnings = []
        
        # Check if file type is supported
        if not file_info['is_supported']:
            errors.append(f"Unsupported file type: {file_info['extension']}")
        
        # Check content type mismatch
        if content_type and not file_info['content_type_match']:
            warnings.append(
                f"Content type mismatch: got {content_type}, "
                f"expected {file_info['expected_content_type']}"
            )
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )


class ConfigManager:
    """Configuration management utility."""
    
    def __init__(self):
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        return {
            'bucket_name': os.environ.get('BUCKET_NAME'),
            'state_machine_arn': os.environ.get('STATE_MACHINE_ARN'),
            'log_level': os.environ.get('LOG_LEVEL', 'INFO'),
            'max_file_size': int(os.environ.get('MAX_FILE_SIZE', 100 * 1024 * 1024)),
            'supported_extensions': ['.pdf'],
            'processing_timeout': int(os.environ.get('PROCESSING_TIMEOUT', 300)),
            'retry_attempts': int(os.environ.get('RETRY_ATTEMPTS', 3))
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self.config.get(key, default)
    
    def validate_config(self) -> ValidationResult:
        """Validate required configuration."""
        errors = []
        warnings = []
        
        required_keys = ['bucket_name', 'state_machine_arn']
        for key in required_keys:
            if not self.config.get(key):
                errors.append(f"Missing required configuration: {key}")
        
        # Validate file size limit
        max_size = self.config.get('max_file_size', 0)
        if max_size <= 0:
            warnings.append("Invalid max file size configuration")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )


# Utility functions
def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format."""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"


def generate_correlation_id() -> str:
    """Generate correlation ID for tracking."""
    return f"corr-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{os.urandom(4).hex()}"


def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe processing."""
    # Remove or replace unsafe characters
    unsafe_chars = ['<', '>', ':', '"', '|', '?', '*', '\\', '/']
    sanitized = filename
    
    for char in unsafe_chars:
        sanitized = sanitized.replace(char, '_')
    
    # Limit length
    if len(sanitized) > 255:
        name, ext = os.path.splitext(sanitized)
        sanitized = name[:255-len(ext)] + ext
    
    return sanitized
