import json
import logging
import os
import time
import urllib.parse
from datetime import datetime
from typing import Dict, Any, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

s3_client = boto3.client('s3')
stepfunctions_client = boto3.client('stepfunctions')

BUCKET_NAME = os.environ.get('BUCKET_NAME')
STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN')

MAX_FILE_SIZE = 5 * 1024 * 1024

class FileProcessor:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = s3_client

    def get_file_info(self, key: str) -> Dict[str, Any]:
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
            file_extension = os.path.splitext(key)[1].lower()
            file_info['extension'] = file_extension
            file_info['is_supported'] = file_extension == '.pdf'
            return file_info
        except ClientError as e:
            logger.error(f"Error getting file info for {key}: {e}")
            raise

    def _get_object_tags(self, key: str) -> Dict[str, str]:
        try:
            response = self.s3_client.get_object_tagging(Bucket=self.bucket_name, Key=key)
            return {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
        except ClientError as e:
            logger.warning(f"Could not get tags for {key}: {e}")
            return {}

    def validate_file(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        if not file_info['is_supported']:
            validation_result['is_valid'] = False
            validation_result['errors'].append(
                f"Unsupported file type: {file_info['extension']}"
            )
        if file_info['size'] > MAX_FILE_SIZE:
            validation_result['is_valid'] = False
            validation_result['errors'].append(
                f"File size {file_info['size']:,} bytes exceeds maximum {MAX_FILE_SIZE:,} bytes ({MAX_FILE_SIZE/1024/1024:.1f}MB)"
            )
            raise ValueError(f"File size {file_info['size']} exceeds maximum {MAX_FILE_SIZE}")
        if file_info['size'] == 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("File is empty")
        return validation_result

    def extract_metadata(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        metadata = {
            'file_name': os.path.basename(file_info['key']),
            'file_path': file_info['key'],
            'file_size': file_info['size'],
            'file_type': file_info['extension'],
            'content_type': file_info['content_type'],
            'upload_timestamp': datetime.utcnow().isoformat(),
            'etag': file_info['etag']
        }
        metadata['document_type'] = 'pdf'
        metadata['processing_priority'] = 'high'
        metadata.update(file_info.get('metadata', {}))
        return metadata

    def apply_processing_tags(self, key: str, metadata: Dict[str, Any]) -> None:
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
                    return
                else:
                    logger.warning(f"Tagging attempt {attempt + 1} failed for {key}, retrying: {e}")
                    time.sleep(retry_delay * (2 ** attempt))

class WorkflowOrchestrator:
    def __init__(self, state_machine_arn: str):
        self.state_machine_arn = state_machine_arn
        self.stepfunctions_client = stepfunctions_client

    def start_workflow(self, input_data: Dict[str, Any]) -> str:
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
    start_time = time.time()
    logger.info("Ingestion Lambda starting")
    try:
        if 'Records' in event:
            return handle_s3_event(event, context)
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
    finally:
        duration = time.time() - start_time
        records = len(event.get('Records', [])) if isinstance(event, dict) else 0
        logger.info(f"Metrics: duration={duration:.2f}s, records={records}")
        gc.collect()

def handle_s3_event(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    results = []
    batch_files = []
    for record in event['Records']:
        try:
            bucket = record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
            logger.info(f"Processing file: s3://{bucket}/{key}")
            file_extension = os.path.splitext(key)[1].lower()
            if file_extension != '.pdf':
                logger.warning(f"Rejected non-PDF file: {key}")
                raise ValueError("Only PDF files are supported")
            file_processor = FileProcessor(bucket)
            file_info = file_processor.get_file_info(key)
            logger.info(f"File info: {file_info}")
            batch_files.append({
                'file_info': file_info,
                'bucket': bucket,
                'key': key,
                'record': record
            })
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Error processing S3 record: {e}")
            results.append({
                'file': key if 'key' in locals() else 'unknown',
                'status': 'error',
                'error': str(e)
            })
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

if __name__ == '__main__':
    sample_event = {
        'Records': [
            {
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'test.pdf'}
                }
            }
        ]
    }
    lambda_handler(sample_event, None)
