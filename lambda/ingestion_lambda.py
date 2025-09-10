import gc
import json
import logging
import os
import time
import urllib.parse
from datetime import datetime
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

s3_client = boto3.client('s3')
stepfunctions_client = boto3.client('stepfunctions')
lambda_client = boto3.client('lambda')

STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN')
MAX_FILE_SIZE = 5 * 1024 * 1024
USE_SFN = os.environ.get('USE_SFN', 'true').lower() == 'true'
EXTRACTION_LAMBDA_NAME = os.environ.get('EXTRACTION_LAMBDA_NAME', 'extraction-lambda')
RECONCILIATION_LAMBDA_NAME = os.environ.get('RECONCILIATION_LAMBDA_NAME', 'reconciliation-lambda')
REPORT_LAMBDA_NAME = os.environ.get('REPORT_LAMBDA_NAME', 'report-lambda')
DEFAULT_VENDOR_NAME = os.environ.get('DEFAULT_VENDOR_NAME', 'UNKNOWN')

class FileProcessor:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = s3_client

    def get_file_info(self, key: str) -> Dict[str, Any]:
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            object_metadata = response.get('Metadata', {}) or {}
            tags = self._get_object_tags(key)
            file_info = {
                'key': key,
                'size': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified').isoformat() if response.get('LastModified') else None,
                'content_type': response.get('ContentType', ''),
                'etag': response.get('ETag', '').strip('"'),
                'metadata': object_metadata,
                'tags': tags
            }
            vendor_value = (
                object_metadata.get('vendor')
                or object_metadata.get('Vendor')
                or tags.get('vendor')
                or tags.get('Vendor')
            )
            if vendor_value:
                file_info['vendor'] = str(vendor_value).strip().upper()
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
        vendor = file_info.get('vendor')
        if vendor:
            metadata['vendor'] = vendor
        return metadata

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
        records = event.get('Records') if isinstance(event, dict) else None
        record = records[0] if records else {}
        file_key = record.get('s3', {}).get('object', {}).get('key', 'unknown')
        return {
            'statusCode': 400,
            'error': 'Invalid file type',
            'body': {
                'message': 'Invalid file type',
                'results': [
                    {
                        'file': file_key,
                        'status': 'error',
                        'error': str(error) or 'Invalid file type',
                    }
                ],
                'total_files': 1,
            },
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
    results: List[Dict[str, Any]] = []
    orchestrator = WorkflowOrchestrator(STATE_MACHINE_ARN) if USE_SFN and STATE_MACHINE_ARN else None

    for record in event['Records']:
        key = None
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
            validation = file_processor.validate_file(file_info)
            if not validation['is_valid']:
                results.append(
                    {
                        'file': key,
                        'status': 'error',
                        'error': '; '.join(validation['errors']) or 'Validation failed'
                    }
                )
                continue

            workflow_input = {
                'file_info': file_info,
                'bucket': bucket,
                'key': key,
                'event_time': datetime.utcnow().isoformat(),
                'source': 's3_event',
                'batch_mode': False,
                'vendor': file_info.get('vendor') or DEFAULT_VENDOR_NAME,
            }

            if orchestrator:
                execution_arn = orchestrator.start_workflow(workflow_input)
                results.append(
                    {
                        'file': key,
                        'status': 'workflow_started',
                        'execution_arn': execution_arn
                    }
                )
            else:
                fallback_result = _fallback_direct_processing(bucket, key, workflow_input)
                results.append(
                    {
                        'file': key,
                        'status': 'fallback_completed',
                        'details': fallback_result
                    }
                )
        except ValueError:
            raise
        except Exception as exc:
            logger.error(f"Error processing S3 record: {exc}")
            results.append(
                {
                    'file': key or 'unknown',
                    'status': 'error',
                    'error': str(exc)
                }
            )

    return {
        'statusCode': 200,
        'body': {
            'message': 'S3 event processed',
            'results': results,
            'total_files': len(results)
        }
    }

def _fallback_direct_processing(bucket: str, key: str, workflow_input: Dict[str, Any]) -> Dict[str, Any]:
    vendor = workflow_input.get('vendor') or DEFAULT_VENDOR_NAME
    file_info = workflow_input.get('file_info', {})

    extraction_payload = _invoke_lambda(
        EXTRACTION_LAMBDA_NAME,
        {
            'bucket': bucket,
            'key': key,
            'file_info': file_info,
            'vendor': vendor,
        }
    )

    reconciliation_payload = _invoke_lambda(
        RECONCILIATION_LAMBDA_NAME,
        {
            'extraction': {'Payload': extraction_payload},
            'vendor': vendor,
        }
    )

    report_payload = _invoke_lambda(
        REPORT_LAMBDA_NAME,
        {
            'extracted_data': extraction_payload,
            'reconciliation': reconciliation_payload,
            'vendor': vendor,
            'file_info': file_info,
        }
    )

    return {
        'extraction': extraction_payload,
        'reconciliation': reconciliation_payload,
        'report': report_payload,
    }


def _invoke_lambda(function_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    logger.info(f"Invoking {function_name} with payload keys: {list(payload.keys())}")
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload, default=str).encode('utf-8'),
        )
        raw_payload = response.get('Payload')
        if raw_payload is None:
            return {}
        body = raw_payload.read().decode('utf-8')
        return json.loads(body) if body else {}
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error(f"Invocation of {function_name} failed: {exc}")
        return {'status': 'error', 'error': str(exc)}

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
