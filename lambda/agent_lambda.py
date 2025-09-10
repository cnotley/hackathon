"""
AI Agent Lambda Function for Invoice Auditing

This module implements an AI agent using Amazon Bedrock that can audit invoices
against MSA (Master Services Agreement) standards, leveraging the existing extraction
pipeline and knowledge base integration.
"""

import json
import logging
import os
import boto3
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid
import re
from functools import lru_cache
import time
import gc

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.getenv('LOG_LEVEL', 'INFO'))

# Initialize AWS clients
bedrock_agent_client = boto3.client('bedrock-agent-runtime')
bedrock_client = boto3.client('bedrock-runtime')
lambda_client = boto3.client('lambda')
stepfunctions_client = boto3.client('stepfunctions')
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# Environment variables
AGENT_ID = os.getenv('BEDROCK_AGENT_ID')
AGENT_ALIAS_ID = os.getenv('BEDROCK_AGENT_ALIAS_ID', 'TSTALIASID')
KNOWLEDGE_BASE_ID = os.getenv('KNOWLEDGE_BASE_ID')
MSA_RATES_TABLE = os.getenv('MSA_RATES_TABLE', 'msa-rates')
EXTRACTION_LAMBDA_NAME = os.getenv('EXTRACTION_LAMBDA_NAME', 'extraction-lambda')
BUCKET_NAME = os.getenv('BUCKET_NAME')
GUARDRAIL_ID = os.getenv('GUARDRAIL_ID', 'default-guardrail')


class InputValidator:
    """Validates input data and configuration."""
    
    @staticmethod
    def validate_labor_type_format(labor_type: str) -> bool:
        """Validate labor type format using regex check."""
        if not labor_type:
            return False
        
        # Valid labor type codes: RS, US, SS, SU, EN
        pattern = r'^(RS|US|SS|SU|EN)$'
        return bool(re.match(pattern, labor_type.upper()))
    
    @staticmethod
    def validate_agent_configuration() -> Dict[str, Any]:
        """Validate agent configuration and return status."""
        config_status = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check required environment variables
        required_vars = [
            ('BEDROCK_AGENT_ID', AGENT_ID),
            ('MSA_RATES_TABLE', MSA_RATES_TABLE),
            ('BUCKET_NAME', BUCKET_NAME)
        ]
        
        for var_name, var_value in required_vars:
            if not var_value:
                config_status['valid'] = False
                config_status['errors'].append(f"Missing required environment variable: {var_name}")
        
        # Test DynamoDB connection
        try:
            table = dynamodb.Table(MSA_RATES_TABLE)
            table.load()
        except Exception as e:
            config_status['warnings'].append(f"DynamoDB connection issue: {str(e)}")
        
        # Test Bedrock agent availability
        if AGENT_ID:
            try:
                bedrock_agent_client.get_agent(agentId=AGENT_ID)
            except Exception as e:
                config_status['warnings'].append(f"Bedrock agent not accessible: {str(e)}")
        
        return config_status


class MSARatesManager:
    """Manages MSA (Master Services Agreement) rates from DynamoDB."""
    
    def __init__(self):
        self.table = dynamodb.Table(MSA_RATES_TABLE)
        self._rate_cache = {}  # Local cache for fallback
    
    @lru_cache(maxsize=128)
    def get_rate_for_labor_type(self, labor_type: str, location: str = 'default') -> Optional[float]:
        """Get the standard MSA rate for a specific labor type."""
        try:
            response = self.table.get_item(
                Key={
                    'labor_type': labor_type,
                    'location': location
                }
            )
            
            if 'Item' in response:
                return float(response['Item']['standard_rate'])
            
            # Fallback to default location if specific location not found
            if location != 'default':
                return self.get_rate_for_labor_type(labor_type, 'default')
            
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving MSA rate for {labor_type}: {str(e)}")
            return None
    
    def get_overtime_threshold(self, labor_type: str = 'default') -> float:
        """Get overtime threshold hours per week."""
        try:
            response = self.table.get_item(
                Key={
                    'labor_type': labor_type,
                    'location': 'overtime_rules'
                }
            )
            
            if 'Item' in response:
                return float(response['Item'].get('weekly_threshold', 40.0))
            
            return 40.0  # Default overtime threshold
            
        except Exception as e:
            logger.error(f"Error retrieving overtime threshold: {str(e)}")
            return 40.0


class InvoiceAuditor:
    """Handles invoice auditing logic and discrepancy detection."""
    
    def __init__(self):
        self.msa_manager = MSARatesManager()
    
    def audit_extracted_data(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Audit extracted invoice data against MSA standards."""
        audit_results = {
            'audit_id': str(uuid.uuid4()),
            'timestamp': datetime.utcnow().isoformat(),
            'discrepancies': [],
            'summary': {
                'total_discrepancies': 0,
                'rate_variances': 0,
                'overtime_violations': 0,
                'total_labor_cost': 0.0,
                'expected_labor_cost': 0.0
            }
        }
        
        # Audit labor costs
        if 'normalized_data' in extracted_data and 'labor' in extracted_data['normalized_data']:
            labor_data = extracted_data['normalized_data']['labor']
            audit_results = self._audit_labor_costs(labor_data, audit_results)
        
        # Audit materials if present
        if 'normalized_data' in extracted_data and 'materials' in extracted_data['normalized_data']:
            materials_data = extracted_data['normalized_data']['materials']
            audit_results = self._audit_materials(materials_data, audit_results)
        
        # Calculate summary
        audit_results['summary']['total_discrepancies'] = len(audit_results['discrepancies'])
        
        return audit_results
    
    def _audit_labor_costs(self, labor_data: List[Dict], audit_results: Dict) -> Dict:
        """Audit labor costs against MSA rates."""
        for labor_entry in labor_data:
            name = labor_entry.get('name', 'Unknown')
            labor_type = labor_entry.get('type', 'RS')
            actual_rate = float(labor_entry.get('unit_price', 0))
            hours = float(labor_entry.get('total_hours', 0))
            total_cost = float(labor_entry.get('total_cost', actual_rate * hours))
            
            # Get MSA standard rate
            msa_rate = self.msa_manager.get_rate_for_labor_type(labor_type)
            
            if msa_rate is not None:
                # Check rate variance
                rate_variance = abs(actual_rate - msa_rate)
                variance_percentage = (rate_variance / msa_rate) * 100 if msa_rate > 0 else 0
                
                if variance_percentage > 5.0:  # 5% tolerance
                    audit_results['discrepancies'].append({
                        'type': 'rate_variance',
                        'severity': 'high' if variance_percentage > 15 else 'medium',
                        'worker': name,
                        'labor_type': labor_type,
                        'actual_rate': actual_rate,
                        'msa_rate': msa_rate,
                        'variance_amount': rate_variance,
                        'variance_percentage': round(variance_percentage, 2),
                        'description': f"Rate variance for {name} ({labor_type}): ${actual_rate} vs MSA ${msa_rate}"
                    })
                    audit_results['summary']['rate_variances'] += 1
                
                # Update cost tracking
                audit_results['summary']['total_labor_cost'] += total_cost
                audit_results['summary']['expected_labor_cost'] += (msa_rate * hours)
            
            # Check overtime violations
            overtime_threshold = self.msa_manager.get_overtime_threshold(labor_type)
            if hours > overtime_threshold:
                overtime_hours = hours - overtime_threshold
                audit_results['discrepancies'].append({
                    'type': 'overtime_violation',
                    'severity': 'medium',
                    'worker': name,
                    'labor_type': labor_type,
                    'total_hours': hours,
                    'overtime_hours': overtime_hours,
                    'threshold': overtime_threshold,
                    'description': f"Overtime violation for {name}: {hours} hours (>{overtime_threshold} threshold)"
                })
                audit_results['summary']['overtime_violations'] += 1
        
        return audit_results
    
    def _audit_materials(self, materials_data: List[Dict], audit_results: Dict) -> Dict:
        """Audit materials costs (placeholder for future enhancement)."""
        # Future enhancement: Add materials auditing logic
        # For now, just log that materials were found
        logger.info(f"Found {len(materials_data)} material entries for auditing")
        return audit_results


class BedrockAgentManager:
    """Manages interactions with Bedrock Agent with session management and fallback."""
    
    def __init__(self):
        self.agent_id = AGENT_ID
        self.agent_alias_id = AGENT_ALIAS_ID
        self._session_cache = {}  # Local session state cache
    
    def invoke_agent(self, query: str, session_id: str = None) -> Dict[str, Any]:
        """Invoke the Bedrock agent with a query with fallback handling."""
        if not session_id:
            session_id = str(uuid.uuid4())
        
        try:
            # Validate labor type if query contains labor type
            if any(ltype in query.upper() for ltype in ['RS', 'US', 'SS', 'SU', 'EN']):
                labor_type_match = re.search(r'\b(RS|US|SS|SU|EN)\b', query.upper())
                if labor_type_match:
                    labor_type = labor_type_match.group(1)
                    if not InputValidator.validate_labor_type_format(labor_type):
                        return {
                            'session_id': session_id,
                            'error': f"Invalid labor type format: {labor_type}",
                            'status': 'validation_error'
                        }
            
            # Opportunistic KB retrieve to enrich responses on MSA scope/rates
            kb_citations = []
            try:
                if KNOWLEDGE_BASE_ID and any(word in query.lower() for word in ['msa','scope','exception','rate','labor']):
                    kb_resp = bedrock_agent_client.retrieve(
                        knowledgeBaseId=KNOWLEDGE_BASE_ID,
                        retrievalQuery={'text': query[:2000]},
                        retrievalConfiguration={'vectorSearchConfiguration': {'numberOfResults': 3}}
                    )
                    kb_citations = kb_resp.get('retrievalResults', [])
            except Exception as e:
                logger.warning(f"KB retrieve failed: {e}")

            # Try Bedrock agent invocation
            if self.agent_id:
                response = bedrock_agent_client.invoke_agent(
                    agentId=self.agent_id,
                    agentAliasId=self.agent_alias_id,
                    sessionId=session_id,
                    inputText=self._build_agent_prompt(query, history),
                    guardrailConfig={
                        'guardrailIdentifier': GUARDRAIL_ID,
                        'guardrailVersion': '1',
                        'trace': 'ENABLED'
                    }
                )
                
                # Process the response stream
                result = self._process_agent_response(response)
                
                # Store in session cache
                self._session_cache[session_id] = {
                    'last_query': query,
                    'last_response': result,
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                result_payload = {
                    'session_id': session_id,
                    'response': result,
                    'citations': kb_citations,
                    'status': 'success'
                }
                return result_payload
            else:
                # Fallback to local cache if Bedrock unavailable
                return self._fallback_response(query, session_id)
            
        except Exception as e:
            logger.error(f"Error invoking Bedrock agent: {str(e)}")
            # Fallback to local cache response
            return self._fallback_response(query, session_id)
    
    def invoke_agent_async(self, query: str, callback_url: str = None, session_id: str = None) -> Dict[str, Any]:
        """Make lambda invocation async for UI integration."""
        if not session_id:
            session_id = str(uuid.uuid4())
        
        try:
            # Prepare async payload
            async_payload = {
                'action': 'async_agent_query',
                'query': query,
                'session_id': session_id,
                'callback_url': callback_url
            }
            
            # Invoke this same Lambda function asynchronously
            response = lambda_client.invoke(
                FunctionName=os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'agent-lambda'),
                InvocationType='Event',  # Async invocation
                Payload=json.dumps(async_payload)
            )
            
            return {
                'session_id': session_id,
                'status': 'async_started',
                'async_execution_id': response.get('Payload', {}).get('ExecutionId'),
                'message': 'Async processing started. Results will be available via callback or S3.'
            }
            
        except Exception as e:
            logger.error(f"Error starting async agent invocation: {str(e)}")
            return {
                'session_id': session_id,
                'error': str(e),
                'status': 'async_error'
            }
    
    def _process_agent_response(self, response) -> str:
        """Process the streaming response from Bedrock agent."""
        result_text = ""
        
        if 'completion' in response:
            for event in response['completion']:
                if 'chunk' in event:
                    chunk = event['chunk']
                    if 'bytes' in chunk:
                        result_text += chunk['bytes'].decode('utf-8')
        
        return result_text
    
    def _fallback_response(self, query: str, session_id: str) -> Dict[str, Any]:
        """Provide fallback response when Bedrock is unavailable."""
        logger.warning("Using fallback response - Bedrock agent unavailable")
        
        # Simple rule-based responses for common MSA queries
        query_lower = query.lower()
        
        if 'msa rate' in query_lower or 'labor rate' in query_lower:
            fallback_msg = """
Based on standard MSA rates:
- RS (Regular Skilled): $70/hour
- US (Unskilled): $45/hour  
- SS (Semi-Skilled): $55/hour
- SU (Supervisor): $85/hour
- EN (Engineer): $95/hour

Please verify these rates against your current MSA agreement as rates may vary by location and effective date.
"""
        elif 'overtime' in query_lower:
            fallback_msg = "Standard MSA overtime threshold is 40 hours per week. Hours exceeding this threshold require proper documentation and may incur premium rates."
        
        elif 'audit' in query_lower:
            fallback_msg = "MSA invoice audit should check: 1) Labor rates against MSA standards, 2) Overtime compliance, 3) Proper documentation, 4) Calculation accuracy. Contact your MSA administrator for specific requirements."
        
        else:
            fallback_msg = "MSA audit system is currently operating in fallback mode. Please try again later or contact your system administrator."
        
        # Store in local cache
        self._session_cache[session_id] = {
            'last_query': query,
            'last_response': fallback_msg,
            'timestamp': datetime.utcnow().isoformat(),
            'fallback': True
        }
        
        return {
            'session_id': session_id,
            'response': fallback_msg,
            'status': 'fallback_success',
            'note': 'Response generated using fallback logic due to Bedrock unavailability'
        }
    
    def get_session_state(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session state for UI continuity."""
        return self._session_cache.get(session_id)
    
    def clear_session(self, session_id: str) -> bool:
        """Clear session state for cleanup."""
        if session_id in self._session_cache:
            del self._session_cache[session_id]
            return True
        return False


def _extract_discrepancy_flags(audit_results: Dict[str, Any]) -> List[Dict[str, Any]]:
    flags: List[Dict[str, Any]] = []
    if not audit_results:
        return flags
    for entry in audit_results.get('discrepancies', []) or []:
        flags.append(entry)
    return flags


def call_extraction_lambda(bucket: str, key: str) -> Dict[str, Any]:
    """Call the existing extraction Lambda function."""
    try:
        # Get file info
        s3_response = s3_client.head_object(Bucket=bucket, Key=key)
        file_size = s3_response['ContentLength']
        file_extension = os.path.splitext(key)[1].lower()
        
        # Prepare payload for extraction Lambda
        payload = {
            'task': 'extract',
            'input': {
                'file_info': {
                    'key': key,
                    'size': file_size,
                    'extension': file_extension
                },
                'bucket': bucket
            }
        }
        
        # Invoke extraction Lambda
        response = lambda_client.invoke(
            FunctionName=EXTRACTION_LAMBDA_NAME,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        # Parse response
        response_payload = json.loads(response['Payload'].read())
        
        if response.get('StatusCode') == 200:
            return response_payload
        else:
            raise Exception(f"Extraction Lambda failed: {response_payload}")
            
    except Exception as e:
        logger.error(f"Error calling extraction Lambda: {str(e)}")
        raise


def handle_audit_request(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle audit request for an invoice."""
    try:
        # Extract parameters
        bucket = event.get('bucket', BUCKET_NAME)
        key = event.get('key')
        query = event.get('query', 'Audit this invoice against MSA standards')
        
        if not key:
            raise ValueError("Missing required parameter: key")
        
        logger.info(f"Starting audit for {bucket}/{key}")
        
        # Step 1: Extract data from the invoice
        logger.info("Calling extraction Lambda...")
        extracted_data = call_extraction_lambda(bucket, key)
        
        if extracted_data.get('extraction_status') != 'completed':
            raise Exception("Data extraction failed")
        
        # Step 2: Perform audit analysis
        logger.info("Performing audit analysis...")
        auditor = InvoiceAuditor()
        audit_results = auditor.audit_extracted_data(extracted_data)
        
        # Step 3: Prepare context for Bedrock agent
        context = {
            'extracted_data': extracted_data,
            'audit_results': audit_results,
            'file_info': {
                'bucket': bucket,
                'key': key
            }
        }
        
        # Step 4: Create enhanced query with context
        enhanced_query = f"""
        {query}
        
        Context:
        - File: {key}
        - Extraction Status: {extracted_data.get('extraction_status')}
        - Total Discrepancies Found: {audit_results['summary']['total_discrepancies']}
        - Rate Variances: {audit_results['summary']['rate_variances']}
        - Overtime Violations: {audit_results['summary']['overtime_violations']}
        
        Audit Results Summary:
        {json.dumps(audit_results, indent=2)}
        
        Please provide a comprehensive audit report with recommendations.
        """
        
        # Step 5: Invoke Bedrock agent
        logger.info("Invoking Bedrock agent...")
        agent_manager = BedrockAgentManager()
        agent_response = agent_manager.invoke_agent(enhanced_query)

        # If discrepancies require human review, return early with pending status
        audit_discrepancies = audit_results.get('discrepancies', [])
        task_token = event.get('taskToken')
        if audit_discrepancies:
            pending_payload = {
                'status': 'pending_approval',
                'session_id': agent_response.get('session_id'),
                'flags': audit_discrepancies,
                'audit_results': audit_results,
                'file_info': context['file_info'],
                'task_token': task_token
            }
            logger.info("HITL approval required; pausing workflow")
            return pending_payload
        
        # Step 6: Combine results
        final_result = {
            'audit_id': audit_results['audit_id'],
            'timestamp': audit_results['timestamp'],
            'file_info': context['file_info'],
            'extraction_summary': {
                'status': extracted_data.get('extraction_status'),
                'page_count': extracted_data.get('raw_extracted_data', {}).get('page_count', 0) or extracted_data.get('extracted_data', {}).get('page_count', 0),
                'processing_method': extracted_data.get('processing_summary', {}).get('processing_method')
            },
            'audit_results': audit_results,
            'agent_response': agent_response,
            'recommendations': agent_response.get('response', ''),
            'status': 'completed'
        }
        
        logger.info(f"Audit completed successfully for {key}")
        return final_result
        
    except Exception as e:
        logger.error(f"Error in audit request: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }


def handle_async_agent_query(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle async agent query processing."""
    try:
        query = event.get('query')
        session_id = event.get('session_id')
        callback_url = event.get('callback_url')
        
        if not query:
            raise ValueError("Missing required parameter: query")
        
        logger.info(f"Processing async agent query (session: {session_id})")
        
        # Process the query synchronously in this async context
        agent_manager = BedrockAgentManager()
        result = agent_manager.invoke_agent(query, session_id)
        
        # If callback URL is provided, send results there
        if callback_url:
            try:
                import requests
                requests.post(callback_url, json=result, timeout=30)
                logger.info(f"Results sent to callback URL: {callback_url}")
            except Exception as e:
                logger.error(f"Failed to send callback: {e}")
        
        # Store results in S3 for retrieval
        if BUCKET_NAME:
            result_key = f"async-results/{session_id}.json"
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=result_key,
                Body=json.dumps(result, default=str),
                ContentType='application/json'
            )
            logger.info(f"Async results stored: s3://{BUCKET_NAME}/{result_key}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in async agent query: {str(e)}")
        return {
            'status': 'async_error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }


def handle_hitl_approval(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle human-in-the-loop approval workflow."""
    try:
        approved = event.get('approved', False)
        session_id = event.get('session_id')
        execution_arn = event.get('execution_arn')
        comparison_result = event.get('comparison_result', {})
        bucket = event.get('bucket')
        key = event.get('key')
        task_token = event.get('taskToken')

        logger.info(f"Processing HITL approval for {bucket}/{key} (approved={approved})")

        if task_token:
            sfn = stepfunctions_client
            try:
                if approved:
                    sfn.send_task_success(
                        taskToken=task_token,
                        output=json.dumps({'decision': 'approved'})
                    )
                else:
                    sfn.send_task_failure(
                        taskToken=task_token,
                        error='Rejected',
                        cause='User rejected discrepancies'
                    )
            except Exception as e:
                logger.error(f"Failed to signal Step Functions: {e}")

        if session_id:
            manager = BedrockAgentManager()
            manager.clear_session(session_id)

        return {
            'status': 'hitl_processed',
            'approved': approved,
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error in HITL approval: {str(e)}")
        return {
            'status': 'hitl_error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start_time = time.time()
    try:
        logger.info(f"Received event: {json.dumps(event, default=str)}")
        
        # Validate basic event structure
        if not isinstance(event, dict):
            raise ValueError("Event must be a dictionary")
        
        # Determine the action to perform
        action = event.get('action', 'audit')
        
        if action == 'audit':
            return handle_audit_request(event)
        elif action == 'query':
            # Direct agent query without file processing
            query = event.get('query')
            if not query:
                raise ValueError("Missing required parameter: query")
            
            # Validate query length
            if len(query) > 10000:
                raise ValueError("Query too long (max 10,000 characters)")
            
            session_id = event.get('session_id')
            use_async = event.get('async', False)
            
            agent_manager = BedrockAgentManager()
            
            if use_async:
                callback_url = event.get('callback_url')
                return agent_manager.invoke_agent_async(query, callback_url, session_id)
            else:
                return agent_manager.invoke_agent(query, session_id)
                
        elif action == 'async_agent_query':
            # Internal action for async processing
            return handle_async_agent_query(event)
        elif action == 'hitl_approval':
            # Human-in-the-loop approval handling
            return handle_hitl_approval(event)
        elif action == 'health_check':
            # Health check endpoint
            config_validation = InputValidator.validate_agent_configuration()
            return {
                'status': 'healthy' if config_validation['valid'] else 'degraded',
                'timestamp': datetime.utcnow().isoformat(),
                'configuration': config_validation,
                'version': '1.0.0'
            }
        else:
            raise ValueError(f"Unknown action: {action}")
            
    except ValueError as e:
        logger.warning(f"Validation error: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': str(e),
                'error_type': 'validation_error',
                'timestamp': datetime.utcnow().isoformat()
            })
        }
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'error_type': 'internal_error',
                'timestamp': datetime.utcnow().isoformat()
            })
        }
    finally:
        duration = time.time() - start_time
        logger.info(f"Metrics: duration={duration:.2f}s")
        gc.collect()


if __name__ == '__main__':
    # Test locally
    test_event = {
        'action': 'audit',
        'bucket': 'test-bucket',
        'key': 'test-invoice.pdf',
        'query': 'Audit this invoice against MSA standards'
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, default=str))
