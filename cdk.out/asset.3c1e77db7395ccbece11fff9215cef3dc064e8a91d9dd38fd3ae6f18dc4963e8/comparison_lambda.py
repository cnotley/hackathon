"""
Comparison and Discrepancy Flagging Lambda Function

This module implements intelligent comparison of extracted invoice data against
MSA standards using Bedrock for analysis and SageMaker for anomaly detection.
It flags discrepancies, calculates savings, and provides detailed audit reports.
"""

import json
import logging
import os
import uuid
import boto3  # type: ignore
import pandas as pd  # type: ignore
import numpy as np  # type: ignore
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import uuid
from decimal import Decimal
import re
import copy
import time
from botocore.exceptions import ClientError  # type: ignore
import gc

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.getenv('LOG_LEVEL', 'INFO'))

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
bedrock_client = boto3.client('bedrock-runtime')
s3_client = boto3.client('s3')
sagemaker_client = boto3.client('sagemaker-runtime')
s3_client = boto3.client('s3')
bedrock_agent_runtime = boto3.client('bedrock-agent-runtime')
comprehend_client = boto3.client('comprehend')

# Custom exceptions
class ValidationError(Exception):
    """Custom exception for data validation errors."""
    pass


# Environment variables
MSA_RATES_TABLE = os.getenv('MSA_RATES_TABLE', 'msa-rates')
BEDROCK_MODEL_ID = os.getenv('BEDROCK_MODEL_ID', 'anthropic.claude-3-5-sonnet-20241022-v2:0')
GUARDRAIL_ID = os.getenv('GUARDRAIL_ID', 'default-guardrail')
SAGEMAKER_ENDPOINT = os.getenv('SAGEMAKER_ENDPOINT', 'invoice-anomaly-detection')
BUCKET_NAME = os.getenv('BUCKET_NAME')
KNOWLEDGE_BASE_ID = os.getenv('KNOWLEDGE_BASE_ID')
DEFAULT_EFFECTIVE_DATE = os.getenv('MSA_DEFAULT_EFFECTIVE_DATE', '2024-01-01')

# Configuration constants
VARIANCE_THRESHOLD = 0.05  # 5% variance threshold
OVERTIME_THRESHOLD = 40.0  # Standard overtime threshold
ANOMALY_THRESHOLD = 2.0    # Anomaly detection threshold (standard deviations)
SAGEMAKER_MAX_RETRIES = 3
SAGEMAKER_RETRY_DELAY = 2  # seconds
ANOMALY_THRESHOLD = float(os.getenv('ANOMALY_THRESHOLD', '2.0'))


class DataValidator:
    """Validates input data for negative values and data integrity."""
    
    @staticmethod
    def validate_extracted_data(extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate extracted data for negative values and integrity issues."""
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'corrected_data': extracted_data.copy()
        }
        
        try:
            # Validate labor data
            labor_data = extracted_data.get('normalized_data', {}).get('labor', [])
            corrected_labor = []
            
            if extracted_data.get('normalized_data', {}).get('materials'):
                raise ValidationError("Materials handling removed")
            
            for i, labor in enumerate(labor_data):
                labor_errors = []
                corrected_labor_entry = labor.copy()
                
                # Check for negative values
                unit_price = labor.get('unit_price', 0)
                total_hours = labor.get('total_hours', 0)
                total_cost = labor.get('total_cost', 0)
                
                if unit_price < 0:
                    labor_errors.append(f"Negative unit_price: {unit_price}")
                    corrected_labor_entry['unit_price'] = abs(unit_price)
                    validation_results['warnings'].append(f"Labor entry {i}: Corrected negative unit_price to {abs(unit_price)}")
                
                if total_hours < 0:
                    labor_errors.append(f"Negative total_hours: {total_hours}")
                    corrected_labor_entry['total_hours'] = abs(total_hours)
                    validation_results['warnings'].append(f"Labor entry {i}: Corrected negative total_hours to {abs(total_hours)}")
                
                if total_cost < 0:
                    labor_errors.append(f"Negative total_cost: {total_cost}")
                    corrected_labor_entry['total_cost'] = abs(total_cost)
                    validation_results['warnings'].append(f"Labor entry {i}: Corrected negative total_cost to {abs(total_cost)}")
                
                # Check for unrealistic values
                if unit_price > 1000:  # $1000/hour seems unrealistic
                    validation_results['warnings'].append(f"Labor entry {i}: Unusually high unit_price: ${unit_price}")
                
                if total_hours > 168:  # More than hours in a week
                    validation_results['warnings'].append(f"Labor entry {i}: Unusually high total_hours: {total_hours}")
                
                # Check for missing critical fields
                if not labor.get('name') or labor.get('name', '').strip() == '':
                    labor_errors.append("Missing or empty worker name")
                
                if not labor.get('type') or labor.get('type', '').strip() == '':
                    corrected_labor_entry['type'] = 'RS'  # Default to Regular Skilled
                    validation_results['warnings'].append(f"Labor entry {i}: Missing labor type, defaulted to 'RS'")
                
                if labor_errors:
                    validation_results['errors'].extend([f"Labor entry {i}: {error}" for error in labor_errors])
                
                corrected_labor.append(corrected_labor_entry)
            
            # Update corrected data
            if 'normalized_data' not in validation_results['corrected_data']:
                validation_results['corrected_data']['normalized_data'] = {}
            
            validation_results['corrected_data']['normalized_data']['labor'] = corrected_labor
            validation_results['corrected_data']['normalized_data'].pop('materials', None)
            
            # Set validation status
            if validation_results['errors']:
                validation_results['valid'] = False
            
            logger.info(f"Data validation completed: {len(validation_results['errors'])} errors, {len(validation_results['warnings'])} warnings")
            
        except Exception as e:
            logger.error(f"Error during data validation: {str(e)}")
            validation_results['valid'] = False
            validation_results['errors'].append(f"Validation process error: {str(e)}")
        
        return validation_results
    

def _rate_key(labor_type: str, location: str) -> Dict[str, str]:
    return {
        'rate_id': f"{str(labor_type).upper()}#{location}",
        'effective_date': DEFAULT_EFFECTIVE_DATE
    }


class MSARatesComparator:
    """Handles comparison of extracted data against MSA rates."""
    
    def __init__(self):
        self.table = dynamodb.Table(MSA_RATES_TABLE)
        self.rates_cache = {}
    
    def get_msa_rate(self, labor_type: str, location: str = 'default') -> Optional[float]:
        """Get MSA rate with caching."""
        cache_key = f"{labor_type}:{location}"
        
        if cache_key in self.rates_cache:
            return self.rates_cache[cache_key]
        
        try:
            response = self.table.get_item(Key=_rate_key(labor_type, location))
            
            if 'Item' in response:
                rate = float(response['Item']['standard_rate'])
                self.rates_cache[cache_key] = rate
                return rate
            
            # Fallback to default location
            if location != 'default':
                return self.get_msa_rate(labor_type, 'default')
            
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving MSA rate for {labor_type}: {str(e)}")
            return None
    
    def get_overtime_threshold(self, labor_type: str = 'default') -> float:
        """Get overtime threshold for labor type."""
        try:
            response = self.table.get_item(Key=_rate_key(labor_type, 'overtime_rules'))
            
            if 'Item' in response:
                return float(response['Item'].get('weekly_threshold', OVERTIME_THRESHOLD))
            
            return OVERTIME_THRESHOLD
            
        except Exception as e:
            logger.error(f"Error retrieving overtime threshold: {str(e)}")
            return OVERTIME_THRESHOLD


class BedrockAnalyzer:
    """Handles Bedrock-based intelligent analysis of discrepancies."""
    
    def __init__(self):
        self.model_id = BEDROCK_MODEL_ID
    
    def analyze_discrepancies(self, extracted_data: Dict, comparison_results: Dict) -> Dict[str, Any]:
        """Use Bedrock to analyze discrepancies and provide insights."""
        try:
            # Prepare analysis prompt
            prompt = self._build_analysis_prompt(extracted_data, comparison_results)
            
            # Call Bedrock
            response = bedrock_client.invoke_model(
                modelId=self.model_id,
                body=json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 2000,
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                }),
                guardrailConfig={
                    'guardrailIdentifier': GUARDRAIL_ID,
                    'guardrailVersion': '1',
                    'trace': 'ENABLED'
                }
            )
            
            # Parse response
            response_body = json.loads(response['body'].read())
            analysis = response_body['content'][0]['text']
            
            return {
                'bedrock_analysis': analysis,
                'analysis_timestamp': datetime.utcnow().isoformat(),
                'model_used': self.model_id
            }
            
        except Exception as e:
            logger.error(f"Error in Bedrock analysis: {str(e)}")
            return {
                'bedrock_analysis': 'Analysis unavailable due to service error',
                'error': str(e)
            }
    
    def _build_analysis_prompt(self, extracted_data: Dict, comparison_results: Dict) -> str:
        """Build analysis prompt for Bedrock."""
        discrepancies = comparison_results.get('discrepancies', [])
        total_savings = comparison_results.get('summary', {}).get('total_potential_savings', 0)
        
        if extracted_data.get('normalized_data', {}).get('materials'):
            raise ValidationError("Materials handling removed")

        prompt = f"""
You are an expert invoice auditor analyzing discrepancies found in contractor invoices against MSA (Master Services Agreement) standards. Only reference the provided JSON data; do not infer beyond what is supplied.

EXTRACTED DATA SUMMARY:
- Total Labor Entries: {len(extracted_data.get('normalized_data', {}).get('labor', []))}

DISCREPANCIES FOUND: {len(discrepancies)}
POTENTIAL SAVINGS: ${total_savings:,.2f}

DETAILED DISCREPANCIES:
{json.dumps(discrepancies, indent=2)}

Please provide:
1. Risk assessment (High/Medium/Low) for each discrepancy type
2. Recommended actions for each discrepancy
3. Compliance concerns and regulatory implications
4. Suggestions for preventing similar issues in future invoices
5. Priority ranking of discrepancies by financial impact

Focus on actionable insights and specific recommendations for the audit team.
"""
        return prompt


class AnomalyDetector:
    """Handles SageMaker-based anomaly detection for outlier identification."""
    
    def __init__(self):
        self.endpoint_name = SAGEMAKER_ENDPOINT
    
    def detect_anomalies(self, extracted_data: Dict) -> List[Dict[str, Any]]:
        """Detect anomalies in extracted data using SageMaker with labor-specific features."""
        try:
            labor_df = self._prepare_labor_dataframe(extracted_data)
            if labor_df.empty:
                logger.info("No labor data available for anomaly detection")
                return []

            features = labor_df[['total_hours', 'ot_hours', 'unit_price', 'total_cost']].values.astype(float)
            features = self._scale_features(features)

            max_retries = SAGEMAKER_MAX_RETRIES
            retry_delay = SAGEMAKER_RETRY_DELAY

            for attempt in range(max_retries):
                try:
                    response = sagemaker_client.invoke_endpoint(
                        EndpointName=self.endpoint_name,
                        ContentType='application/json',
                        Body=json.dumps({'instances': features.tolist()})
                    )
                    result = json.loads(response['Body'].read().decode())
                    anomalies = self._process_anomaly_results(result, labor_df)

                    # Append rule-based anomalies for overtime/cost thresholds
                    anomalies.extend(self._rule_based_anomalies(labor_df))

                    logger.info(f"SageMaker anomaly detection completed: {len(anomalies)} anomalies")
                    try:
                        s3_client.put_object(
                            Bucket=BUCKET_NAME,
                            Key=f"anomalies/{uuid.uuid4()}.json",
                            Body=json.dumps({'features': features.tolist(), 'predictions': result}, default=str),
                            ContentType='application/json'
                        )
                    except Exception as log_e:
                        logger.warning(f"Failed to log anomaly inference: {log_e}")
                    return anomalies

                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    if error_code in {'ModelError', 'ValidationException'}:
                        logger.error(f"SageMaker model/validation error: {e}")
                        return self._statistical_anomaly_detection(labor_df)
                    if error_code in ['ThrottlingException', 'ServiceUnavailableException'] and attempt < max_retries - 1:
                        logger.warning(f"SageMaker throttled/unavailable (attempt {attempt + 1}), retrying in {retry_delay}s")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    logger.error(f"SageMaker client error: {e}")
                    if attempt == max_retries - 1:
                        return self._statistical_anomaly_detection(labor_df)
                    time.sleep(retry_delay)
                except Exception as e:
                    logger.error(f"Unexpected SageMaker error on attempt {attempt + 1}: {e}")
                    if attempt == max_retries - 1:
                        return self._statistical_anomaly_detection(labor_df)
                    time.sleep(retry_delay)

            logger.error("All SageMaker attempts failed, using statistical fallback")
            return self._statistical_anomaly_detection(labor_df)

        except Exception as e:
            logger.error(f"Error preparing anomaly detection data: {e}")
            return self._statistical_anomaly_detection(self._prepare_labor_dataframe(extracted_data, safe_mode=True))

    def _prepare_labor_dataframe(self, extracted_data: Dict, safe_mode: bool = False) -> pd.DataFrame:
        if extracted_data.get('normalized_data', {}).get('materials'):
            raise ValidationError("Materials handling removed")
        labor_data = extracted_data.get('normalized_data', {}).get('labor', []) or []
        if not labor_data:
            return pd.DataFrame()
        labor_df = pd.DataFrame(labor_data)
        numeric_columns = {
            'total_hours': 0.0,
            'unit_price': 0.0,
            'total_cost': None
        }
        for col, default in numeric_columns.items():
            if col not in labor_df:
                labor_df[col] = default
            labor_df[col] = pd.to_numeric(labor_df[col], errors='coerce').fillna(default if default is not None else 0.0)
        # Derive total cost when missing
        missing_cost = labor_df['total_cost'] == 0
        labor_df.loc[missing_cost, 'total_cost'] = labor_df.loc[missing_cost, 'unit_price'] * labor_df.loc[missing_cost, 'total_hours']
        labor_df['location'] = labor_df.get('location', 'default').fillna('default')
        labor_df['labor_type'] = labor_df.get('type', 'RS').fillna('RS')
        labor_df['ot_hours'] = (labor_df['total_hours'] - OVERTIME_THRESHOLD).clip(lower=0.0)
        if safe_mode:
            return labor_df
        return labor_df

    def _scale_features(self, features: np.ndarray) -> np.ndarray:
        means = np.mean(features, axis=0)
        stds = np.std(features, axis=0)
        stds[stds == 0] = 1.0
        return ((features - means) / stds).tolist()

    def _process_anomaly_results(self, sagemaker_result: Dict, labor_df: pd.DataFrame) -> List[Dict[str, Any]]:
        anomalies: List[Dict[str, Any]] = []
        raw_predictions = sagemaker_result.get('predictions') or sagemaker_result.get('scores') or []
        scores: List[float] = []
        for item in raw_predictions:
            if isinstance(item, dict):
                score = item.get('anomaly_score')
                if score is None:
                    score = item.get('score')
                if score is None and 'scores' in item and isinstance(item['scores'], list):
                    score = item['scores'][0]
            else:
                score = item
            if isinstance(score, (int, float)):
                scores.append(float(score))

        for row, score in zip(labor_df.itertuples(index=False), scores):
            if abs(score) > ANOMALY_THRESHOLD:
                anomalies.append({
                    'type': 'labor_anomaly',
                    'category': 'sagemaker_isolation_forest',
                    'item': getattr(row, 'name', None) or row.__dict__.get('name', 'Unknown'),
                    'labor_type': getattr(row, 'labor_type', 'Unknown'),
                    'anomaly_score': round(score, 2),
                    'total_hours': round(row.total_hours, 2),
                    'total_cost': round(row.total_cost, 2),
                    'description': f"Isolation forest flagged cost ${row.total_cost:,.2f} for {getattr(row, 'name', 'Unknown')}"
                })
        return anomalies

    def _rule_based_anomalies(self, labor_df: pd.DataFrame) -> List[Dict[str, Any]]:
        anomalies: List[Dict[str, Any]] = []
        rates = MSARatesComparator()
        for row in labor_df.itertuples(index=False):
            worker_name = getattr(row, 'name', None) or row.__dict__.get('name', 'Unknown')
            labor_type = getattr(row, 'labor_type', 'RS')
            hours = float(row.total_hours)
            if row.ot_hours > 0:
                anomalies.append({
                    'type': 'labor_anomaly',
                    'category': 'overtime_spike',
                    'item': worker_name,
                    'labor_type': labor_type,
                    'total_hours': round(hours, 2),
                    'overtime_hours': round(row.ot_hours, 2),
                    'description': f"Overtime detected: {hours:.1f} hours (> {OVERTIME_THRESHOLD})"
                })
            expected_rate = rates.get_msa_rate(str(labor_type), getattr(row, 'location', 'default'))
            if expected_rate:
                expected_cost = expected_rate * hours
                if row.total_cost > expected_cost * 1.1:
                    anomalies.append({
                        'type': 'labor_anomaly',
                        'category': 'cost_threshold',
                        'item': worker_name,
                        'labor_type': labor_type,
                        'total_cost': round(row.total_cost, 2),
                        'expected_cost': round(expected_cost, 2),
                        'description': f"Cost ${row.total_cost:,.2f} exceeds MSA expectation (${expected_cost:,.2f})"
                    })
        return anomalies

    def _statistical_anomaly_detection(self, labor_df: pd.DataFrame) -> List[Dict[str, Any]]:
        anomalies: List[Dict[str, Any]] = []
        try:
            if labor_df.empty:
                return anomalies
            valid_costs = labor_df['total_cost'].replace([np.inf, -np.inf], np.nan).dropna()
            if valid_costs.empty:
                return anomalies
            mean_cost = valid_costs.mean()
            std_cost = valid_costs.std()
            if std_cost == 0:
                return anomalies
            for row in labor_df.itertuples(index=False):
                cost = float(row.total_cost)
                if cost <= 0:
                    continue
                z_score = abs((cost - mean_cost) / std_cost)
                if z_score > ANOMALY_THRESHOLD:
                    anomalies.append({
                        'type': 'labor_anomaly',
                        'category': 'statistical_outlier',
                        'item': getattr(row, 'name', 'Unknown'),
                        'labor_type': getattr(row, 'labor_type', 'Unknown'),
                        'z_score': round(z_score, 2),
                        'value': round(cost, 2),
                        'description': f"Labor cost ${cost:,.2f} is {z_score:.1f} std devs from mean (${mean_cost:,.2f})"
                    })
        except Exception as e:
            logger.error(f"Statistical anomaly fallback failed: {e}")
        return anomalies


def _calculate_rate_variances(extracted: Dict[str, Any], rates: MSARatesComparator) -> Tuple[List[Dict[str, Any]], float]:
    """Compute rate variances and total potential savings from normalized labor data."""
    variances: List[Dict[str, Any]] = []
    total_savings: float = 0.0

    labor_entries = extracted.get('normalized_data', {}).get('labor', [])
    for entry in labor_entries:
        try:
            labor_type = str(entry.get('type', 'RS'))
            location = str(entry.get('location', 'default'))
            billed_rate = float(entry.get('unit_price', 0) or 0)
            hours = float(entry.get('total_hours', 0) or 0)
            expected_rate = rates.get_msa_rate(labor_type, location)
            if expected_rate is None or expected_rate <= 0:
                continue

            variance_pct = ((billed_rate - expected_rate) / expected_rate) * 100.0
            if variance_pct > VARIANCE_THRESHOLD * 100.0:
                variance_amount = (billed_rate - expected_rate) * hours
                total_savings += max(0.0, variance_amount)
                variances.append({
                    'worker': entry.get('name', 'Unknown'),
                    'labor_type': labor_type,
                    'location': location,
                    'billed_rate': round(billed_rate, 2),
                    'msa_rate': round(expected_rate, 2),
                    'variance_percentage': round(variance_pct, 2),
                    'variance_amount': round(variance_amount, 2),
                    'hours': hours
                })
        except Exception as e:
            logger.warning(f"Rate variance calc error: {e}")
            continue

    return variances, round(total_savings, 2)


def _detect_overtime_violations(extracted: Dict[str, Any], rates: MSARatesComparator) -> List[Dict[str, Any]]:
    """Detect overtime violations based on thresholds per labor type."""
    violations: List[Dict[str, Any]] = []
    for entry in extracted.get('normalized_data', {}).get('labor', []):
        try:
            labor_type = str(entry.get('type', 'RS'))
            hours = float(entry.get('total_hours', 0) or 0)
            threshold = rates.get_overtime_threshold(labor_type)
            if hours > threshold:
                violations.append({
                    'worker': entry.get('name', 'Unknown'),
                    'labor_type': labor_type,
                    'total_hours': hours,
                    'overtime_hours': round(hours - threshold, 2),
                    'threshold': threshold
                })
        except Exception as e:
            logger.warning(f"Overtime detection error: {e}")
            continue
    return violations


def _detect_duplicates(extracted: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Detect duplicate labor entries by key fields (worker, type, hours, rate, week)."""
    seen = {}
    duplicates: List[Dict[str, Any]] = []
    for entry in extracted.get('normalized_data', {}).get('labor', []):
        key = (
            entry.get('name', '').strip().lower(),
            str(entry.get('type', 'RS')).upper(),
            float(entry.get('total_hours', 0) or 0),
            float(entry.get('unit_price', 0) or 0),
            entry.get('week', 'unknown')
        )
        if key in seen:
            duplicates.append({
                'worker': entry.get('name', 'Unknown'),
                'labor_type': entry.get('type', 'RS'),
                'hours': entry.get('total_hours', 0),
                'rate': entry.get('unit_price', 0),
                'week': entry.get('week', 'unknown'),
                'duplicate_of_index': seen[key]
            })
        else:
            seen[key] = len(seen)
    return duplicates


def _validate_classifications(extracted: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Validate labor classification using Comprehend entity hints (lightweight)."""
    issues: List[Dict[str, Any]] = []
    for entry in extracted.get('normalized_data', {}).get('labor', []):
        try:
            t = str(entry.get('type','')).upper()
            desc = str(entry.get('name','')) + ' ' + str(entry.get('description',''))
            if t not in {'RS','US','SS','SU','EN'}:
                issues.append({'type':'classification_error','worker':entry.get('name','Unknown'),'value':t})
                continue
            if desc.strip():
                resp = comprehend_client.detect_entities(Text=desc[:4000], LanguageCode='en')
                entity_texts = {e.get('Text','').lower() for e in resp.get('Entities', [])}
                mapping = {
                    'EN': {'engineer','professional','pe'},
                    'SU': {'supervisor','foreman','manager'},
                    'SS': {'semi-skilled','semi skilled','technician','journeyman'},
                    'RS': {'skilled','regular','craft'},
                    'US': {'laborer','helper','unskilled'}
                }
                expected = mapping.get(t, set())
                if expected and not (expected & entity_texts):
                    issues.append({'type':'classification_mismatch','worker':entry.get('name','Unknown'),'labor_type':t,'entities':list(entity_texts)})
        except Exception:
            continue
    return issues


def _check_scope_with_kb(description: str) -> Dict[str, Any]:
    """Use Bedrock Retrieve against KB to check scope-of-work relevance."""
    if not description:
        return {'in_scope': False, 'score': 0.0, 'evidence': []}
    try:
        if not KNOWLEDGE_BASE_ID:
            return {'in_scope': True, 'score': 0.5, 'evidence': []}
        resp = bedrock_agent_runtime.retrieve(
            knowledgeBaseId=KNOWLEDGE_BASE_ID,
            retrievalQuery={'text': description[:2000]},
            retrievalConfiguration={'vectorSearchConfiguration': {'numberOfResults': 3}}
        )
        citations = resp.get('retrievalResults', [])
        top_score = max((c.get('score', 0.0) for c in citations), default=0.0)
        return {'in_scope': top_score >= 0.5, 'score': float(top_score), 'evidence': citations}
    except Exception as e:
        logger.warning(f"KB retrieve failed: {e}")
        return {'in_scope': True, 'score': 0.5, 'evidence': []}


def _check_management_to_labor_ratio(extracted: Dict[str, Any], rates: MSARatesComparator) -> List[Dict[str, Any]]:
    """Heuristic: SU:RS <= 1:6 unless overridden in DynamoDB (location 'ratio_rules')."""
    issues: List[Dict[str, Any]] = []
    try:
        table = dynamodb.Table(MSA_RATES_TABLE)
        default_ratio = 6.0
        try:
            rr = table.get_item(Key=_rate_key('SU', 'ratio_rules')).get('Item')
            if rr and 'max_ratio' in rr:
                default_ratio = float(rr['max_ratio'])
        except Exception:
            pass
        labor = extracted.get('normalized_data', {}).get('labor', [])
        su_hours = sum(float(e.get('total_hours',0) or 0) for e in labor if str(e.get('type','')).upper()=='SU')
        rs_hours = sum(float(e.get('total_hours',0) or 0) for e in labor if str(e.get('type','')).upper()=='RS')
        if rs_hours > 0:
            ratio = su_hours / rs_hours
            if ratio > (1.0 / default_ratio):
                issues.append({
                    'type':'management_to_labor_ratio',
                    'supervisor_hours': round(su_hours,2),
                    'rs_hours': round(rs_hours,2),
                    'observed_ratio': round(ratio,3),
                    'policy_max_ratio': f"1:{int(default_ratio)}",
                    'description': f"Supervisor hours exceed policy (SU:RS should be <= 1:{int(default_ratio)})"
                })
    except Exception as e:
        logger.warning(f"Ratio heuristic failed: {e}")
    return issues


def _normalize_input(event: Dict[str, Any]) -> Dict[str, Any]:
    """Accept either full extraction payload or 'extraction_data' subfield."""
    if 'extraction_data' in event and isinstance(event['extraction_data'], dict):
        return {'normalized_data': event['extraction_data'].get('normalized_data', {}), **event['extraction_data']}
    # Full extraction lambda output case
    if 'normalized_data' in event:
        return event
    # Step Functions style: $.extraction.Payload
    if 'extraction' in event and isinstance(event['extraction'], dict):
        payload = event['extraction'].get('Payload', {})
        if isinstance(payload, dict):
            return payload
    return {}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start_time = time.time()
    try:
        logger.info(f"Received comparison event: {json.dumps(event, default=str)[:1000]}")
        try:
            extracted = _normalize_input(event)
            if not extracted:
                raise ValidationError("Missing or invalid extraction data")

            # Validate numerical integrity and sanitize negatives
            validation = DataValidator.validate_extracted_data(extracted)
            corrected = validation.get('corrected_data', extracted)

            # Compute discrepancies
            rates = MSARatesComparator()
            rate_variances, total_savings = _calculate_rate_variances(corrected, rates)
            overtime_violations = _detect_overtime_violations(corrected, rates)

            # Anomaly detection (SageMaker with fallback)
            anomalies = AnomalyDetector().detect_anomalies(corrected)

            # Duplicate detection
            duplicates = _detect_duplicates(corrected)

            # Classification validation and scope checks (labor-focused)
            classification_issues = _validate_classifications(corrected)
            for item in corrected.get('normalized_data', {}).get('labor', []):
                scope = _check_scope_with_kb(item.get('description', item.get('name','')))
                if not scope.get('in_scope', True):
                    rate_variances.append({'type':'scope_flag','item':item.get('name',''), 'score': scope.get('score',0.0)})

            # Management-to-labor ratio heuristic
            ratio_flags = _check_management_to_labor_ratio(corrected, rates)

            analysis = {
                'rate_variances': rate_variances,
                'overtime_violations': overtime_violations,
                'anomalies': anomalies,
                'duplicates': duplicates,
                'ratio_flags': ratio_flags,
                'total_savings': total_savings,
                'summary': {
                    'total_discrepancies': len(rate_variances) + len(overtime_violations) + len(anomalies) + len(duplicates) + len(ratio_flags) + len(classification_issues),
                    'rate_variances': len(rate_variances),
                    'overtime_violations': len(overtime_violations),
                    'anomalies': len(anomalies),
                    'duplicates': len(duplicates),
                    'classification_issues': len(classification_issues)
                }
            }

            return {
                'statusCode': 200,
                'discrepancy_analysis': analysis
            }

        except ValidationError as e:
            logger.warning(f"Validation error: {e}")
            return {
                'statusCode': 400,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Comparison handler error: {e}")
            return {
                'statusCode': 500,
                'error': str(e)
            }
    finally:
        duration = time.time() - start_time
        logger.info(f"Metrics: duration={duration:.2f}s")
        gc.collect()
