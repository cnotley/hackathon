"""
Comparison and Discrepancy Flagging Lambda Function

This module implements intelligent comparison of extracted invoice data against
MSA standards using Bedrock for analysis and SageMaker for anomaly detection.
It flags discrepancies, calculates savings, and provides detailed audit reports.
"""

import json
import logging
import os
import boto3
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import uuid
from decimal import Decimal
import re
import time
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.getenv('LOG_LEVEL', 'INFO'))

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
bedrock_client = boto3.client('bedrock-runtime')
sagemaker_client = boto3.client('sagemaker-runtime')
s3_client = boto3.client('s3')

# Custom exceptions
class ValidationError(Exception):
    """Custom exception for data validation errors."""
    pass

class SageMakerError(Exception):
    """Custom exception for SageMaker-related errors."""
    pass

# Environment variables
MSA_RATES_TABLE = os.getenv('MSA_RATES_TABLE', 'msa-rates')
BEDROCK_MODEL_ID = os.getenv('BEDROCK_MODEL_ID', 'anthropic.claude-3-5-sonnet-20241022-v2:0')
SAGEMAKER_ENDPOINT = os.getenv('SAGEMAKER_ENDPOINT', 'invoice-anomaly-detection')
BUCKET_NAME = os.getenv('BUCKET_NAME')

# Configuration constants
VARIANCE_THRESHOLD = 0.05  # 5% variance threshold
OVERTIME_THRESHOLD = 40.0  # Standard overtime threshold
ANOMALY_THRESHOLD = 2.0    # Anomaly detection threshold (standard deviations)
SAGEMAKER_MAX_RETRIES = 3
SAGEMAKER_RETRY_DELAY = 2  # seconds


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
            
            # Validate materials data
            materials_data = extracted_data.get('normalized_data', {}).get('materials', [])
            corrected_materials = []
            
            for i, material in enumerate(materials_data):
                material_errors = []
                corrected_material_entry = material.copy()
                
                # Check for negative values
                unit_price = material.get('unit_price', 0)
                quantity = material.get('quantity', 0)
                total_cost = material.get('total_cost', 0)
                
                if unit_price < 0:
                    material_errors.append(f"Negative unit_price: {unit_price}")
                    corrected_material_entry['unit_price'] = abs(unit_price)
                    validation_results['warnings'].append(f"Material entry {i}: Corrected negative unit_price to {abs(unit_price)}")
                
                if quantity < 0:
                    material_errors.append(f"Negative quantity: {quantity}")
                    corrected_material_entry['quantity'] = abs(quantity)
                    validation_results['warnings'].append(f"Material entry {i}: Corrected negative quantity to {abs(quantity)}")
                
                if total_cost < 0:
                    material_errors.append(f"Negative total_cost: {total_cost}")
                    corrected_material_entry['total_cost'] = abs(total_cost)
                    validation_results['warnings'].append(f"Material entry {i}: Corrected negative total_cost to {abs(total_cost)}")
                
                # Check for missing description
                if not material.get('description') or material.get('description', '').strip() == '':
                    material_errors.append("Missing or empty material description")
                
                if material_errors:
                    validation_results['errors'].extend([f"Material entry {i}: {error}" for error in material_errors])
                
                corrected_materials.append(corrected_material_entry)
            
            # Update corrected data
            if 'normalized_data' not in validation_results['corrected_data']:
                validation_results['corrected_data']['normalized_data'] = {}
            
            validation_results['corrected_data']['normalized_data']['labor'] = corrected_labor
            validation_results['corrected_data']['normalized_data']['materials'] = corrected_materials
            
            # Set validation status
            if validation_results['errors']:
                validation_results['valid'] = False
            
            logger.info(f"Data validation completed: {len(validation_results['errors'])} errors, {len(validation_results['warnings'])} warnings")
            
        except Exception as e:
            logger.error(f"Error during data validation: {str(e)}")
            validation_results['valid'] = False
            validation_results['errors'].append(f"Validation process error: {str(e)}")
        
        return validation_results
    
    @staticmethod
    def validate_numeric_ranges(value: float, field_name: str, min_val: float = 0, max_val: float = None) -> Tuple[bool, str]:
        """Validate numeric values are within acceptable ranges."""
        if value < min_val:
            return False, f"{field_name} cannot be less than {min_val}"
        
        if max_val is not None and value > max_val:
            return False, f"{field_name} cannot be greater than {max_val}"
        
        return True, ""


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
            response = self.table.get_item(
                Key={
                    'labor_type': labor_type,
                    'location': location
                }
            )
            
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
            response = self.table.get_item(
                Key={
                    'labor_type': labor_type,
                    'location': 'overtime_rules'
                }
            )
            
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
                })
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
        
        prompt = f"""
You are an expert invoice auditor analyzing discrepancies found in contractor invoices against MSA (Master Services Agreement) standards.

EXTRACTED DATA SUMMARY:
- Total Labor Entries: {len(extracted_data.get('normalized_data', {}).get('labor', []))}
- Total Materials Entries: {len(extracted_data.get('normalized_data', {}).get('materials', []))}

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
        """Detect anomalies in extracted data using SageMaker with proper error handling."""
        try:
            # Prepare data for anomaly detection
            features = self._extract_features(extracted_data)
            
            if not features:
                logger.info("No features extracted for anomaly detection")
                return []
            
            # Call SageMaker endpoint with retry logic
            max_retries = SAGEMAKER_MAX_RETRIES
            retry_delay = SAGEMAKER_RETRY_DELAY
            
            for attempt in range(max_retries):
                try:
                    response = sagemaker_client.invoke_endpoint(
                        EndpointName=self.endpoint_name,
                        ContentType='application/json',
                        Body=json.dumps({'instances': features})
                    )
                    
                    # Parse response
                    result = json.loads(response['Body'].read().decode())
                    anomalies = self._process_anomaly_results(result, extracted_data)
                    
                    logger.info(f"SageMaker anomaly detection completed successfully: {len(anomalies)} anomalies found")
                    return anomalies
                    
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    
                    if error_code == 'ModelError':
                        logger.error(f"SageMaker model error: {e}")
                        # Fallback immediately on model errors
                        return self._statistical_anomaly_detection(extracted_data)
                    
                    elif error_code == 'ValidationException':
                        logger.error(f"SageMaker validation error: {e}")
                        # Fallback immediately on validation errors
                        return self._statistical_anomaly_detection(extracted_data)
                    
                    elif error_code in ['ThrottlingException', 'ServiceUnavailableException']:
                        if attempt < max_retries - 1:
                            logger.warning(f"SageMaker throttling/unavailable, attempt {attempt + 1}, retrying in {retry_delay}s")
                            time.sleep(retry_delay)
                            retry_delay *= 2  # Exponential backoff
                            continue
                        else:
                            logger.error(f"SageMaker service unavailable after {max_retries} attempts")
                            return self._statistical_anomaly_detection(extracted_data)
                    
                    else:
                        logger.error(f"SageMaker client error: {e}")
                        if attempt == max_retries - 1:
                            return self._statistical_anomaly_detection(extracted_data)
                        time.sleep(retry_delay)
                        
                except Exception as e:
                    logger.error(f"Unexpected SageMaker error on attempt {attempt + 1}: {e}")
                    if attempt == max_retries - 1:
                        return self._statistical_anomaly_detection(extracted_data)
                    time.sleep(retry_delay)
            
            # If we get here, all retries failed
            logger.error("All SageMaker retry attempts failed, using statistical fallback")
            return self._statistical_anomaly_detection(extracted_data)
            
        except Exception as e:
            logger.error(f"Error in anomaly detection setup: {e}")
            # Fallback to statistical anomaly detection
            return self._statistical_anomaly_detection(extracted_data)
    
    def _extract_features(self, extracted_data: Dict) -> List[List[float]]:
        """Extract numerical features for anomaly detection."""
        features = []
        
        # Extract labor features
        labor_data = extracted_data.get('normalized_data', {}).get('labor', [])
        for labor in labor_data:
            feature_vector = [
                float(labor.get('unit_price', 0)),
                float(labor.get('total_hours', 0)),
                float(labor.get('total_cost', 0)),
                len(labor.get('name', '')),  # Name length as feature
                hash(labor.get('type', 'RS')) % 1000  # Labor type as numeric
            ]
            features.append(feature_vector)
        
        # Extract material features
        materials_data = extracted_data.get('normalized_data', {}).get('materials', [])
        for material in materials_data:
            feature_vector = [
                float(material.get('unit_price', 0)),
                float(material.get('quantity', 0)),
                float(material.get('total_cost', 0)),
                len(material.get('description', '')),  # Description length
                0  # Placeholder for material type
            ]
            features.append(feature_vector)
        
        return features
    
    def _process_anomaly_results(self, sagemaker_result: Dict, extracted_data: Dict) -> List[Dict[str, Any]]:
        """Process SageMaker anomaly detection results."""
        anomalies = []
        predictions = sagemaker_result.get('predictions', [])
        
        # Process labor anomalies
        labor_data = extracted_data.get('normalized_data', {}).get('labor', [])
        for i, (labor, prediction) in enumerate(zip(labor_data, predictions[:len(labor_data)])):
            if prediction.get('anomaly_score', 0) > ANOMALY_THRESHOLD:
                anomalies.append({
                    'type': 'labor_anomaly',
                    'category': 'statistical_outlier',
                    'item': labor.get('name', 'Unknown'),
                    'labor_type': labor.get('type', 'Unknown'),
                    'anomaly_score': prediction.get('anomaly_score'),
                    'value': labor.get('total_cost', 0),
                    'description': f"Labor cost ${labor.get('total_cost', 0):,.2f} is statistically anomalous",
                    'severity': 'high' if prediction.get('anomaly_score', 0) > 3.0 else 'medium'
                })
        
        # Process material anomalies
        materials_data = extracted_data.get('normalized_data', {}).get('materials', [])
        material_predictions = predictions[len(labor_data):]
        for material, prediction in zip(materials_data, material_predictions):
            if prediction.get('anomaly_score', 0) > ANOMALY_THRESHOLD:
                anomalies.append({
                    'type': 'material_anomaly',
                    'category': 'statistical_outlier',
                    'item': material.get('description', 'Unknown'),
                    'anomaly_score': prediction.get('anomaly_score'),
                    'value': material.get('total_cost', 0),
                    'description': f"Material cost ${material.get('total_cost', 0):,.2f} is statistically anomalous (e.g., $6,313 respirators)",
                    'severity': 'high' if prediction.get('anomaly_score', 0) > 3.0 else 'medium'
                })
        
        return anomalies
    
    def _statistical_anomaly_detection(self, extracted_data: Dict) -> List[Dict[str, Any]]:
        """Enhanced statistical anomaly detection with negative value handling."""
        anomalies = []
        
        try:
            # Analyze labor costs with validation
            labor_data = extracted_data.get('normalized_data', {}).get('labor', [])
            if labor_data and len(labor_data) > 1:  # Need at least 2 data points for statistics
                # Filter out negative and zero values, validate data
                valid_labor_costs = []
                valid_labor_entries = []
                
                for labor in labor_data:
                    try:
                        cost = float(labor.get('total_cost', 0))
                        if cost > 0 and not (np.isnan(cost) or np.isinf(cost)):
                            valid_labor_costs.append(cost)
                            valid_labor_entries.append(labor)
                        elif cost < 0:
                            logger.warning(f"Negative labor cost detected: ${cost} for {labor.get('name', 'Unknown')}")
                            # Flag negative values as anomalies
                            anomalies.append({
                                'type': 'labor_anomaly',
                                'category': 'negative_value',
                                'item': labor.get('name', 'Unknown'),
                                'value': cost,
                                'description': f"Negative labor cost: ${cost:,.2f}",
                                'severity': 'high',
                                'validation_error': True
                            })
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid labor cost for {labor.get('name', 'Unknown')}")
                        continue
                
                # Perform statistical analysis on valid data
                if len(valid_labor_costs) > 1:
                    mean_cost = np.mean(valid_labor_costs)
                    std_cost = np.std(valid_labor_costs)
                    
                    if std_cost > 0:
                        for labor, cost in zip(valid_labor_entries, valid_labor_costs):
                            z_score = abs((cost - mean_cost) / std_cost)
                            if z_score > ANOMALY_THRESHOLD:
                                anomalies.append({
                                    'type': 'labor_anomaly',
                                    'category': 'statistical_outlier',
                                    'item': labor.get('name', 'Unknown'),
                                    'labor_type': labor.get('type', 'Unknown'),
                                    'z_score': round(z_score, 2),
                                    'value': cost,
                                    'mean_value': round(mean_cost, 2),
                                    'std_deviation': round(std_cost, 2),
                                    'description': f"Labor cost ${cost:,.2f} is {z_score:.1f} standard deviations from mean (${mean_cost:,.2f})",
                                    'severity': 'high' if z_score > 3.0 else 'medium'
                                })
            
            # Analyze material costs with validation
            materials_data = extracted_data.get('normalized_data', {}).get('materials', [])
            if materials_data and len(materials_data) > 1:
                # Filter out negative and zero values, validate data
                valid_material_costs = []
                valid_material_entries = []
                
                for material in materials_data:
                    try:
                        cost = float(material.get('total_cost', 0))
                        if cost > 0 and not (np.isnan(cost) or np.isinf(cost)):
                            valid_material_costs.append(cost)
                            valid_material_entries.append(material)
                        elif cost < 0:
                            logger.warning(f"Negative material cost detected: ${cost} for {material.get('description', 'Unknown')}")
                            # Flag negative values as anomalies
                            anomalies.append({
                                'type': 'material_anomaly',
                                'category': 'negative_value',
                                'item': material.get('description', 'Unknown'),
                                'value': cost,
                                'description': f"Negative material cost: ${cost:,.2f}",
                                'severity': 'high',
                                'validation_error': True
                            })
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid material cost for {material.get('description', 'Unknown')}")
                        continue
                
                # Perform statistical analysis on valid data
                if len(valid_material_costs) > 1:
                    mean_cost = np.mean(valid_material_costs)
                    std_cost = np.std(valid_material_costs)
                    
                    if std_cost > 0:
                        for material, cost in zip(valid_material_entries, valid_material_costs):
                            z_score = abs((cost - mean_cost) / std_cost)
                            if z_score > ANOMALY_THRESHOLD:
                                anomalies.append({
                                    'type': 'material_anomaly',
                                    'category': 'statistical_outlier',
                                    'item': material.get('description', 'Unknown'),
                                    'z_score': round(z_score, 2),
                                    'value': cost,
                                    'mean_value': round(mean_cost, 2),
                                    'std_deviation': round(std_cost, 2),
                                    'description': f"Material cost ${cost:,.2f} is {z_score:.1f} standard deviations from mean (${mean_cost:,.2f}) - e.g., $6,313 respirators",
                                    'severity': 'high' if z_score > 3.0 else 'medium'
                                })
            
            logger.info(f"Statistical anomaly detection completed: {len(anomalies)} anomalies found")
            
        except Exception as e:
            logger.error(f"Error in statistical anomaly detection: {str(e)}")
