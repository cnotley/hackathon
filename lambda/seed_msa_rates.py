"""
MSA Rates Seeding Lambda Function

This module seeds the MSA rates DynamoDB table with standard labor rates
and overtime rules as referenced in the requirements.
"""

import json
import logging
import os
import boto3
from typing import Dict, Any, List
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError
import time

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.getenv('LOG_LEVEL', 'INFO'))

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

# Environment variables
MSA_RATES_TABLE = os.getenv('MSA_RATES_TABLE', 'msa-rates')

def seed_msa_rates() -> Dict[str, Any]:
    """Seed MSA rates table with standard rates like RS:70 as mentioned in requirements."""
    try:
        table = dynamodb.Table(MSA_RATES_TABLE)

        # Ensure table exists with retries
        for attempt in range(5):
            try:
                table.load()
                break
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    logger.info(f"DynamoDB table {MSA_RATES_TABLE} not found (attempt {attempt + 1}/5); retrying in 5s...")
                    time.sleep(5)
                else:
                    raise
        else:
            raise ValueError(f"DynamoDB table {MSA_RATES_TABLE} not found after retries. Deploy infrastructure first.")
        
        # Get current effective date
        effective_date = datetime.utcnow().strftime('%Y-%m-%d')
        
        # Standard MSA rates as mentioned in requirements (RS:70, etc.)
        msa_rates_data = [
            # Regular rates for different labor types
            {
                'rate_id': 'RS_default',
                'effective_date': effective_date,
                'labor_type': 'RS',
                'location': 'default',
                'standard_rate': Decimal('70.00'),
                'description': 'Regular Skilled Labor',
                'category': 'labor',
                'subcategory': 'skilled',
                'created_by': 'seeding_script',
                'created_at': datetime.utcnow().isoformat()
            },
            {
                'rate_id': 'US_default', 
                'effective_date': effective_date,
                'labor_type': 'US',
                'location': 'default',
                'standard_rate': Decimal('45.00'),
                'description': 'Unskilled Labor',
                'category': 'labor',
                'subcategory': 'unskilled',
                'created_by': 'seeding_script',
                'created_at': datetime.utcnow().isoformat()
            },
            {
                'rate_id': 'SS_default',
                'effective_date': effective_date,
                'labor_type': 'SS',
                'location': 'default', 
                'standard_rate': Decimal('55.00'),
                'description': 'Semi-Skilled Labor',
                'category': 'labor',
                'subcategory': 'semi_skilled',
                'created_by': 'seeding_script',
                'created_at': datetime.utcnow().isoformat()
            },
            {
                'rate_id': 'SU_default',
                'effective_date': effective_date,
                'labor_type': 'SU',
                'location': 'default',
                'standard_rate': Decimal('85.00'),
                'description': 'Supervisor',
                'category': 'labor',
                'subcategory': 'supervision',
                'created_by': 'seeding_script',
                'created_at': datetime.utcnow().isoformat()
            },
            {
                'rate_id': 'EN_default',
                'effective_date': effective_date,
                'labor_type': 'EN',
                'location': 'default',
                'standard_rate': Decimal('95.00'),
                'description': 'Engineer/Professional',
                'category': 'labor', 
                'subcategory': 'professional',
                'created_by': 'seeding_script',
                'created_at': datetime.utcnow().isoformat()
            },
            
            # Overtime rules
            {
                'rate_id': 'overtime_default',
                'effective_date': effective_date,
                'labor_type': 'default',
                'location': 'overtime_rules',
                'standard_rate': Decimal('40.0'),  # This represents the threshold hours
                'weekly_threshold': Decimal('40.0'),
                'overtime_multiplier': Decimal('1.5'),
                'description': 'Standard overtime threshold and multiplier',
                'category': 'overtime',
                'subcategory': 'rules',
                'created_by': 'seeding_script',
                'created_at': datetime.utcnow().isoformat()
            },
            
            # Regional variations (example for high-cost areas)
            {
                'rate_id': 'RS_high_cost',
                'effective_date': effective_date,
                'labor_type': 'RS',
                'location': 'high_cost',
                'standard_rate': Decimal('85.00'),
                'description': 'Regular Skilled Labor - High Cost Area',
                'category': 'labor',
                'subcategory': 'skilled',
                'region': 'high_cost_metropolitan',
                'created_by': 'seeding_script',
                'created_at': datetime.utcnow().isoformat()
            },
            {
                'rate_id': 'SU_high_cost',
                'effective_date': effective_date,
                'labor_type': 'SU',
                'location': 'high_cost',
                'standard_rate': Decimal('105.00'),
                'description': 'Supervisor - High Cost Area',
                'category': 'labor',
                'subcategory': 'supervision',
                'region': 'high_cost_metropolitan',
                'created_by': 'seeding_script',
                'created_at': datetime.utcnow().isoformat()
            },
            
            # Equipment allowance rates (3% mentioned in requirements)
            {
                'rate_id': 'equipment_allowance',
                'effective_date': effective_date,
                'labor_type': 'equipment',
                'location': 'allowance',
                'standard_rate': Decimal('0.03'),  # 3% as mentioned
                'description': 'Equipment Allowance Rate',
                'category': 'allowance',
                'subcategory': 'equipment',
                'percentage': True,
                'created_by': 'seeding_script',
                'created_at': datetime.utcnow().isoformat()
            },
            
            # Emergency/Holiday rates
            {
                'rate_id': 'RS_emergency',
                'effective_date': effective_date,
                'labor_type': 'RS',
                'location': 'emergency',
                'standard_rate': Decimal('105.00'),  # 1.5x base rate
                'description': 'Regular Skilled Labor - Emergency/Holiday',
                'category': 'labor',
                'subcategory': 'skilled',
                'multiplier': Decimal('1.5'),
                'created_by': 'seeding_script',
                'created_at': datetime.utcnow().isoformat()
            }
            ,
            # Management-to-labor ratio heuristic (SU:RS <= 1:6)
            {
                'rate_id': 'ratio_rules_su_rs',
                'effective_date': effective_date,
                'labor_type': 'SU',
                'location': 'ratio_rules',
                'max_ratio': Decimal('6.0'),
                'description': 'Maximum supervisor-to-RS ratio (1:6)',
                'category': 'policy',
                'subcategory': 'ratio',
                'created_by': 'seeding_script',
                'created_at': datetime.utcnow().isoformat()
            }
        ]
        
        # Insert rates into DynamoDB
        seeded_count = 0
        errors = []
        
        with table.batch_writer() as batch:
            for rate_data in msa_rates_data:
                try:
                    batch.put_item(Item=rate_data)
                    seeded_count += 1
                    logger.info(f"Seeded rate: {rate_data['rate_id']} - {rate_data['description']}")
                except Exception as e:
                    error_msg = f"Failed to seed {rate_data['rate_id']}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
        
        # Add sample test data matching the requirements ($160k total, labor $77k/1,119.75 hrs)
        sample_invoice_data = [
            {
                'rate_id': 'sample_invoice_totals',
                'effective_date': effective_date,
                'labor_type': 'sample',
                'location': 'test_data',
                'total_invoice': Decimal('160000.00'),
                'total_labor': Decimal('77000.00'),
                'total_hours': Decimal('1119.75'),
                'description': 'Sample labor-focused invoice totals for testing',
                'category': 'test_data',
                'subcategory': 'sample_invoice',
                'created_by': 'seeding_script',
                'created_at': datetime.utcnow().isoformat()
            }
        ]
        
        # Insert sample data
        for sample_data in sample_invoice_data:
            try:
                table.put_item(Item=sample_data)
                seeded_count += 1
                logger.info(f"Seeded sample data: {sample_data['rate_id']}")
            except Exception as e:
                error_msg = f"Failed to seed sample data {sample_data['rate_id']}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
        
        # Return results
        result = {
            'seeding_status': 'completed',
            'seeded_count': seeded_count,
            'total_records': len(msa_rates_data) + len(sample_invoice_data),
            'errors': errors,
            'timestamp': datetime.utcnow().isoformat(),
            'table_name': MSA_RATES_TABLE
        }
        
        if errors:
            result['seeding_status'] = 'completed_with_errors'
        
        logger.info(f"MSA rates seeding completed: {seeded_count} records seeded")
        return result
        
    except Exception as e:
        error_msg = f"Error during MSA rates seeding: {str(e)}"
        logger.error(error_msg)
        return {
            'seeding_status': 'failed',
            'error': error_msg,
            'timestamp': datetime.utcnow().isoformat()
        }

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for MSA rates seeding.
    
    Can be invoked:
    1. During deployment (automated seeding)
    2. Manually for re-seeding
    3. As part of infrastructure setup
    """
    try:
        logger.info("Starting MSA rates seeding process...")
        
        # Check if table exists and is accessible
        try:
            table = dynamodb.Table(MSA_RATES_TABLE)
            table.load()
            logger.info(f"DynamoDB table {MSA_RATES_TABLE} is accessible")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'error': f"DynamoDB table {MSA_RATES_TABLE} not found",
                        'suggestion': 'Deploy the infrastructure first to create the table'
                    })
                }
            else:
                raise
        
        # Perform seeding
        result = seed_msa_rates()
        
        # Determine HTTP status code
        if result['seeding_status'] == 'completed':
            status_code = 200
        elif result['seeding_status'] == 'completed_with_errors':
            status_code = 207  # Multi-status
        else:
            status_code = 500
        
        return {
            'statusCode': status_code,
            'body': json.dumps(result, default=str),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        
    except Exception as e:
        logger.error(f"Error in MSA seeding Lambda handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'seeding_status': 'failed',
                'timestamp': datetime.utcnow().isoformat()
            })
        }

if __name__ == '__main__':
    # Test locally
    test_result = seed_msa_rates()
    print(json.dumps(test_result, indent=2, default=str))
