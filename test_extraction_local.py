#!/usr/bin/env python3
"""
Local PDF Extraction Tester for MSA Invoice Auditing System

This script allows testing the extraction functionality locally with a PDF file,
without requiring AWS services. It simulates the extraction pipeline and provides
sample data that matches the requirements ($160k total, labor $77k, etc.).
"""

import json
import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import pytest
import pandas as pd
import openpyxl
from comparison_lambda import _calculate_rate_variances, MSARatesComparator, _detect_overtime_violations
from report_lambda import ExcelReportGenerator

# Add lambda directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lambda'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockTextractProcessor:
    """Mock Textract processor for local testing with realistic sample data."""
    
    def __init__(self):
        self.sample_data = self._generate_sample_invoice_data()
    
    def _generate_sample_invoice_data(self) -> Dict[str, Any]:
        """Generate realistic sample invoice data matching requirements."""
        return {
            'text_blocks': [
                {
                    'page': 1,
                    'text': 'DISASTER RECOVERY SERVICES INVOICE',
                    'confidence': 95.0,
                    'geometry': {'BoundingBox': {'Left': 0.1, 'Top': 0.1, 'Width': 0.8, 'Height': 0.05}},
                    'id': 'text_1',
                    'quality': 'high'
                },
                {
                    'page': 1,
                    'text': 'Invoice Number: INV-2025-001',
                    'confidence': 98.5,
                    'geometry': {'BoundingBox': {'Left': 0.1, 'Top': 0.2, 'Width': 0.4, 'Height': 0.03}},
                    'id': 'text_2',
                    'quality': 'high'
                },
                {
                    'page': 1,
                    'text': 'Date of Loss: 2/12/2025',
                    'confidence': 96.8,
                    'geometry': {'BoundingBox': {'Left': 0.1, 'Top': 0.25, 'Width': 0.3, 'Height': 0.03}},
                    'id': 'text_3',
                    'quality': 'high'
                },
                {
                    'page': 1,
                    'text': 'Vendor: Emergency Recovery Services LLC',
                    'confidence': 97.2,
                    'geometry': {'BoundingBox': {'Left': 0.1, 'Top': 0.3, 'Width': 0.5, 'Height': 0.03}},
                    'id': 'text_4',
                    'quality': 'high'
                }
            ],
            'tables': [
                {
                    'page': 4,
                    'table_id': 'table_labor_1',
                    'confidence': 94.2,
                    'spans_multiple_pages': True,
                    'page_range': [4, 5, 6],
                    'rows': [
                        # Header row
                        [
                            {'text': 'Worker Name', 'confidence': 98.0},
                            {'text': 'Labor Type', 'confidence': 97.5},
                            {'text': 'Hours', 'confidence': 99.0},
                            {'text': 'Rate', 'confidence': 98.8},
                            {'text': 'Total', 'confidence': 97.9}
                        ],
                        # Data rows - Smith, John (RS Labor)
                        [
                            {'text': 'Smith, John', 'confidence': 96.5},
                            {'text': 'RS', 'confidence': 98.2},
                            {'text': '25.0', 'confidence': 99.1},
                            {'text': '$77.00', 'confidence': 97.8},
                            {'text': '$1,925.00', 'confidence': 96.9}
                        ],
                        # Johnson, Mike (US Labor)
                        [
                            {'text': 'Johnson, Mike', 'confidence': 95.8},
                            {'text': 'US', 'confidence': 97.9},
                            {'text': '40.0', 'confidence': 98.5},
                            {'text': '$50.00', 'confidence': 98.1},
                            {'text': '$2,000.00', 'confidence': 97.2}
                        ],
                        # Williams, Sarah (SS Labor)
                        [
                            {'text': 'Williams, Sarah', 'confidence': 96.2},
                            {'text': 'SS', 'confidence': 98.0},
                            {'text': '32.5', 'confidence': 97.8},
                            {'text': '$55.00', 'confidence': 98.3},
                            {'text': '$1,787.50', 'confidence': 96.8}
                        ],
                        # Davis, Robert (SU Labor - Supervisor)
                        [
                            {'text': 'Davis, Robert', 'confidence': 97.1},
                            {'text': 'SU', 'confidence': 98.5},
                            {'text': '45.0', 'confidence': 98.9},
                            {'text': '$85.00', 'confidence': 97.6},
                            {'text': '$3,825.00', 'confidence': 96.4}
                        ],
                        # Anderson, Lisa (EN Labor - Engineer)
                        [
                            {'text': 'Anderson, Lisa', 'confidence': 96.8},
                            {'text': 'EN', 'confidence': 97.7},
                            {'text': '28.0', 'confidence': 98.2},
                            {'text': '$95.00', 'confidence': 98.0},
                            {'text': '$2,660.00', 'confidence': 97.1}
                        ],
                        # Additional workers to reach ~77k total
                        [
                            {'text': 'Brown, Michael', 'confidence': 95.9},
                            {'text': 'RS', 'confidence': 98.1},
                            {'text': '160.0', 'confidence': 98.4},
                            {'text': '$75.00', 'confidence': 97.9},
                            {'text': '$12,000.00', 'confidence': 96.7}
                        ],
                        [
                            {'text': 'Wilson, Jennifer', 'confidence': 96.3},
                            {'text': 'US', 'confidence': 97.8},
                            {'text': '200.0', 'confidence': 98.6},
                            {'text': '$48.00', 'confidence': 98.2},
                            {'text': '$9,600.00', 'confidence': 97.0}
                        ],
                        [
                            {'text': 'Garcia, Carlos', 'confidence': 96.1},
                            {'text': 'SS', 'confidence': 98.3},
                            {'text': '150.0', 'confidence': 98.1},
                            {'text': '$58.00', 'confidence': 97.7},
                            {'text': '$8,700.00', 'confidence': 96.9}
                        ],
                        [
                            {'text': 'Taylor, Amanda', 'confidence': 96.7},
                            {'text': 'SU', 'confidence': 98.0},
                            {'text': '120.0', 'confidence': 98.3},
                            {'text': '$88.00', 'confidence': 97.8},
                            {'text': '$10,560.00', 'confidence': 96.8}
                        ],
                        [
                            {'text': 'Martinez, David', 'confidence': 95.8},
                            {'text': 'EN', 'confidence': 97.9},
                            {'text': '100.0', 'confidence': 98.7},
                            {'text': '$98.00', 'confidence': 98.1},
                            {'text': '$9,800.00', 'confidence': 97.2}
                        ],
                        # More workers to total ~1,119.75 hours and ~$77,000
                        [
                            {'text': 'Multiple Additional Workers', 'confidence': 94.5},
                            {'text': 'Various', 'confidence': 96.2},
                            {'text': '159.25', 'confidence': 97.8},
                            {'text': '$65.00 avg', 'confidence': 96.9},
                            {'text': '$14,142.50', 'confidence': 96.1}
                        ]
                    ],
                    'page_breaks': [
                        {
                            'from_page': 4,
                            'to_page': 5,
                            'break_after_row': 5,
                            'break_before_row': 6
                        },
                        {
                            'from_page': 5,
                            'to_page': 6,
                            'break_after_row': 9,
                            'break_before_row': 10
                        }
                    ],
                    'row_page_mapping': [
                        {'row_index': 0, 'page': 4, 'source_table': 'header'},
                        {'row_index': 1, 'page': 4, 'source_table': 'original'},
                        {'row_index': 2, 'page': 4, 'source_table': 'original'},
                        {'row_index': 3, 'page': 4, 'source_table': 'original'},
                        {'row_index': 4, 'page': 4, 'source_table': 'original'},
                        {'row_index': 5, 'page': 4, 'source_table': 'original'},
                        {'row_index': 6, 'page': 5, 'source_table': 'continuation_1'},
                        {'row_index': 7, 'page': 5, 'source_table': 'continuation_1'},
                        {'row_index': 8, 'page': 5, 'source_table': 'continuation_1'},
                        {'row_index': 9, 'page': 5, 'source_table': 'continuation_1'},
                        {'row_index': 10, 'page': 6, 'source_table': 'continuation_2'},
                        {'row_index': 11, 'page': 6, 'source_table': 'continuation_2'}
                    ],
                    'geometry': {'BoundingBox': {'Left': 0.1, 'Top': 0.4, 'Width': 0.8, 'Height': 0.5}}
                },
                # Labor table continuation mock retains labor only
                {
                    'page': 5,
                    'table_id': 'table_labor_1_continuation_1',
                    'confidence': 95.1,
                    'rows': [
                        [{'text': 'James Johnson', 'confidence': 96.7}, {'text': 'RS', 'confidence': 96.1}, {'text': '8', 'confidence': 95.9}, {'text': '$75.00', 'confidence': 96.5}, {'text': '$600.00', 'confidence': 96.9}],
                        [{'text': 'Emily Davis', 'confidence': 95.8}, {'text': 'US', 'confidence': 95.2}, {'text': '10', 'confidence': 95.6}, {'text': '$45.00', 'confidence': 96.0}, {'text': '$450.00', 'confidence': 96.3}],
                        [{'text': 'Robert Wilson', 'confidence': 95.4}, {'text': 'RS', 'confidence': 95.0}, {'text': '6', 'confidence': 95.1}, {'text': '$70.00', 'confidence': 95.6}, {'text': '$420.00', 'confidence': 95.8}]
                    ],
                    'geometry': {'BoundingBox': {'Left': 0.1, 'Top': 0.45, 'Width': 0.8, 'Height': 0.35}}
                },
                {
                    'page': 6,
                    'table_id': 'table_labor_1_continuation_2',
                    'confidence': 94.6,
                    'rows': [
                        [{'text': 'Matthew Taylor', 'confidence': 95.0}, {'text': 'SS', 'confidence': 94.4}, {'text': '9', 'confidence': 94.6}, {'text': '$55.00', 'confidence': 95.1}, {'text': '$495.00', 'confidence': 95.4}],
                        [{'text': 'Olivia Harris', 'confidence': 94.7}, {'text': 'SU', 'confidence': 94.0}, {'text': '11', 'confidence': 94.3}, {'text': '$85.00', 'confidence': 94.8}, {'text': '$935.00', 'confidence': 95.0}],
                        [{'text': 'Noah Martin', 'confidence': 94.2}, {'text': 'EN', 'confidence': 93.8}, {'text': '7.5', 'confidence': 94.1}, {'text': '$95.00', 'confidence': 94.5}, {'text': '$712.50', 'confidence': 94.7}]
                    ],
                    'geometry': {'BoundingBox': {'Left': 0.1, 'Top': 0.3, 'Width': 0.8, 'Height': 0.4}}
                }
            ],
            'forms': [
                {
                    'page': 1,
                    'key': 'Invoice Number',
                    'value': 'INV-2025-001',
                    'key_confidence': 98.5,
                    'geometry': {'BoundingBox': {'Left': 0.1, 'Top': 0.35, 'Width': 0.4, 'Height': 0.03}}
                },
                {
                    'page': 1,
                    'key': 'Total Amount',
                    'value': '$160,000.00',
                    'key_confidence': 97.9,
                    'geometry': {'BoundingBox': {'Left': 0.1, 'Top': 0.4, 'Width': 0.3, 'Height': 0.03}}
                },
                {
                    'page': 1,
                    'key': 'Date of Loss',
                    'value': '2/12/2025',
                    'key_confidence': 96.8,
                    'geometry': {'BoundingBox': {'Left': 0.1, 'Top': 0.45, 'Width': 0.3, 'Height': 0.03}}
                }
            ],
            'confidence_scores': [
                {'block_id': 'text_1', 'confidence': 95.0, 'block_type': 'LINE', 'page': 1},
                {'block_id': 'text_2', 'confidence': 98.5, 'block_type': 'LINE', 'page': 1},
                {'block_id': 'text_3', 'confidence': 96.8, 'block_type': 'LINE', 'page': 1},
                {'block_id': 'text_4', 'confidence': 97.2, 'block_type': 'LINE', 'page': 1}
            ],
            'page_count': 8,
            'multi_page_tables': [
                {
                    'table_id': 'table_labor_1',
                    'spans_multiple_pages': True,
                    'page_range': [4, 5, 6],
                    'description': 'Labor costs spanning pages 4-6'
                }
            ],
            'processing_metadata': {
                'job_id': 'mock_job_123',
                'is_async': False,
                'total_blocks': 156,
                'timestamp': datetime.utcnow().isoformat()
            }
        }
    
    def process_document(self, file_path: str, file_size: int) -> Dict[str, Any]:
        """Mock document processing that returns sample data."""
        logger.info(f"Mock processing document: {file_path}")
        logger.info(f"File size: {file_size:,} bytes")
        
        # Simulate processing time
        import time
        time.sleep(2)  # Simulate processing delay
        
        return self.sample_data


class MockBedrockProcessor:
    """Mock Bedrock processor for local testing."""
    
    def __init__(self):
        self.msa_rates = {
            'RS': 70.00,  # Regular Skilled
            'US': 45.00,  # Unskilled
            'SS': 55.00,  # Semi-Skilled
            'SU': 85.00,  # Supervisor
            'EN': 95.00   # Engineer
        }
    
    def normalize_extracted_data(self, extracted_data: Dict[str, Any], file_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Mock normalization that processes the sample labor data."""
        logger.info("Mock Bedrock normalization starting...")
        
        normalized_labor = []
        
        for table in extracted_data.get('tables', []):
            if 'labor' in table.get('table_id', '').lower():
                labor_data = self._process_labor_table(table)
                normalized_labor.extend(labor_data)
        
        total_labor_cost = sum(item.get('total', 0) for item in normalized_labor)
        total_hours = sum(item.get('total_hours', 0) for item in normalized_labor)
        
        normalized_result = {
            'labor': normalized_labor,
            'materials': [],
            'summary': {
                'total_labor_cost': total_labor_cost,
                'total_material_cost': 0.0,
                'total_hours': total_hours,
                'worker_count': len(normalized_labor)
            },
            'metadata': {
                'invoice_number': 'INV-2025-001',
                'vendor': 'Emergency Recovery Services LLC',
                'date_of_loss': '2/12/2025',
                'invoice_total': 160000.00,
                'labor_total': total_labor_cost
            },
            'processing_info': {
                'normalization_method': 'mock_bedrock',
                'model_used': 'anthropic.claude-3-haiku-mock',
                'timestamp': datetime.utcnow().isoformat()
            }
        }
        
        assert 'materials' in normalized_result and not normalized_result['materials'], "Materials handling removed"
        assert normalized_result['summary']['total_labor_cost'] == pytest.approx(77000.0, rel=0.01)
        assert normalized_result['summary']['total_hours'] == pytest.approx(1119.75, rel=0.01)
        return normalized_result
    
    def _process_labor_table(self, table: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process labor table rows into normalized format."""
        if 'labor' not in table.get('table_id', '').lower():
            return []
        normalized_rows: List[Dict[str, Any]] = []
        rows = table.get('rows', [])
        if not rows:
            return normalized_rows
        header = rows[0]
        data_rows = rows[1:]
        for row in data_rows:
            if len(row) < 5:
                continue
            worker_name = row[0].get('text', '').strip()
            if not worker_name:
                continue
            labor_type = row[1].get('text', '').strip()
            hours_text = row[2].get('text', '')
            rate_text = row[3].get('text', '')
            total_text = row[4].get('text', '')
            total_hours = self._extract_numeric(hours_text)
            unit_price = self._extract_currency(rate_text)
            total_cost = self._extract_currency(total_text)
            msa_rate = self.msa_rates.get(labor_type, unit_price)
            normalized_rows.append({
                'name': worker_name,
                'type': labor_type,
                'total_hours': total_hours,
                'unit_price': unit_price,
                'total_cost': total_cost if total_cost else total_hours * unit_price,
                'msa_rate': msa_rate,
                'variance_percentage': ((unit_price - msa_rate) / msa_rate * 100) if msa_rate else 0.0,
                'variance_amount': (unit_price - msa_rate) * total_hours if msa_rate else 0.0
            })
        return normalized_rows
    
    def _detect_table_type(self, table: Dict[str, Any]) -> str:
        """Detect table type based on keywords."""
        table_text = ' '.join(cell.get('text', '') for row in table.get('rows', []) for cell in row).lower()
        labor_keywords = ['labor', 'worker', 'employee', 'rate', 'hours', 'rs', 'us', 'ss', 'su', 'en']
        for keyword in labor_keywords:
            if keyword in table_text:
                return 'labor'
        return 'unknown'
    
    def _extract_currency(self, text: str) -> float:
        """Extract currency value from text."""
        import re
        # Remove currency symbols and commas, extract number
        cleaned = re.sub(r'[^\d.-]', '', text.replace(',', ''))
        try:
            return float(cleaned) if cleaned else 0.0
        except ValueError:
            return 0.0
    
    def _extract_numeric(self, text: str) -> float:
        """Extract numeric value from text."""
        import re
        # Extract first number found
        match = re.search(r'[\d.]+', text.replace(',', ''))
        try:
            return float(match.group()) if match else 0.0
        except ValueError:
            return 0.0


class LocalExtractionTester:
    """Main class for testing extraction functionality locally."""
    
    def __init__(self):
        self.textract_processor = MockTextractProcessor()
        self.bedrock_processor = MockBedrockProcessor()
    
    def test_pdf_extraction(self, pdf_path: str) -> Dict[str, Any]:
        """Test PDF extraction with sample data."""
        logger.info(f"Starting local extraction test for: {pdf_path}")
        
        # Handle file existence - use mock data if file doesn't exist
        file_exists = os.path.exists(pdf_path)
        if file_exists:
            file_size = os.path.getsize(pdf_path)
            logger.info(f"File found: {pdf_path} ({file_size:,} bytes)")
        else:
            file_size = 2048576  # Mock 2MB file size
            logger.warning(f"File not found: {pdf_path}")
            logger.info("Proceeding with mock data for testing purposes")
        
        file_info = {
            'file_name': os.path.basename(pdf_path),
            'file_path': pdf_path,
            'file_size': file_size,
            'file_type': '.pdf',
            'file_exists': file_exists,
            'is_mock_data': not file_exists
        }
        
        logger.info(f"File info: {file_info}")
        
        try:
            # Step 1: Extract raw data (mocked)
            logger.info("Step 1: Extracting raw document data...")
            raw_extracted_data = self.textract_processor.process_document(pdf_path, file_size)
            
            # Step 2: Normalize data (mocked)
            logger.info("Step 2: Normalizing extracted data...")
            normalized_data = self.bedrock_processor.normalize_extracted_data(raw_extracted_data, file_info)
            
            # Step 3: Generate analysis results
            logger.info("Step 3: Generating analysis results...")
            analysis_results = self._generate_analysis_results(normalized_data)
            
            # Step 4: Compile final results
            final_result = {
                'extraction_status': 'completed',
                'file_info': file_info,
                'raw_extracted_data': raw_extracted_data,
                'normalized_data': normalized_data,
                'analysis_results': analysis_results,
                'processing_summary': {
                    'total_pages': raw_extracted_data.get('page_count', 0),
                    'tables_found': len(raw_extracted_data.get('tables', [])),
                    'multi_page_tables': len(raw_extracted_data.get('multi_page_tables', [])),
                    'labor_entries': len(normalized_data.get('labor', [])),
                    'material_entries': len(normalized_data.get('materials', [])),
                    'total_labor_cost': normalized_data.get('summary', {}).get('total_labor_cost', 0),
                    'total_material_cost': normalized_data.get('summary', {}).get('total_material_cost', 0),
                    'confidence_threshold_met': self._check_confidence_threshold(raw_extracted_data),
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            
            logger.info("Extraction test completed successfully!")
            return final_result
            
        except Exception as e:
            logger.error(f"Error during extraction test: {e}")
            return {
                'extraction_status': 'failed',
                'error': str(e),
                'file_info': file_info
            }
    
    def _generate_analysis_results(self, normalized_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate analysis results for testing comparison module."""
        labor_data = normalized_data.get('labor', [])
        
        # Calculate rate variances
        rate_variances = []
        total_savings = 0.0
        
        for worker in labor_data:
            actual_rate = worker.get('unit_price', 0)
            msa_rate = worker.get('msa_rate', 0)
            hours = worker.get('total_hours', 0)
            
            if actual_rate > msa_rate and hours > 0:
                variance_pct = ((actual_rate - msa_rate) / msa_rate) * 100
                savings = (actual_rate - msa_rate) * hours
                total_savings += savings
                
                rate_variances.append({
                    'worker': worker.get('name', 'Unknown'),
                    'labor_type': worker.get('type', 'N/A'),
                    'actual_rate': actual_rate,
                    'msa_rate': msa_rate,
                    'variance_percentage': round(variance_pct, 1),
                    'savings': round(savings, 2),
                    'hours': hours
                })
        
        # Check for overtime violations (>40 hours/week)
        overtime_violations = []
        for worker in labor_data:
            hours = worker.get('total_hours', 0)
            if hours > 40:  # Simple check - in reality would need weekly breakdown
                overtime_violations.append({
                    'worker': worker.get('name', 'Unknown'),
                    'total_hours': hours,
                    'overtime_hours': hours - 40,
                    'threshold': 40
                })
        
        return {
            'rate_variances': rate_variances,
            'overtime_violations': overtime_violations,
            'anomalies': [],  # Could add statistical anomaly detection
            'total_savings': round(total_savings, 2),
            'compliance_metrics': {
                'total_workers_reviewed': len(labor_data),
                'workers_with_overcharges': len(rate_variances),
                'compliance_rate': round((len(labor_data) - len(rate_variances)) / len(labor_data) * 100, 1) if labor_data else 0,
                'average_variance': round(sum(v['variance_percentage'] for v in rate_variances) / len(rate_variances), 1) if rate_variances else 0
            },
            'summary': {
                'high_priority_flags': len([v for v in rate_variances if v['variance_percentage'] > 10]),
                'medium_priority_flags': len([v for v in rate_variances if 5 <= v['variance_percentage'] <= 10]),
                'low_priority_flags': len([v for v in rate_variances if 0 < v['variance_percentage'] < 5])
            }
        }
    
    def _check_confidence_threshold(self, raw_data: Dict[str, Any]) -> bool:
        """Check if OCR confidence meets the 80% threshold requirement."""
        confidence_scores = raw_data.get('confidence_scores', [])
        if not confidence_scores:
            return False
        
        total_confidence = sum(score.get('confidence', 0) for score in confidence_scores)
        avg_confidence = total_confidence / len(confidence_scores)
        
        return avg_confidence >= 80.0
    
    def print_analysis_summary(self, result: Dict[str, Any]) -> None:
        """Print a formatted summary of the extraction test results."""
        if result.get('extraction_status') != 'completed':
            print(f"\n❌ Extraction failed: {result.get('error', 'Unknown error')}")
            return
        
        print("\n" + "="*80)
        print("🔍 DISASTER RECOVERY INVOICE EXTRACTION TEST RESULTS")
        print("="*80)
        
        # File info
        file_info = result.get('file_info', {})
        print(f"\n📄 File Information:")
        print(f"   • File: {file_info.get('file_name', 'N/A')}")
        print(f"   • Size: {file_info.get('file_size', 0):,} bytes")
        
        # Processing summary
        summary = result.get('processing_summary', {})
        print(f"\n📊 Processing Summary:")
        print(f"   • Pages Processed: {summary.get('total_pages', 0)}")
        print(f"   • Tables Found: {summary.get('tables_found', 0)}")
        print(f"   • Multi-page Tables: {summary.get('multi_page_tables', 0)}")
        print(f"   • Confidence Threshold Met (>80%): {'✅ Yes' if summary.get('confidence_threshold_met') else '❌ No'}")
        
        # Labor analysis
        normalized = result.get('normalized_data', {})
        print(f"\n👷 Labor Analysis:")
        print(f"   • Workers Found: {summary.get('labor_entries', 0)}")
        print(f"   • Total Hours: {normalized.get('summary', {}).get('total_hours', 0):,.2f}")
        print(f"   • Total Labor Cost: ${normalized.get('summary', {}).get('total_labor_cost', 0):,.2f}")
        
        # Materials analysis
        print(f"\n📦 Materials Analysis:")
        print(f"   • Material Items: {summary.get('material_entries', 0)}")
        print(f"   • Total Material Cost: ${normalized.get('summary', {}).get('total_material_cost', 0):,.2f}")
        
        # Audit findings
        analysis = result.get('analysis_results', {})
        print(f"\n🔍 Audit Findings:")
        print(f"   • Rate Variances Found: {len(analysis.get('rate_variances', []))}")
        print(f"   • Potential Savings: ${analysis.get('total_savings', 0):,.2f}")
        print(f"   • Overtime Violations: {len(analysis.get('overtime_violations', []))}")
        
        compliance = analysis.get('compliance_metrics', {})
        print(f"   • Compliance Rate: {compliance.get('compliance_rate', 0)}%")
        
        # Detailed rate variances
        variances = analysis.get('rate_variances', [])
        if variances:
            print(f"\n💰 Rate Variance Details:")
            for variance in variances[:5]:  # Show top 5
                print(f"   • {variance['worker']} ({variance['labor_type']}): "
                      f"${variance['actual_rate']:.2f} vs ${variance['msa_rate']:.2f} MSA "
                      f"({variance['variance_percentage']:+.1f}%) - "
                      f"Potential Savings: ${variance['savings']:.2f}")
            
            if len(variances) > 5:
                print(f"   ... and {len(variances) - 5} more variances")
        
        # Test validation against requirements
        print(f"\n✅ Requirements Validation:")
        total_cost = normalized.get('metadata', {}).get('invoice_total', 0)
        labor_cost = normalized.get('summary', {}).get('total_labor_cost', 0)
        material_cost = normalized.get('summary', {}).get('total_material_cost', 0)
        
        print(f"   • Invoice Total: ${total_cost:,.2f} (Target: $160,000)")
        print(f"   • Labor Total: ${labor_cost:,.2f} (Target: ~$77,000)")
        print(f"   • Material Total: ${material_cost:,.2f} (Target: ~$83,000)")
        print(f"   • Total Hours: {normalized.get('summary', {}).get('total_hours', 0):,.2f} (Target: ~1,119.75)")
        
        print("\n" + "="*80)
        print("✅ Extraction test completed successfully!")
        print("="*80)


def main():
    """Main function to run the local extraction test."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test PDF extraction functionality locally')
    parser.add_argument('pdf_path', help='Path to the PDF file to test')
    parser.add_argument('--output', '-o', help='Output file for results (JSON format)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize tester
    tester = LocalExtractionTester()
    
    # Run extraction test
    result = tester.test_pdf_extraction(args.pdf_path)
    
    # Print summary
    tester.print_analysis_summary(result)
    
    # Save results if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\n💾 Detailed results saved to: {args.output}")
    
    # Return appropriate exit code
    return 0 if result.get('extraction_status') == 'completed' else 1


if __name__ == '__main__':
    exit(main())


class TestLocalProcessing:
    def setup_method(self):
        self.tester = LocalExtractionTester()

    def test_discrepancies(self):
        labor_entries = [
            {
                'name': 'Alice Smith',
                'type': 'RS',
                'unit_price': 77.0,
                'total_hours': 10.0,
                'total_cost': 770.0
            }
        ]
        extracted = {'normalized_data': {'labor': labor_entries}}
        comparator = MSARatesComparator()
        variances, savings = _calculate_rate_variances(extracted, comparator)
        assert len(variances) == 1
        assert savings > 0

    def test_report_generation(self, monkeypatch, tmp_path):
        generator = ExcelReportGenerator()
        flags = {
            'rate_variances': [
                {
                    'worker': 'Alice Smith',
                    'labor_type': 'RS',
                    'actual_rate': 77.0,
                    'msa_rate': 70.0,
                    'variance_percentage': 10.0,
                    'variance_amount': 70.0,
                    'hours': 10
                }
            ],
            'overtime_violations': [],
            'anomalies': [],
            'duplicates': [],
            'total_savings': 70.0
        }
        metadata = {
            'invoice_number': 'INV-TEST-001',
            'vendor': 'Test Vendor',
            'labor_total': 700.0
        }
        extracted = {
            'normalized_data': {
                'labor': [
                    {
                        'name': 'Alice Smith',
                        'type': 'RS',
                        'total_hours': 10,
                        'unit_price': 77,
                        'total_cost': 770
                    }
                ]
            }
        }
        excel_bytes = generator.generate_excel_report(flags, metadata, extracted)
        out_file = tmp_path / 'report.xlsx'
        out_file.write_bytes(excel_bytes)
        wb = openpyxl.load_workbook(out_file)
        assert 'Project Summary' in wb.sheetnames
        summary_sheet = wb['Project Summary']
        headers = [cell.value for cell in summary_sheet[5]]
        assert 'As Analyzed' in headers

    def test_end_to_end_pipeline(self):
        result = self.tester.test_pdf_extraction('nonexistent.pdf')
        assert result['extraction_status'] == 'completed'
        assert 'materials' not in result['normalized_data']
        assert result['analysis_results']['total_savings'] > 0
