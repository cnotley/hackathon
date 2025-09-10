"""
Intelligent Invoice Data Extraction Lambda Handler

This module handles advanced document data extraction using Amazon Textract with Custom Queries,
Amazon Bedrock for semantic mapping/normalization, Amazon Comprehend for entity recognition,
and pandas for Excel files. It provides adaptive field mapping and vendor terminology normalization.
"""

import json
import logging
import os
import time
import uuid
import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Set
from io import BytesIO
import math

import boto3
import pandas as pd
from botocore.exceptions import ClientError

# Custom exceptions
class ExtractionFailure(Exception):
    """Custom exception for extraction failures."""
    pass

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Initialize AWS clients
s3_client = boto3.client('s3')
textract_client = boto3.client('textract')
stepfunctions_client = boto3.client('stepfunctions')
bedrock_client = boto3.client('bedrock-runtime')
comprehend_client = boto3.client('comprehend')

# Configuration
BUCKET_NAME = os.environ.get('BUCKET_NAME')
STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN')
BEDROCK_MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'anthropic.claude-3-5-sonnet-20241022-v2:0')
ASYNC_THRESHOLD_BYTES = 500 * 1024  # 500KB threshold for async processing
MAX_CHUNK_SIZE = 4000  # Maximum tokens per chunk for Bedrock
CONFIDENCE_THRESHOLD = 0.8  # Minimum confidence for OCR results (80%)
LOW_CONFIDENCE_THRESHOLD = 0.6  # Fallback threshold for Bedrock processing
MAX_MEMORY_SIZE = 512 * 1024 * 1024  # 512MB memory limit for text processing

# Field mapping and normalization dictionaries
FIELD_MAPPINGS = {
    # Rate/Price variations
    'rate': ['rate', 'unit_price', 'price', 'cost_per_unit', 'hourly_rate', 'daily_rate'],
    'unit_price': ['unit_price', 'rate', 'price', 'cost', 'unit_cost'],
    
    # Quantity variations
    'quantity': ['quantity', 'qty', 'amount', 'count', 'units', 'uom', 'hours', 'days'],
    'hours': ['hours', 'hrs', 'time', 'duration', 'daily_hours', 'total_hours'],
    
    # Material/Item variations
    'materials': ['materials', 'consumables', 'supplies', 'items', 'parts', 'components'],
    'description': ['description', 'item', 'service', 'work_description', 'task'],
    
    # Worker/Personnel variations
    'name': ['name', 'worker', 'employee', 'personnel', 'technician', 'contractor'],
    'type': ['type', 'classification', 'category', 'role', 'position', 'grade'],
    
    # Total variations
    'total': ['total', 'amount', 'sum', 'subtotal', 'line_total', 'extended_amount']
}

# Labor type classifications
LABOR_TYPES = {
    'RS': ['RS', 'Regular Skilled', 'Skilled Worker', 'Technician'],
    'US': ['US', 'Unskilled', 'General Labor', 'Helper'],
    'SS': ['SS', 'Semi-Skilled', 'Semi Skilled', 'Apprentice'],
    'SU': ['SU', 'Supervisor', 'Lead', 'Foreman', 'Team Lead'],
    'EN': ['EN', 'Engineer', 'Engineering', 'Professional']
}


class TextractProcessor:
    """Handles Amazon Textract document processing."""
    
    def __init__(self):
        self.textract_client = textract_client
        self.s3_client = s3_client
    
    def process_document(self, bucket: str, key: str, file_size: int) -> Dict[str, Any]:
        """Process document using appropriate Textract method based on file size."""
        try:
            if file_size > ASYNC_THRESHOLD_BYTES:
                return self._process_document_async(bucket, key)
            else:
                return self._process_document_sync(bucket, key)
        except Exception as e:
            logger.error(f"Error processing document {key}: {e}")
            raise
    
    def _process_document_sync(self, bucket: str, key: str) -> Dict[str, Any]:
        """Process document synchronously using AnalyzeDocument."""
        logger.info(f"Processing document synchronously: {key}")
        
        try:
            response = self.textract_client.analyze_document(
                Document={
                    'S3Object': {
                        'Bucket': bucket,
                        'Name': key
                    }
                },
                FeatureTypes=['TABLES', 'FORMS', 'LAYOUT']
            )
            
            return self._parse_textract_response(response, is_async=False)
            
        except ClientError as e:
            logger.error(f"Textract sync processing failed for {key}: {e}")
            raise
    
    def _process_document_async(self, bucket: str, key: str) -> Dict[str, Any]:
        """Process document asynchronously using StartDocumentAnalysis."""
        logger.info(f"Processing document asynchronously: {key}")
        
        try:
            # Start async job
            response = self.textract_client.start_document_analysis(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': bucket,
                        'Name': key
                    }
                },
                FeatureTypes=['TABLES', 'FORMS', 'LAYOUT'],
                JobTag=f"extraction-{uuid.uuid4().hex[:8]}"
            )
            
            job_id = response['JobId']
            logger.info(f"Started async Textract job: {job_id}")
            
            # Poll for completion
            return self._wait_for_async_job(job_id)
            
        except ClientError as e:
            logger.error(f"Textract async processing failed for {key}: {e}")
            raise
    
    def _wait_for_async_job(self, job_id: str, max_wait_time: int = 300) -> Dict[str, Any]:
        """Wait for async Textract job to complete with exponential backoff."""
        start_time = time.time()
        backoff_delay = 5  # Start with 5 seconds
        max_backoff = 60   # Maximum backoff delay
        
        while time.time() - start_time < max_wait_time:
            try:
                response = self.textract_client.get_document_analysis(JobId=job_id)
                status = response['JobStatus']
                
                if status == 'SUCCEEDED':
                    logger.info(f"Async job {job_id} completed successfully")
                    return self._parse_textract_response(response, is_async=True, job_id=job_id)
                elif status == 'FAILED':
                    error_msg = f"Textract job {job_id} failed: {response.get('StatusMessage', 'Unknown error')}"
                    logger.error(error_msg)
                    raise ExtractionFailure(error_msg)
                elif status in ['IN_PROGRESS']:
                    logger.info(f"Job {job_id} still in progress, waiting {backoff_delay}s...")
                    time.sleep(min(backoff_delay, max_backoff))
                    backoff_delay = min(backoff_delay * 1.5, max_backoff)  # Exponential backoff
                else:
                    logger.warning(f"Unexpected job status: {status}, waiting...")
                    time.sleep(min(backoff_delay, max_backoff))
                    
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ThrottlingException':
                    logger.warning(f"Throttling detected, backing off for {backoff_delay}s")
                    time.sleep(min(backoff_delay, max_backoff))
                    backoff_delay = min(backoff_delay * 2, max_backoff)
                else:
                    logger.error(f"Error checking job status {job_id}: {e}")
                    raise ExtractionFailure(f"Failed to check Textract job status: {str(e)}")
        
        raise TimeoutError(f"Async job {job_id} timed out after {max_wait_time} seconds")
    
    def _parse_textract_response(self, response: Dict[str, Any], is_async: bool = False, job_id: str = None) -> Dict[str, Any]:
        """Parse Textract response and extract structured data with enhanced multi-page table handling."""
        blocks = response.get('Blocks', [])
        
        # Collect all pages if async (may have NextToken)
        if is_async and response.get('NextToken'):
            all_blocks = blocks.copy()
            next_token = response.get('NextToken')
            
            while next_token:
                try:
                    next_response = self.textract_client.get_document_analysis(
                        JobId=job_id,
                        NextToken=next_token
                    )
                    all_blocks.extend(next_response.get('Blocks', []))
                    next_token = next_response.get('NextToken')
                except ClientError as e:
                    logger.error(f"Error getting next page for job {job_id}: {e}")
                    break
            
            blocks = all_blocks
        
        # Parse blocks into structured data
        parsed_data = {
            'text_blocks': [],
            'tables': [],
            'forms': [],
            'layout_elements': [],
            'confidence_scores': [],
            'page_count': 0,
            'multi_page_tables': [],  # Track tables spanning multiple pages
            'processing_metadata': {
                'job_id': job_id,
                'is_async': is_async,
                'total_blocks': len(blocks),
                'timestamp': datetime.utcnow().isoformat()
            }
        }
        
        # Group blocks by type and page
        pages = {}
        block_map = {}
        table_blocks_by_page = {}
        
        for block in blocks:
            block_id = block['Id']
            block_map[block_id] = block
            
            page_num = block.get('Page', 1)
            if page_num not in pages:
                pages[page_num] = {'blocks': [], 'tables': [], 'forms': []}
                table_blocks_by_page[page_num] = []
            
            pages[page_num]['blocks'].append(block)
            
            # Group table blocks by page for multi-page detection
            if block['BlockType'] == 'TABLE':
                table_blocks_by_page[page_num].append(block)
            
            # Track confidence scores
            if 'Confidence' in block:
                parsed_data['confidence_scores'].append({
                    'block_id': block_id,
                    'confidence': block['Confidence'],
                    'block_type': block['BlockType'],
                    'page': page_num
                })
        
        parsed_data['page_count'] = len(pages)
        
        # Detect and merge multi-page tables (labor pp.4-6 as mentioned in requirements)
        merged_tables = self._detect_and_merge_multi_page_tables(table_blocks_by_page, block_map)
        
        # Process each page
        for page_num, page_data in pages.items():
            page_blocks = page_data['blocks']
            
            # Extract text blocks with confidence filtering
            text_blocks = [b for b in page_blocks if b['BlockType'] == 'LINE']
            for text_block in text_blocks:
                confidence = text_block.get('Confidence', 0)
                text_content = text_block.get('Text', '')
                
                # Apply confidence threshold
                if confidence >= CONFIDENCE_THRESHOLD * 100:
                    parsed_data['text_blocks'].append({
                        'page': page_num,
                        'text': text_content,
                        'confidence': confidence,
                        'geometry': text_block.get('Geometry', {}),
                        'id': text_block['Id'],
                        'quality': 'high'
                    })
                elif confidence >= LOW_CONFIDENCE_THRESHOLD * 100:
                    # Low confidence - flag for manual review
                    parsed_data['text_blocks'].append({
                        'page': page_num,
                        'text': text_content,
                        'confidence': confidence,
                        'geometry': text_block.get('Geometry', {}),
                        'id': text_block['Id'],
                        'quality': 'low',
                        'requires_manual_review': True
                    })
                    logger.warning(f"Low confidence text detected: '{text_content}' (confidence: {confidence:.1f}%)")
            
            # Extract single-page tables (non-merged)
            table_blocks = [b for b in page_blocks if b['BlockType'] == 'TABLE']
            for table_block in table_blocks:
                # Check if this table is part of a merged multi-page table
                if not any(table_block['Id'] in merged_table.get('source_table_ids', []) for merged_table in merged_tables):
                    table_data = self._extract_table_data(table_block, block_map, page_num)
                    if table_data:
                        parsed_data['tables'].append(table_data)
            
            # Extract forms
            key_blocks = [b for b in page_blocks if b['BlockType'] == 'KEY_VALUE_SET' and b.get('EntityTypes') == ['KEY']]
            for key_block in key_blocks:
                form_data = self._extract_form_data(key_block, block_map, page_num)
                if form_data:
                    parsed_data['forms'].append(form_data)
        
        # Add merged multi-page tables
        parsed_data['tables'].extend(merged_tables)
        parsed_data['multi_page_tables'] = [t for t in merged_tables if t.get('spans_multiple_pages', False)]
        
        logger.info(f"Processed {len(parsed_data['tables'])} tables, {len(parsed_data['multi_page_tables'])} spanning multiple pages")
        
        return parsed_data
    
    def _detect_and_merge_multi_page_tables(self, table_blocks_by_page: Dict[int, List], block_map: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect and merge tables that span multiple pages (like labor pp.4-6)."""
        merged_tables = []
        processed_table_ids = set()
        
        try:
            pages = sorted(table_blocks_by_page.keys())
            
            for i, page_num in enumerate(pages):
                page_tables = table_blocks_by_page[page_num]
                
                for table_block in page_tables:
                    if table_block['Id'] in processed_table_ids:
                        continue
                    
                    # Extract table structure for analysis
                    table_data = self._extract_table_data(table_block, block_map, page_num)
                    if not table_data:
                        continue
                    
                    # Look for continuation tables on subsequent pages
                    continuation_tables = []
                    page_range = [page_num]
                    
                    for j in range(i + 1, len(pages)):
                        next_page = pages[j]
                        if next_page - page_num > 3:  # Don't look more than 3 pages ahead
                            break
                        
                        next_page_tables = table_blocks_by_page[next_page]
                        for next_table_block in next_page_tables:
                            if next_table_block['Id'] in processed_table_ids:
                                continue
                            
                            # Check if this could be a continuation table
                            next_table_data = self._extract_table_data(next_table_block, block_map, next_page)
                            if next_table_data and self._is_table_continuation(table_data, next_table_data):
                                continuation_tables.append({
                                    'table_block': next_table_block,
                                    'table_data': next_table_data,
                                    'page': next_page
                                })
                                page_range.append(next_page)
                                processed_table_ids.add(next_table_block['Id'])
                    
                    # Mark original table as processed
                    processed_table_ids.add(table_block['Id'])
                    
                    if continuation_tables:
                        # Merge tables spanning multiple pages
                        merged_table = self._merge_multi_page_table(table_data, continuation_tables, page_range)
                        merged_table['spans_multiple_pages'] = True
                        merged_table['page_range'] = page_range
                        merged_table['source_table_ids'] = [table_block['Id']] + [ct['table_block']['Id'] for ct in continuation_tables]
                        merged_tables.append(merged_table)
                        logger.info(f"Merged table spanning pages {page_range[0]}-{page_range[-1]} (labor data pattern detected)")
                    else:
                        # Single page table
                        table_data['spans_multiple_pages'] = False
                        merged_tables.append(table_data)
            
        except Exception as e:
            logger.error(f"Error detecting multi-page tables: {e}")
            # Return single-page tables as fallback
            for page_num, page_tables in table_blocks_by_page.items():
                for table_block in page_tables:
                    if table_block['Id'] not in processed_table_ids:
                        table_data = self._extract_table_data(table_block, block_map, page_num)
                        if table_data:
                            table_data['spans_multiple_pages'] = False
                            merged_tables.append(table_data)
        
        return merged_tables
    
    def _is_table_continuation(self, first_table: Dict[str, Any], second_table: Dict[str, Any]) -> bool:
        """Determine if second table is a continuation of the first table."""
        try:
            first_rows = first_table.get('rows', [])
            second_rows = second_table.get('rows', [])
            
            if not first_rows or not second_rows:
                return False
            
            # Check column count consistency
            first_col_count = len(first_rows[0]) if first_rows else 0
            second_col_count = len(second_rows[0]) if second_rows else 0
            
            if abs(first_col_count - second_col_count) > 1:  # Allow 1 column difference
                return False
            
            # Check for labor-related content patterns (mentioned in requirements)
            first_text = self._get_table_text_content(first_table)
            second_text = self._get_table_text_content(second_table)
            
            # Labor table indicators
            labor_keywords = ['name', 'worker', 'rate', 'hours', 'labor', 'skilled', 'supervisor', 'engineer', 'RS', 'US', 'SS', 'SU', 'EN']
            
            first_has_labor = any(keyword.lower() in first_text.lower() for keyword in labor_keywords)
            second_has_labor = any(keyword.lower() in second_text.lower() for keyword in labor_keywords)
            
            if first_has_labor and second_has_labor:
                return True
            
            # Check for similar column headers or structure
            if first_rows and second_rows:
                first_header = [cell.get('text', '').lower().strip() for cell in first_rows[0]]
                
                # Look for similar headers in first few rows of second table
                for row_idx in range(min(3, len(second_rows))):
                    second_row = [cell.get('text', '').lower().strip() for cell in second_rows[row_idx]]
                    
                    # Calculate header similarity
                    if len(first_header) == len(second_row):
                        matches = sum(1 for h1, h2 in zip(first_header, second_row) 
                                    if h1 and h2 and (h1 == h2 or h1 in h2 or h2 in h1))
                        if matches >= len(first_header) * 0.6:  # 60% similarity threshold
                            return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking table continuation: {e}")
            return False
    
    def _merge_multi_page_table(self, first_table: Dict[str, Any], continuation_tables: List[Dict[str, Any]], page_range: List[int]) -> Dict[str, Any]:
        """Merge multiple tables spanning pages with metadata tracking."""
        try:
            merged_table = first_table.copy()
            merged_table['page_breaks'] = []
            
            # Track which rows came from which pages
            row_page_mapping = []
            current_rows = first_table.get('rows', [])
            
            # Mark original rows with page info
            for i, row in enumerate(current_rows):
                row_page_mapping.append({
                    'row_index': i,
                    'page': first_table['page'],
                    'source_table': 'original'
                })
            
            # Add continuation table rows
            for continuation in continuation_tables:
                continuation_data = continuation['table_data']
                continuation_rows = continuation_data.get('rows', [])
                page = continuation['page']
                
                # Record page break
                merged_table['page_breaks'].append({
                    'from_page': merged_table['page'],
                    'to_page': page,
                    'break_after_row': len(current_rows) - 1,
                    'break_before_row': len(current_rows)
                })
                
                # Skip potential header rows in continuation tables
                start_row = 0
                if continuation_rows and len(continuation_rows) > 1:
                    # Check if first row looks like a header (similar to original header)
                    first_table_header = current_rows[0] if current_rows else []
                    continuation_first_row = continuation_rows[0]
                    
                    if self._rows_similar(first_table_header, continuation_first_row):
                        start_row = 1  # Skip the repeated header
                
                # Add rows with page tracking
                for i, row in enumerate(continuation_rows[start_row:], start_row):
                    # Enhanced row data with page information
                    enhanced_row = {
                        'cells': row,
                        'metadata': {
                            'page': page,
                            'original_row_index': i,
                            'merged_row_index': len(current_rows),
                            'spans_pages': False,
                            'source_table': continuation['table_block']['Id']
                        }
                    }
                    
                    current_rows.append(row)  # Keep backward compatibility
                    row_page_mapping.append({
                        'row_index': len(current_rows) - 1,
                        'page': page,
                        'source_table': continuation['table_block']['Id']
                    })
            
            merged_table['rows'] = current_rows
            merged_table['row_page_mapping'] = row_page_mapping
            merged_table['merged_from_pages'] = page_range
            merged_table['total_pages_spanned'] = len(page_range)
            
            logger.info(f"Successfully merged table with {len(current_rows)} total rows from {len(page_range)} pages")
            
            return merged_table
            
        except Exception as e:
            logger.error(f"Error merging multi-page table: {e}")
            return first_table  # Return original table as fallback
    
    def _get_table_text_content(self, table_data: Dict[str, Any]) -> str:
        """Extract all text content from a table for analysis."""
        text_parts = []
        for row in table_data.get('rows', []):
            for cell in row:
                text_parts.append(cell.get('text', ''))
        return ' '.join(text_parts)
    
    def _rows_similar(self, row1: List[Dict], row2: List[Dict]) -> bool:
        """Check if two table rows are similar (for header detection)."""
        if len(row1) != len(row2):
            return False
        
        matches = 0
        for cell1, cell2 in zip(row1, row2):
            text1 = cell1.get('text', '').lower().strip()
            text2 = cell2.get('text', '').lower().strip()
            if text1 and text2 and (text1 == text2 or text1 in text2 or text2 in text1):
                matches += 1
        
        return matches >= len(row1) * 0.7  # 70% similarity threshold
    
    def _extract_table_data(self, table_block: Dict[str, Any], block_map: Dict[str, Any], page_num: int) -> Optional[Dict[str, Any]]:
        """Extract table data from Textract table block."""
        try:
            table_data = {
                'page': page_num,
                'table_id': table_block['Id'],
                'confidence': table_block.get('Confidence', 0),
                'rows': [],
                'geometry': table_block.get('Geometry', {})
            }
            
            # Get table cells
            if 'Relationships' not in table_block:
                return None
            
            cell_blocks = []
            for relationship in table_block['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for cell_id in relationship['Ids']:
                        if cell_id in block_map:
                            cell_block = block_map[cell_id]
                            if cell_block['BlockType'] == 'CELL':
                                cell_blocks.append(cell_block)
            
            # Organize cells by row and column
            rows_dict = {}
            for cell in cell_blocks:
                row_index = cell.get('RowIndex', 1)
                col_index = cell.get('ColumnIndex', 1)
                
                if row_index not in rows_dict:
                    rows_dict[row_index] = {}
                
                # Get cell text
                cell_text = self._get_cell_text(cell, block_map)
                rows_dict[row_index][col_index] = {
                    'text': cell_text,
                    'confidence': cell.get('Confidence', 0),
                    'geometry': cell.get('Geometry', {})
                }
            
            # Convert to ordered list
            for row_index in sorted(rows_dict.keys()):
                row_data = []
                row_cells = rows_dict[row_index]
                for col_index in sorted(row_cells.keys()):
                    row_data.append(row_cells[col_index])
                table_data['rows'].append(row_data)
            
            return table_data
            
        except Exception as e:
            logger.error(f"Error extracting table data: {e}")
            return None
    
    def _extract_form_data(self, key_block: Dict[str, Any], block_map: Dict[str, Any], page_num: int) -> Optional[Dict[str, Any]]:
        """Extract form key-value pair data."""
        try:
            # Get key text
            key_text = self._get_block_text(key_block, block_map)
            
            # Find corresponding value block
            value_text = ""
            if 'Relationships' in key_block:
                for relationship in key_block['Relationships']:
                    if relationship['Type'] == 'VALUE':
                        for value_id in relationship['Ids']:
                            if value_id in block_map:
                                value_block = block_map[value_id]
                                value_text = self._get_block_text(value_block, block_map)
                                break
            
            return {
                'page': page_num,
                'key': key_text,
                'value': value_text,
                'key_confidence': key_block.get('Confidence', 0),
                'geometry': key_block.get('Geometry', {})
            }
            
        except Exception as e:
            logger.error(f"Error extracting form data: {e}")
            return None
    
    def _get_cell_text(self, cell_block: Dict[str, Any], block_map: Dict[str, Any]) -> str:
        """Get text content from a table cell."""
        return self._get_block_text(cell_block, block_map)
    
    def _get_block_text(self, block: Dict[str, Any], block_map: Dict[str, Any]) -> str:
        """Get text content from a block and its children."""
        text_parts = []
        
        if 'Relationships' in block:
            for relationship in block['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        if child_id in block_map:
                            child_block = block_map[child_id]
                            if child_block['BlockType'] == 'WORD':
                                text_parts.append(child_block.get('Text', ''))
        
        return ' '.join(text_parts)


class ExcelProcessor:
    """Handles Excel file processing using pandas."""
    
    def __init__(self):
        self.s3_client = s3_client
    
    def process_excel_file(self, bucket: str, key: str) -> Dict[str, Any]:
        """Process Excel file and extract structured data."""
        logger.info(f"Processing Excel file: {key}")
        
        try:
            # Download file from S3
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            file_content = response['Body'].read()
            
            # Read Excel file
            excel_data = pd.read_excel(BytesIO(file_content), sheet_name=None, engine='openpyxl')
            
            parsed_data = {
                'sheets': [],
                'summary': {
                    'total_sheets': len(excel_data),
                    'sheet_names': list(excel_data.keys())
                },
                'processing_metadata': {
                    'timestamp': datetime.utcnow().isoformat(),
                    'file_size': len(file_content)
                }
            }
            
            # Process each sheet
            for sheet_name, df in excel_data.items():
                sheet_data = self._process_sheet(sheet_name, df)
                parsed_data['sheets'].append(sheet_data)
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Error processing Excel file {key}: {e}")
            raise
    
    def _process_sheet(self, sheet_name: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Process individual Excel sheet."""
        try:
            # Convert DataFrame to structured format
            sheet_data = {
                'sheet_name': sheet_name,
                'dimensions': {
                    'rows': len(df),
                    'columns': len(df.columns)
                },
                'columns': df.columns.tolist(),
                'data': [],
                'summary_stats': {}
            }
            
            # Convert data to JSON-serializable format
            for index, row in df.iterrows():
                row_data = {}
                for col in df.columns:
                    value = row[col]
                    # Handle NaN and other non-serializable values
                    if pd.isna(value):
                        row_data[col] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        row_data[col] = value.isoformat()
                    else:
                        row_data[col] = str(value)
                
                sheet_data['data'].append(row_data)
            
            # Generate summary statistics for numeric columns
            numeric_columns = df.select_dtypes(include=['number']).columns
            for col in numeric_columns:
                try:
                    sheet_data['summary_stats'][col] = {
                        'mean': float(df[col].mean()) if not df[col].empty else 0,
                        'sum': float(df[col].sum()) if not df[col].empty else 0,
                        'min': float(df[col].min()) if not df[col].empty else 0,
                        'max': float(df[col].max()) if not df[col].empty else 0,
                        'count': int(df[col].count())
                    }
                except Exception as e:
                    logger.warning(f"Could not generate stats for column {col}: {e}")
            
            return sheet_data
            
        except Exception as e:
            logger.error(f"Error processing sheet {sheet_name}: {e}")
            raise


class BedrockProcessor:
    """Handles semantic mapping and normalization using Amazon Bedrock."""
    
    def __init__(self, model_id: str = BEDROCK_MODEL_ID):
        self.bedrock_client = bedrock_client
        self.model_id = model_id
    
    def normalize_extracted_data(self, extracted_data: Dict[str, Any], file_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize extracted data using Bedrock for semantic mapping."""
        try:
            logger.info("Starting Bedrock normalization process")
            
            # Process tables for labor data extraction
            normalized_labor = []
            normalized_materials = []
            
            if 'tables' in extracted_data:
                for table in extracted_data['tables']:
                    table_analysis = self._analyze_table_with_bedrock(table, file_metadata)
                    
                    if table_analysis.get('type') == 'labor':
                        normalized_labor.extend(table_analysis.get('data', []))
                    elif table_analysis.get('type') == 'materials':
                        normalized_materials.extend(table_analysis.get('data', []))
            
            # Process forms for additional metadata
            form_data = self._extract_form_metadata(extracted_data.get('forms', []))
            
            # Create normalized output structure
            normalized_result = {
                'labor': normalized_labor,
                'materials': normalized_materials,
                'metadata': form_data,
                'processing_info': {
                    'normalization_method': 'bedrock',
                    'model_used': self.model_id,
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            
            return normalized_result
            
        except Exception as e:
            logger.error(f"Error in Bedrock normalization: {e}")
            # Fallback to rule-based normalization
            return self._fallback_normalization(extracted_data)
    
    def _analyze_table_with_bedrock(self, table: Dict[str, Any], file_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze table structure and content using Bedrock."""
        try:
            # Convert table to text for analysis
            table_text = self._table_to_text(table)
            
            # Create prompt for table analysis
            prompt = self._create_table_analysis_prompt(table_text, file_metadata)
            
            # Call Bedrock
            response = self._call_bedrock(prompt)
            
            # Parse response
            analysis = self._parse_bedrock_table_response(response, table)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing table with Bedrock: {e}")
            return {'type': 'unknown', 'data': []}
    
    def _create_table_analysis_prompt(self, table_text: str, file_metadata: Dict[str, Any]) -> str:
        """Create prompt for Bedrock table analysis."""
        return f"""
Analyze the following table from an invoice/contract document and extract structured labor or materials data.

Document: {file_metadata.get('file_name', 'Unknown')}
Table Content:
{table_text}

Instructions:
1. Identify if this is a LABOR table or MATERIALS table
2. Extract and normalize the data with these standard fields:

For LABOR tables:
- name: Worker/employee name
- type: Labor classification (RS=Regular Skilled, US=Unskilled, SS=Semi-Skilled, SU=Supervisor, EN=Engineer)
- rate: Hourly/daily rate (normalize to decimal)
- hours: Total hours worked
- total: Total amount (rate × hours)

For MATERIALS tables:
- description: Item/material description
- quantity: Amount/count
- unit_price: Price per unit
- total: Total cost

Field Mapping Rules:
- "Rate" → "rate" or "unit_price"
- "Consumables" → "Materials"
- "UOM" → "quantity"
- "Daily Hours" → "hours"
- Various worker types → standardized codes (RS, US, SS, SU, EN)

Return JSON format:
{{
  "type": "labor" or "materials",
  "confidence": 0.0-1.0,
  "data": [
    {{
      "name": "Smith, John",
      "type": "RS",
      "rate": 100.00,
      "hours": 8.0,
      "total": 800.00
    }}
  ]
}}

Focus on accuracy and handle variations in terminology.
"""
    
    def _call_bedrock(self, prompt: str) -> str:
        """Call Bedrock with the given prompt and exponential backoff."""
        # Validate input size to prevent token limit overflow
        estimated_tokens = len(prompt) // 4  # Rough estimation
        if estimated_tokens > MAX_CHUNK_SIZE:
            logger.warning(f"Prompt too large ({estimated_tokens} tokens), truncating...")
            # Truncate prompt to fit within limits
            max_chars = MAX_CHUNK_SIZE * 4
            prompt = prompt[:max_chars] + "...[truncated]"
        
        max_retries = 3
        backoff_delay = 1
        
        for attempt in range(max_retries):
            try:
                body = {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 2000,
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                }
                
                response = self.bedrock_client.invoke_model(
                    modelId=self.model_id,
                    body=json.dumps(body)
                )
                
                response_body = json.loads(response['body'].read())
                return response_body['content'][0]['text']
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['ThrottlingException', 'ServiceQuotaExceededException']:
                    if attempt < max_retries - 1:
                        logger.warning(f"Bedrock throttling, attempt {attempt + 1}, backing off {backoff_delay}s")
                        time.sleep(backoff_delay)
                        backoff_delay *= 1.5  # Exponential backoff
                        continue
                logger.error(f"Error calling Bedrock: {e}")
                raise
            except Exception as e:
                logger.error(f"Error calling Bedrock: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(backoff_delay)
                backoff_delay *= 1.5
        
        raise Exception("Failed to call Bedrock after all retry attempts")
    
    def _parse_bedrock_table_response(self, response: str, original_table: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Bedrock response for table analysis."""
        try:
            # Try to extract JSON from response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
                
                # Add original table metadata
                result['original_table_id'] = original_table.get('table_id')
                result['page'] = original_table.get('page')
                result['confidence_score'] = original_table.get('confidence', 0)
                
                return result
            else:
                logger.warning("Could not parse JSON from Bedrock response")
                return {'type': 'unknown', 'data': []}
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in Bedrock response: {e}")
            return {'type': 'unknown', 'data': []}
    
    def _table_to_text(self, table: Dict[str, Any]) -> str:
        """Convert table structure to text for analysis with page span handling."""
        text_lines = []
        
        # Add table metadata
        if table.get('spans_multiple_pages'):
            page_range = table.get('page_range', [])
            text_lines.append(f"Multi-page table spanning pages {page_range[0]}-{page_range[-1]}:")
        else:
            text_lines.append(f"Table on page {table.get('page', 'unknown')}:")
        
        text_lines.append("")  # Empty line for readability
        
        for row_idx, row in enumerate(table.get('rows', [])):
            # Handle both old and new row formats
            if isinstance(row, dict) and 'cells' in row:
                # New format with metadata
                cells = row['cells']
                row_metadata = row.get('metadata', {})
                
                row_text = " | ".join([cell.get('text', '') for cell in cells])
                
                # Add page information for multi-page tables
                if row_metadata.get('spans_pages'):
                    pages_info = f" (pages: {row_metadata['pages']})"
                    text_lines.append(f"Row {row_idx + 1}: {row_text}{pages_info}")
                else:
                    text_lines.append(f"Row {row_idx + 1}: {row_text}")
            else:
                # Old format - backward compatibility
                row_text = " | ".join([cell.get('text', '') for cell in row])
                text_lines.append(f"Row {row_idx + 1}: {row_text}")
        
        # Add page break information for multi-page tables
        if table.get('spans_multiple_pages') and table.get('page_breaks'):
            text_lines.append("\nPage breaks detected:")
            for page_break in table['page_breaks']:
                text_lines.append(f"  Break between rows {page_break['break_after_row']} and {page_break['break_before_row']} (page {page_break['from_page']} → {page_break['to_page']})")
        
        return "\n".join(text_lines)
    
    def _extract_form_metadata(self, forms: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract metadata from form fields."""
        metadata = {}
        
        for form in forms:
            key = form.get('key', '').lower().strip()
            value = form.get('value', '').strip()
            
            # Map common form fields
            if 'invoice' in key and 'number' in key:
                metadata['invoice_number'] = value
            elif 'date' in key:
                metadata['invoice_date'] = value
            elif 'total' in key or 'amount' in key:
                metadata['total_amount'] = self._extract_currency_value(value)
            elif 'vendor' in key or 'contractor' in key:
                metadata['vendor_name'] = value
        
        return metadata
    
    def _extract_currency_value(self, text: str) -> Optional[float]:
        """Extract currency value from text."""
        try:
            # Remove currency symbols and commas
            cleaned = re.sub(r'[^\d.-]', '', text)
            return float(cleaned) if cleaned else None
        except ValueError:
            return None
    
    def _fallback_normalization(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Fallback rule-based normalization when Bedrock fails."""
        logger.info("Using fallback rule-based normalization")
        
        normalized_labor = []
        normalized_materials = []
        
        # Simple rule-based table processing
        for table in extracted_data.get('tables', []):
            if self._is_labor_table(table):
                labor_data = self._extract_labor_data_rules(table)
                normalized_labor.extend(labor_data)
            elif self._is_materials_table(table):
                materials_data = self._extract_materials_data_rules(table)
                normalized_materials.extend(materials_data)
        
        return {
            'labor': normalized_labor,
            'materials': normalized_materials,
            'metadata': {},
            'processing_info': {
                'normalization_method': 'fallback_rules',
                'timestamp': datetime.utcnow().isoformat()
            }
        }
    
    def _is_labor_table(self, table: Dict[str, Any]) -> bool:
        """Determine if table contains labor data using rules."""
        table_text = self._table_to_text(table).lower()
        labor_keywords = ['name', 'worker', 'employee', 'rate', 'hours', 'labor', 'personnel']
        return any(keyword in table_text for keyword in labor_keywords)
    
    def _is_materials_table(self, table: Dict[str, Any]) -> bool:
        """Determine if table contains materials data using rules."""
        table_text = self._table_to_text(table).lower()
        materials_keywords = ['material', 'consumable', 'supply', 'item', 'part', 'component', 'quantity']
        return any(keyword in table_text for keyword in materials_keywords)
    
    def _extract_labor_data_rules(self, table: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract labor data using rule-based approach."""
        labor_data = []
        rows = table.get('rows', [])
        
        if not rows:
            return labor_data
        
        # Assume first row is headers
        headers = [cell.get('text', '').lower().strip() for cell in rows[0]]
        
        # Map headers to standard fields
        field_map = {}
        for i, header in enumerate(headers):
            for standard_field, variations in FIELD_MAPPINGS.items():
                if any(var in header for var in variations):
                    field_map[i] = standard_field
                    break
        
        # Process data rows
        for row in rows[1:]:
            if len(row) <= len(headers):
                labor_entry = {}
                
                for i, cell in enumerate(row):
                    if i in field_map:
                        field_name = field_map[i]
                        cell_text = cell.get('text', '').strip()
                        
                        # Type conversion based on field
                        if field_name in ['rate', 'total']:
                            labor_entry[field_name] = self._extract_currency_value(cell_text)
                        elif field_name in ['hours', 'quantity']:
                            labor_entry[field_name] = self._extract_numeric_value(cell_text)
                        else:
                            labor_entry[field_name] = cell_text
                
                if labor_entry:
                    labor_data.append(labor_entry)
        
        return labor_data
    
    def _extract_materials_data_rules(self, table: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract materials data using rule-based approach."""
        materials: List[Dict[str, Any]] = []
        rows = table.get('rows', [])
        if not rows:
            return materials
        headers = [c.get('text','').lower() for c in rows[0]]
        idx = {i:h for i,h in enumerate(headers)}
        for row in rows[1:]:
            item: Dict[str, Any] = {}
            for i, cell in enumerate(row):
                h = idx.get(i,'')
                t = cell.get('text','').strip()
                if 'desc' in h or 'item' in h or 'material' in h:
                    item['description'] = t
                elif 'qty' in h or 'quantity' in h:
                    item['quantity'] = self._extract_numeric_value(t)
                elif 'unit' in h and ('price' in h or 'rate' in h or 'cost' in h):
                    item['unit_price'] = self._extract_currency_value(t)
                elif 'total' in h or 'amount' in h:
                    item['total'] = self._extract_currency_value(t)
            if item:
                materials.append(item)
        return materials
    
    def _extract_numeric_value(self, text: str) -> Optional[float]:
        """Extract numeric value from text."""
        try:
            # Remove non-numeric characters except decimal point
            cleaned = re.sub(r'[^\d.]', '', text)
            return float(cleaned) if cleaned else None
        except ValueError:
            return None


class ComprehendProcessor:
    """Handles entity recognition using Amazon Comprehend."""
    
    def __init__(self):
        self.comprehend_client = comprehend_client
    
    def extract_entities(self, text: str) -> Dict[str, Any]:
        """Extract entities from text using Comprehend."""
        try:
            # Built-in entity detection
            entities_response = self.comprehend_client.detect_entities(
                Text=text,
                LanguageCode='en'
            )
            
            # Custom entity detection for labor types
            custom_entities = self._detect_custom_entities(text)
            
            return {
                'standard_entities': entities_response.get('Entities', []),
                'custom_entities': custom_entities,
                'processing_metadata': {
                    'timestamp': datetime.utcnow().isoformat(),
                    'text_length': len(text)
                }
            }
            
        except Exception as e:
            logger.error(f"Error in Comprehend entity extraction: {e}")
            return {'standard_entities': [], 'custom_entities': []}
    
    def _detect_custom_entities(self, text: str) -> List[Dict[str, Any]]:
        """Detect custom entities like labor types."""
        custom_entities = []
        
        # Detect labor type codes
        for labor_code, variations in LABOR_TYPES.items():
            for variation in variations:
                pattern = r'\b' + re.escape(variation) + r'\b'
                matches = re.finditer(pattern, text, re.IGNORECASE)
                
                for match in matches:
                    custom_entities.append({
                        'Text': match.group(),
                        'Type': 'LABOR_TYPE',
                        'StandardCode': labor_code,
                        'BeginOffset': match.start(),
                        'EndOffset': match.end(),
                        'Score': 0.95  # High confidence for exact matches
                    })
        
        return custom_entities


class IntelligentExtractor:
    """Main class that orchestrates intelligent extraction with all processors."""
    
    def __init__(self):
        self.textract_processor = TextractProcessor()
        self.excel_processor = ExcelProcessor()
        self.bedrock_processor = BedrockProcessor()
        self.comprehend_processor = ComprehendProcessor()
        self.chunker = SemanticChunker()
    
    def process_document_intelligently(self, bucket: str, key: str, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Process document with full intelligent extraction pipeline."""
        try:
            file_size = file_info.get('size', 0)
            file_extension = file_info.get('extension', '').lower()
            
            logger.info(f"Starting intelligent extraction for {key}")
            
            # Step 1: Basic extraction
            if file_extension in ['.pdf', '.png', '.jpg', '.jpeg']:
                raw_data = self.textract_processor.process_document(bucket, key, file_size)
            elif file_extension in ['.xlsx', '.xls']:
                raw_data = self.excel_processor.process_excel_file(bucket, key)
            else:
                raise ValueError(f"Unsupported file type: {file_extension}")
            
            # Step 2: Entity recognition on text content
            all_text = self._extract_all_text(raw_data)
            entities = self.comprehend_processor.extract_entities(all_text)
            
            # Step 3: Semantic normalization with Bedrock
            file_metadata = {
                'file_name': os.path.basename(key),
                'file_path': key,
                'file_size': file_size,
                'file_type': file_extension
            }
            
            normalized_data = self.bedrock_processor.normalize_extracted_data(raw_data, file_metadata)
            
            # Step 4: Create semantic chunks
            chunks = self.chunker.chunk_extracted_data(raw_data, file_metadata)
            
            # Step 5: Compile final result
            result = {
                'extraction_status': 'completed',
                'file_info': file_info,
                'raw_extracted_data': raw_data,
                'normalized_data': normalized_data,
                'entities': entities,
                'semantic_chunks': chunks,
                'processing_summary': {
                    'total_chunks': len(chunks),
                    'processing_method': 'async' if file_size > ASYNC_THRESHOLD_BYTES else 'sync',
                    'confidence_threshold': CONFIDENCE_THRESHOLD,
                    'normalization_method': normalized_data.get('processing_info', {}).get('normalization_method', 'unknown'),
                    'entities_found': len(entities.get('standard_entities', [])) + len(entities.get('custom_entities', [])),
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error in intelligent extraction: {e}")
            raise
    
    def _extract_all_text(self, extracted_data: Dict[str, Any]) -> str:
        """Extract all text content for entity recognition with memory limits."""
        all_text = []
        current_size = 0
        
        # Text blocks
        for block in extracted_data.get('text_blocks', []):
            text = block.get('text', '')
            text_size = len(text.encode('utf-8'))
            
            if current_size + text_size > MAX_MEMORY_SIZE:
                logger.warning(f"Text extraction stopped at {current_size:,} bytes to prevent memory overflow")
                break
                
            all_text.append(text)
            current_size += text_size
        
        # Table text (if memory allows)
        if current_size < MAX_MEMORY_SIZE * 0.8:  # Leave 20% buffer
            for table in extracted_data.get('tables', []):
                for row in table.get('rows', []):
                    for cell in row:
                        text = cell.get('text', '')
                        text_size = len(text.encode('utf-8'))
                        
                        if current_size + text_size > MAX_MEMORY_SIZE:
                            logger.warning("Stopping table text extraction due to memory limits")
                            break
                            
                        all_text.append(text)
                        current_size += text_size
        
        # Form text (if memory allows)
        if current_size < MAX_MEMORY_SIZE * 0.9:  # Leave 10% buffer
            for form in extracted_data.get('forms', []):
                text = f"{form.get('key', '')}: {form.get('value', '')}"
                text_size = len(text.encode('utf-8'))
                
                if current_size + text_size > MAX_MEMORY_SIZE:
                    logger.warning("Stopping form text extraction due to memory limits")
                    break
                    
                all_text.append(text)
                current_size += text_size
        
        result_text = ' '.join(all_text)
        logger.info(f"Extracted {len(result_text):,} characters ({current_size:,} bytes) of text")
        return result_text


class SemanticChunker:
    """Handles semantic chunking of extracted data for Bedrock compatibility."""
    
    def __init__(self, max_chunk_size: int = MAX_CHUNK_SIZE):
        self.max_chunk_size = max_chunk_size
    
    def chunk_extracted_data(self, extracted_data: Dict[str, Any], file_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Chunk extracted data into semantic chunks with metadata."""
        chunks = []
        
        try:
            # Chunk text blocks by page
            if 'text_blocks' in extracted_data:
                text_chunks = self._chunk_text_blocks(extracted_data['text_blocks'], file_metadata)
                chunks.extend(text_chunks)
            
            # Chunk tables separately
            if 'tables' in extracted_data:
                table_chunks = self._chunk_tables(extracted_data['tables'], file_metadata)
                chunks.extend(table_chunks)
            
            # Chunk forms
            if 'forms' in extracted_data:
                form_chunks = self._chunk_forms(extracted_data['forms'], file_metadata)
                chunks.extend(form_chunks)
            
            # Chunk Excel sheets
            if 'sheets' in extracted_data:
                excel_chunks = self._chunk_excel_sheets(extracted_data['sheets'], file_metadata)
                chunks.extend(excel_chunks)
            
            # Add chunk metadata
            for i, chunk in enumerate(chunks):
                chunk['chunk_metadata'] = {
                    'chunk_id': f"{file_metadata.get('file_name', 'unknown')}_{i+1}",
                    'chunk_index': i + 1,
                    'total_chunks': len(chunks),
                    'estimated_tokens': self._estimate_tokens(chunk['content']),
                    'created_at': datetime.utcnow().isoformat()
                }
            
            return chunks
            
        except Exception as e:
            logger.error(f"Error chunking data: {e}")
            raise
    
    def _chunk_text_blocks(self, text_blocks: List[Dict[str, Any]], file_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Chunk text blocks by page and size."""
        chunks = []
        current_chunk = {
            'type': 'text',
            'content': '',
            'metadata': {
                'source_file': file_metadata.get('file_name', ''),
                'pages': [],
                'confidence_scores': [],
                'overlap_ratio': 0.2
            }
        }
        
        current_page = None
        
        for block in text_blocks:
            page_num = block['page']
            text = block['text']
            confidence = block['confidence']
            
            # Start new chunk if page changes or size limit reached
            if (current_page is not None and page_num != current_page) or \
               self._estimate_tokens(current_chunk['content'] + text) > self.max_chunk_size:
                
                if current_chunk['content'].strip():
                    chunks.append(current_chunk.copy())
                
                current_chunk = {
                    'type': 'text',
                    'content': '',
                    'metadata': {
                        'source_file': file_metadata.get('file_name', ''),
                        'pages': [],
                        'confidence_scores': [],
                        'overlap_ratio': 0.2
                    }
                }
            
            current_chunk['content'] += text + '\n'
            if page_num not in current_chunk['metadata']['pages']:
                current_chunk['metadata']['pages'].append(page_num)
            current_chunk['metadata']['confidence_scores'].append(confidence)
            current_page = page_num
        
        # Add final chunk
        if current_chunk['content'].strip():
            chunks.append(current_chunk)

        # Apply explicit 20% overlap by duplicating tail/head portions between adjacent chunks
        overlapped_chunks = []
        for i, ch in enumerate(chunks):
            if i == 0:
                overlapped_chunks.append(ch)
                continue
            prev = overlapped_chunks[-1]
            # compute overlap window in characters
            overlap_len = int(len(prev['content']) * 0.2)
            if overlap_len > 0:
                prefix = prev['content'][-overlap_len:]
                ch['content'] = prefix + ch['content']
                ch['metadata']['overlap_from_previous'] = overlap_len
            overlapped_chunks.append(ch)
        return overlapped_chunks
        
        return chunks
    
    def _chunk_tables(self, tables: List[Dict[str, Any]], file_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Chunk tables individually."""
        chunks = []
        
        for i, table in enumerate(tables):
            # Convert table to text representation
            table_text = f"Table {i+1} (Page {table['page']}):\n"
            
            for row_idx, row in enumerate(table['rows']):
                row_text = " | ".join([cell['text'] for cell in row])
                table_text += f"Row {row_idx + 1}: {row_text}\n"
            
            chunk = {
                'type': 'table',
                'content': table_text,
                'metadata': {
                    'source_file': file_metadata.get('file_name', ''),
                    'page': table['page'],
                    'table_index': i + 1,
                    'table_id': table['table_id'],
                    'confidence': table['confidence'],
                    'dimensions': {
                        'rows': len(table['rows']),
                        'columns': len(table['rows'][0]) if table['rows'] else 0
                    }
                }
            }
            
            chunks.append(chunk)
        
        return chunks
    
    def _chunk_forms(self, forms: List[Dict[str, Any]], file_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Chunk form data by page."""
        chunks = []
        page_forms = {}
        
        # Group forms by page
        for form in forms:
            page = form['page']
            if page not in page_forms:
                page_forms[page] = []
            page_forms[page].append(form)
        
        # Create chunks per page
        for page, page_form_list in page_forms.items():
            form_text = f"Form Data (Page {page}):\n"
            
            for form in page_form_list:
                form_text += f"{form['key']}: {form['value']}\n"
            
            chunk = {
                'type': 'form',
                'content': form_text,
                'metadata': {
                    'source_file': file_metadata.get('file_name', ''),
                    'page': page,
                    'form_count': len(page_form_list)
                }
            }
            
            chunks.append(chunk)
        
        return chunks
    
    def _chunk_excel_sheets(self, sheets: List[Dict[str, Any]], file_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Chunk Excel sheets individually."""
        chunks = []
        
        for sheet in sheets:
            # Convert sheet to text representation
            sheet_text = f"Excel Sheet: {sheet['sheet_name']}\n"
            sheet_text += f"Dimensions: {sheet['dimensions']['rows']} rows x {sheet['dimensions']['columns']} columns\n\n"
            
            # Add column headers
            if sheet['columns']:
                sheet_text += "Columns: " + " | ".join(sheet['columns']) + "\n\n"
            
            # Add data (limit to prevent oversized chunks)
            max_rows = min(100, len(sheet['data']))  # Limit to 100 rows per chunk
            for i, row in enumerate(sheet['data'][:max_rows]):
                row_values = [str(row.get(col, '')) for col in sheet['columns']]
                sheet_text += f"Row {i+1}: " + " | ".join(row_values) + "\n"
            
            if len(sheet['data']) > max_rows:
                sheet_text += f"\n... ({len(sheet['data']) - max_rows} more rows)\n"
            
            # Add summary statistics
            if sheet['summary_stats']:
                sheet_text += "\nSummary Statistics:\n"
                for col, stats in sheet['summary_stats'].items():
                    sheet_text += f"{col}: Sum={stats['sum']}, Mean={stats['mean']:.2f}, Count={stats['count']}\n"
            
            chunk = {
                'type': 'excel',
                'content': sheet_text,
                'metadata': {
                    'source_file': file_metadata.get('file_name', ''),
                    'sheet_name': sheet['sheet_name'],
                    'dimensions': sheet['dimensions'],
                    'summary_stats': sheet['summary_stats']
                }
            }
            
            chunks.append(chunk)
        
        return chunks
    
    def _estimate_tokens(self, text: str) -> int:
        """Estimate token count for text (rough approximation)."""
        # Rough estimation: 1 token ≈ 4 characters
        return len(text) // 4


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for data extraction.
    
    Handles Step Functions task executions for document data extraction.
    """
    logger.info(f"Received extraction event: {json.dumps(event, default=str)}")
    
    try:
        # Extract task information
        task = event.get('task', 'extract')
        input_data = event.get('input', {})
        
        if task == 'extract':
            return handle_extraction_task(input_data)
        else:
            raise ValueError(f"Unknown task: {task}")
            
    except Exception as e:
        logger.error(f"Error processing extraction event: {e}")
        raise


def handle_extraction_task(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Handle intelligent document data extraction task."""
    try:
        # Extract file information
        file_info = input_data.get('file_info', {})
        bucket = input_data.get('bucket')
        key = file_info.get('key')
        
        logger.info(f"Starting intelligent extraction for {key}")
        
        # Use intelligent extractor for full pipeline
        intelligent_extractor = IntelligentExtractor()
        result = intelligent_extractor.process_document_intelligently(bucket, key, file_info)
        
        # Store results in S3 for large responses
        result_size = len(json.dumps(result, default=str))
        if result_size > 200000:  # 200KB limit for Step Functions
            result_key = f"extraction-results/{os.path.splitext(key)[0]}_extraction_result.json"
            
            s3_client.put_object(
                Bucket=bucket,
                Key=result_key,
                Body=json.dumps(result, default=str),
                ContentType='application/json'
            )
            
            logger.info(f"Large result ({result_size} bytes) stored in S3: {result_key}")
            
            # Return reference to S3 object with summary
            return {
                'extraction_status': 'completed',
                'result_location': {
                    'bucket': bucket,
                    'key': result_key
                },
                'processing_summary': result['processing_summary'],
                'normalized_data_preview': {
                    'labor_count': len(result.get('normalized_data', {}).get('labor', [])),
                    'materials_count': len(result.get('normalized_data', {}).get('materials', [])),
                    'total_labor_amount': sum([
                        item.get('total', 0) or 0 
                        for item in result.get('normalized_data', {}).get('labor', [])
                        if isinstance(item.get('total'), (int, float))
                    ])
                }
            }
        
        return result
        
    except Exception as e:
        logger.error(f"Error in intelligent extraction task: {e}")
        # Fallback to basic extraction if intelligent extraction fails
        try:
            logger.info("Falling back to basic extraction")
            return handle_basic_extraction_fallback(input_data)
        except Exception as fallback_error:
            logger.error(f"Fallback extraction also failed: {fallback_error}")
            raise e  # Raise original error


def handle_basic_extraction_fallback(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Fallback to basic extraction when intelligent extraction fails."""
    file_info = input_data.get('file_info', {})
    bucket = input_data.get('bucket')
    key = file_info.get('key')
    file_size = file_info.get('size', 0)
    file_extension = file_info.get('extension', '').lower()
    
    logger.info(f"Basic extraction fallback for {key}")
    
    # Process based on file type
    if file_extension in ['.pdf', '.png', '.jpg', '.jpeg']:
        processor = TextractProcessor()
        extracted_data = processor.process_document(bucket, key, file_size)
    elif file_extension in ['.xlsx', '.xls']:
        processor = ExcelProcessor()
        extracted_data = processor.process_excel_file(bucket, key)
    else:
        raise ValueError(f"Unsupported file type for extraction: {file_extension}")
    
    # Create semantic chunks
    chunker = SemanticChunker()
    file_metadata = {
        'file_name': os.path.basename(key),
        'file_path': key,
        'file_size': file_size,
        'file_type': file_extension
    }
    
    chunks = chunker.chunk_extracted_data(extracted_data, file_metadata)
    
    return {
        'extraction_status': 'completed_fallback',
        'file_info': file_info,
        'extracted_data': extracted_data,
        'semantic_chunks': chunks,
        'processing_summary': {
            'total_chunks': len(chunks),
            'processing_method': 'async' if file_size > ASYNC_THRESHOLD_BYTES else 'sync',
            'confidence_threshold': CONFIDENCE_THRESHOLD,
            'normalization_method': 'fallback_basic',
            'timestamp': datetime.utcnow().isoformat()
        }
    }
