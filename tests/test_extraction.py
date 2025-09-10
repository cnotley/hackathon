"""
Tests for the data extraction Lambda function.

This module contains unit tests for the Textract and Excel processing
functionality, including mock PDF input tests and JSON structure validation.
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from moto import mock_s3, mock_textract
import boto3
from io import BytesIO
import pandas as pd

# Import the extraction Lambda function
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lambda'))

from extraction_lambda import (
    TextractProcessor,
    ExcelProcessor,
    SemanticChunker,
    BedrockProcessor,
    ComprehendProcessor,
    IntelligentExtractor,
    lambda_handler,
    handle_extraction_task
)


class TestTextractProcessor:
    """Test cases for TextractProcessor class."""
    
    @mock_textract
    @mock_s3
    def test_process_document_sync_small_file(self):
        """Test synchronous document processing for small files."""
        # Setup
        processor = TextractProcessor()
        bucket = "test-bucket"
        key = "test-document.pdf"
        file_size = 100 * 1024  # 100KB - below async threshold
        
        # Create mock S3 bucket and object
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=b"mock pdf content")
        
        # Mock Textract response
        mock_response = {
            'Blocks': [
                {
                    'Id': 'block-1',
                    'BlockType': 'LINE',
                    'Text': 'Invoice Total: $76,160.00',
                    'Confidence': 95.5,
                    'Page': 1,
                    'Geometry': {'BoundingBox': {'Left': 0.1, 'Top': 0.1, 'Width': 0.3, 'Height': 0.05}}
                },
                {
                    'Id': 'block-2',
                    'BlockType': 'TABLE',
                    'Confidence': 92.0,
                    'Page': 1,
                    'Relationships': [
                        {
                            'Type': 'CHILD',
                            'Ids': ['cell-1', 'cell-2']
                        }
                    ]
                },
                {
                    'Id': 'cell-1',
                    'BlockType': 'CELL',
                    'RowIndex': 1,
                    'ColumnIndex': 1,
                    'Confidence': 90.0,
                    'Relationships': [
                        {
                            'Type': 'CHILD',
                            'Ids': ['word-1']
                        }
                    ]
                },
                {
                    'Id': 'word-1',
                    'BlockType': 'WORD',
                    'Text': 'Labor',
                    'Confidence': 95.0
                }
            ]
        }
        
        with patch.object(processor.textract_client, 'analyze_document', return_value=mock_response):
            result = processor.process_document(bucket, key, file_size)
        
        # Assertions
        assert result is not None
        assert 'text_blocks' in result
        assert 'tables' in result
        assert 'processing_metadata' in result
        assert result['processing_metadata']['is_async'] is False
        assert len(result['text_blocks']) == 1
        assert result['text_blocks'][0]['text'] == 'Invoice Total: $76,160.00'
        assert result['text_blocks'][0]['confidence'] == 95.5
    
    @mock_textract
    @mock_s3
    def test_process_document_async_large_file(self):
        """Test asynchronous document processing for large files."""
        processor = TextractProcessor()
        bucket = "test-bucket"
        key = "large-document.pdf"
        file_size = 600 * 1024  # 600KB - above async threshold
        
        # Create mock S3 bucket and object
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=b"mock large pdf content")
        
        # Mock async job responses
        start_response = {'JobId': 'test-job-123'}
        
        get_response = {
            'JobStatus': 'SUCCEEDED',
            'Blocks': [
                {
                    'Id': 'block-1',
                    'BlockType': 'LINE',
                    'Text': 'Total Labor: $76,160.00',
                    'Confidence': 94.2,
                    'Page': 1,
                    'Geometry': {'BoundingBox': {'Left': 0.1, 'Top': 0.2, 'Width': 0.4, 'Height': 0.05}}
                }
            ]
        }
        
        with patch.object(processor.textract_client, 'start_document_analysis', return_value=start_response), \
             patch.object(processor.textract_client, 'get_document_analysis', return_value=get_response):
            
            result = processor.process_document(bucket, key, file_size)
        
        # Assertions
        assert result is not None
        assert result['processing_metadata']['is_async'] is True
        assert result['processing_metadata']['job_id'] == 'test-job-123'
        assert len(result['text_blocks']) == 1
        assert result['text_blocks'][0]['text'] == 'Total Labor: $76,160.00'
    
    def test_confidence_threshold_filtering(self):
        """Test that low confidence OCR results are filtered out."""
        processor = TextractProcessor()
        
        mock_response = {
            'Blocks': [
                {
                    'Id': 'block-1',
                    'BlockType': 'LINE',
                    'Text': 'High confidence text',
                    'Confidence': 95.0,  # Above threshold
                    'Page': 1,
                    'Geometry': {}
                },
                {
                    'Id': 'block-2',
                    'BlockType': 'LINE',
                    'Text': 'Low confidence text',
                    'Confidence': 70.0,  # Below threshold (80%)
                    'Page': 1,
                    'Geometry': {}
                }
            ]
        }
        
        result = processor._parse_textract_response(mock_response)
        
        # Only high confidence text should be included
        assert len(result['text_blocks']) == 1
        assert result['text_blocks'][0]['text'] == 'High confidence text'


class TestExcelProcessor:
    """Test cases for ExcelProcessor class."""
    
    @mock_s3
    def test_process_excel_file(self):
        """Test Excel file processing with pandas."""
        processor = ExcelProcessor()
        bucket = "test-bucket"
        key = "test-spreadsheet.xlsx"
        
        # Create mock Excel data
        df1 = pd.DataFrame({
            'Item': ['Labor', 'Materials', 'Equipment'],
            'Cost': [76160.00, 25000.00, 15000.00],
            'Quantity': [1, 2, 3]
        })
        
        df2 = pd.DataFrame({
            'Summary': ['Total Cost', 'Tax', 'Grand Total'],
            'Amount': [116160.00, 11616.00, 127776.00]
        })
        
        # Create Excel file in memory
        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df1.to_excel(writer, sheet_name='Items', index=False)
            df2.to_excel(writer, sheet_name='Summary', index=False)
        excel_content = excel_buffer.getvalue()
        
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=excel_content)
        
        # Process Excel file
        result = processor.process_excel_file(bucket, key)
        
        # Assertions
        assert result is not None
        assert 'sheets' in result
        assert 'summary' in result
        assert result['summary']['total_sheets'] == 2
        assert 'Items' in result['summary']['sheet_names']
        assert 'Summary' in result['summary']['sheet_names']
        
        # Check Items sheet
        items_sheet = next(sheet for sheet in result['sheets'] if sheet['sheet_name'] == 'Items')
        assert items_sheet['dimensions']['rows'] == 3
        assert items_sheet['dimensions']['columns'] == 3
        assert 'Cost' in items_sheet['columns']
        assert items_sheet['summary_stats']['Cost']['sum'] == 116160.00
        
        # Verify specific data
        labor_row = next(row for row in items_sheet['data'] if row['Item'] == 'Labor')
        assert float(labor_row['Cost']) == 76160.00


class TestSemanticChunker:
    """Test cases for SemanticChunker class."""
    
    def test_chunk_text_blocks(self):
        """Test chunking of text blocks by page and size."""
        chunker = SemanticChunker(max_chunk_size=100)  # Small size for testing
        
        text_blocks = [
            {'page': 1, 'text': 'First line of text', 'confidence': 95.0},
            {'page': 1, 'text': 'Second line of text', 'confidence': 92.0},
            {'page': 2, 'text': 'Third line on page 2', 'confidence': 88.0},
            {'page': 2, 'text': 'Fourth line on page 2', 'confidence': 90.0}
        ]
        
        file_metadata = {'file_name': 'test.pdf', 'file_size': 1000}
        extracted_data = {'text_blocks': text_blocks}
        
        chunks = chunker.chunk_extracted_data(extracted_data, file_metadata)
        
        # Should create separate chunks for different pages
        assert len(chunks) >= 2
        
        # Check chunk metadata
        for chunk in chunks:
            assert 'chunk_metadata' in chunk
            assert 'chunk_id' in chunk['chunk_metadata']
            assert 'estimated_tokens' in chunk['chunk_metadata']
            assert chunk['type'] == 'text'
    
    def test_chunk_tables(self):
        """Test chunking of table data."""
        chunker = SemanticChunker()
        
        tables = [
            {
                'page': 1,
                'table_id': 'table-1',
                'confidence': 95.0,
                'rows': [
                    [{'text': 'Item', 'confidence': 95}, {'text': 'Cost', 'confidence': 95}],
                    [{'text': 'Labor', 'confidence': 90}, {'text': '$76,160.00', 'confidence': 92}]
                ]
            }
        ]
        
        file_metadata = {'file_name': 'test.pdf'}
        extracted_data = {'tables': tables}
        
        chunks = chunker.chunk_extracted_data(extracted_data, file_metadata)
        
        assert len(chunks) == 1
        assert chunks[0]['type'] == 'table'
        assert 'Table 1 (Page 1)' in chunks[0]['content']
        assert '$76,160.00' in chunks[0]['content']
    
    def test_token_estimation(self):
        """Test token count estimation."""
        chunker = SemanticChunker()
        
        # Test with known text
        text = "This is a test sentence with exactly twenty-four characters."
        estimated_tokens = chunker._estimate_tokens(text)
        
        # Should be approximately text length / 4
        expected_tokens = len(text) // 4
        assert abs(estimated_tokens - expected_tokens) <= 1


class TestLambdaHandler:
    """Test cases for the main Lambda handler."""
    
    @mock_s3
    @mock_textract
    def test_lambda_handler_extraction_task(self):
        """Test the main Lambda handler with extraction task."""
        # Setup test data
        event = {
            'task': 'extract',
            'input': {
                'file_info': {
                    'key': 'test-invoice.pdf',
                    'size': 100000,
                    'extension': '.pdf'
                },
                'bucket': 'test-bucket'
            }
        }
        
        context = Mock()
        
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(Bucket='test-bucket', Key='test-invoice.pdf', Body=b"mock pdf content")
        
        # Mock Textract response
        mock_textract_response = {
            'Blocks': [
                {
                    'Id': 'block-1',
                    'BlockType': 'LINE',
                    'Text': 'Total Labor: $76,160.00',
                    'Confidence': 95.0,
                    'Page': 1,
                    'Geometry': {}
                }
            ]
        }
        
        with patch('extraction_lambda.textract_client') as mock_textract:
            mock_textract.analyze_document.return_value = mock_textract_response
            
            result = lambda_handler(event, context)
        
        # Assertions
        assert result is not None
        assert result['extraction_status'] == 'completed'
        assert 'extracted_data' in result
        assert 'semantic_chunks' in result
        assert 'processing_summary' in result
        
        # Check that the expected invoice total is found
        text_blocks = result['extracted_data']['text_blocks']
        assert any('$76,160.00' in block['text'] for block in text_blocks)
    
    @mock_s3
    def test_lambda_handler_excel_extraction(self):
        """Test Lambda handler with Excel file extraction."""
        # Create test Excel file
        df = pd.DataFrame({
            'Description': ['Labor Costs', 'Material Costs', 'Total'],
            'Amount': [76160.00, 25000.00, 101160.00]
        })
        
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False, engine='openpyxl')
        excel_content = excel_buffer.getvalue()
        
        # Setup event
        event = {
            'task': 'extract',
            'input': {
                'file_info': {
                    'key': 'test-invoice.xlsx',
                    'size': len(excel_content),
                    'extension': '.xlsx'
                },
                'bucket': 'test-bucket'
            }
        }
        
        context = Mock()
        
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        s3_client.put_object(Bucket='test-bucket', Key='test-invoice.xlsx', Body=excel_content)
        
        result = lambda_handler(event, context)
        
        # Assertions
        assert result is not None
        assert result['extraction_status'] == 'completed'
        assert 'extracted_data' in result
        assert 'sheets' in result['extracted_data']
        
        # Check for expected labor cost
        sheets = result['extracted_data']['sheets']
        assert len(sheets) > 0
        sheet_data = sheets[0]['data']
        labor_row = next((row for row in sheet_data if 'Labor' in str(row.get('Description', ''))), None)
        assert labor_row is not None
        assert float(labor_row['Amount']) == 76160.00
    
    def test_lambda_handler_unsupported_file_type(self):
        """Test Lambda handler with unsupported file type."""
        event = {
            'task': 'extract',
            'input': {
                'file_info': {
                    'key': 'test-file.txt',
                    'size': 1000,
                    'extension': '.txt'
                },
                'bucket': 'test-bucket'
            }
        }
        
        context = Mock()
        
        with pytest.raises(ValueError, match="Unsupported file type"):
            lambda_handler(event, context)
    
    def test_lambda_handler_unknown_task(self):
        """Test Lambda handler with unknown task."""
        event = {
            'task': 'unknown_task',
            'input': {}
        }
        
        context = Mock()
        
        with pytest.raises(ValueError, match="Unknown task"):
            lambda_handler(event, context)


class TestIntegration:
    """Integration tests for the complete extraction workflow."""
    
    @mock_s3
    @mock_textract
    def test_complete_pdf_extraction_workflow(self):
        """Test complete PDF extraction workflow with expected JSON structure."""
        # Setup
        bucket = "test-bucket"
        key = "invoice-22-pages.pdf"
        file_size = 600 * 1024  # Large file for async processing
        
        # Create S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=b"mock large pdf content")
        
        # Mock comprehensive Textract response
        mock_response = {
            'JobStatus': 'SUCCEEDED',
            'Blocks': [
                # Text blocks
                {
                    'Id': 'line-1',
                    'BlockType': 'LINE',
                    'Text': 'INVOICE',
                    'Confidence': 98.5,
                    'Page': 1,
                    'Geometry': {'BoundingBox': {'Left': 0.4, 'Top': 0.1, 'Width': 0.2, 'Height': 0.05}}
                },
                {
                    'Id': 'line-2',
                    'BlockType': 'LINE',
                    'Text': 'Total Labor: $76,160.00',
                    'Confidence': 95.2,
                    'Page': 22,
                    'Geometry': {'BoundingBox': {'Left': 0.6, 'Top': 0.8, 'Width': 0.3, 'Height': 0.05}}
                },
                # Table block
                {
                    'Id': 'table-1',
                    'BlockType': 'TABLE',
                    'Confidence': 92.0,
                    'Page': 2,
                    'Relationships': [{'Type': 'CHILD', 'Ids': ['cell-1', 'cell-2']}]
                },
                # Table cells
                {
                    'Id': 'cell-1',
                    'BlockType': 'CELL',
                    'RowIndex': 1,
                    'ColumnIndex': 1,
                    'Confidence': 90.0,
                    'Relationships': [{'Type': 'CHILD', 'Ids': ['word-1']}]
                },
                {
                    'Id': 'cell-2',
                    'BlockType': 'CELL',
                    'RowIndex': 1,
                    'ColumnIndex': 2,
                    'Confidence': 88.0,
                    'Relationships': [{'Type': 'CHILD', 'Ids': ['word-2']}]
                },
                # Words
                {
                    'Id': 'word-1',
                    'BlockType': 'WORD',
                    'Text': 'Description',
                    'Confidence': 95.0
                },
                {
                    'Id': 'word-2',
                    'BlockType': 'WORD',
                    'Text': 'Amount',
                    'Confidence': 93.0
                },
                # Form fields
                {
                    'Id': 'key-1',
                    'BlockType': 'KEY_VALUE_SET',
                    'EntityTypes': ['KEY'],
                    'Confidence': 89.0,
                    'Page': 1,
                    'Relationships': [
                        {'Type': 'VALUE', 'Ids': ['value-1']},
                        {'Type': 'CHILD', 'Ids': ['word-3']}
                    ]
                },
                {
                    'Id': 'value-1',
                    'BlockType': 'KEY_VALUE_SET',
                    'EntityTypes': ['VALUE'],
                    'Confidence': 87.0,
                    'Relationships': [{'Type': 'CHILD', 'Ids': ['word-4']}]
                },
                {
                    'Id': 'word-3',
                    'BlockType': 'WORD',
                    'Text': 'Invoice',
                    'Confidence': 92.0
                },
                {
                    'Id': 'word-4',
                    'BlockType': 'WORD',
                    'Text': 'Number:',
                    'Confidence': 90.0
                }
            ]
        }
        
        # Test the complete workflow
        event = {
            'task': 'extract',
            'input': {
                'file_info': {
                    'key': key,
                    'size': file_size,
                    'extension': '.pdf'
                },
                'bucket': bucket
            }
        }
        
        with patch('extraction_lambda.textract_client') as mock_textract:
            mock_textract.start_document_analysis.return_value = {'JobId': 'test-job-123'}
            mock_textract.get_document_analysis.return_value = mock_response
            
            result = lambda_handler(event, {})
        
        # Comprehensive assertions for expected JSON structure
        assert result['extraction_status'] == 'completed'
        
        # Check extracted data structure
        extracted_data = result['extracted_data']
        assert 'text_blocks' in extracted_data
        assert 'tables' in extracted_data
        assert 'forms' in extracted_data
        assert 'page_count' in extracted_data
        assert 'processing_metadata' in extracted_data
        
        # Verify processing metadata
        metadata = extracted_data['processing_metadata']
        assert metadata['is_async'] is True
        assert metadata['job_id'] == 'test-job-123'
        assert 'timestamp' in metadata
        
        # Check semantic chunks
        chunks = result['semantic_chunks']
        assert len(chunks) > 0
        
        # Verify chunk structure
        for chunk in chunks:
            assert 'type' in chunk
            assert 'content' in chunk
            assert 'metadata' in chunk
            assert 'chunk_metadata' in chunk
            
            chunk_meta = chunk['chunk_metadata']
            assert 'chunk_id' in chunk_meta
            assert 'estimated_tokens' in chunk_meta
            assert 'created_at' in chunk_meta
        
        # Check processing summary
        summary = result['processing_summary']
        assert summary['processing_method'] == 'async'
        assert summary['confidence_threshold'] == 0.8
        assert summary['total_chunks'] == len(chunks)
        
        # Verify specific content - should find the labor total
        text_content = ' '.join([chunk['content'] for chunk in chunks if chunk['type'] == 'text'])
        assert '$76,160.00' in text_content
        
        # Verify page count (22 pages)
        assert extracted_data['page_count'] == 22


class TestBedrockProcessor:
    """Test cases for BedrockProcessor class."""
    
    def test_normalize_extracted_data(self):
        """Test semantic normalization using Bedrock."""
        processor = BedrockProcessor()
        
        # Mock extracted data with vendor terminology
        extracted_data = {
            'text_blocks': [
                {'text': 'Rate: $100.00', 'confidence': 95.0, 'page': 1},
                {'text': 'Consumables: $500.00', 'confidence': 92.0, 'page': 1}
            ],
            'tables': [
                {
                    'page': 1,
                    'rows': [
                        [{'text': 'Worker', 'confidence': 95}, {'text': 'Rate', 'confidence': 95}],
                        [{'text': 'Smith, John', 'confidence': 90}, {'text': '$100.00', 'confidence': 92}]
                    ]
                }
            ]
        }
        
        # Mock Bedrock response
        mock_bedrock_response = {
            'body': Mock()
        }
        mock_bedrock_response['body'].read.return_value = json.dumps({
            "completion": json.dumps({
                "labor": [
                    {"name": "Smith, John", "type": "RS", "unit_price": 100.00, "total_hours": 55.0}
                ],
                "materials": [
                    {"description": "Consumables", "unit_price": 500.00, "quantity": 1}
                ]
            })
        }).encode()
        
        with patch.object(processor.bedrock_client, 'invoke_model', return_value=mock_bedrock_response):
            result = processor.normalize_extracted_data(extracted_data)
        
        assert result is not None
        assert 'labor' in result
        assert 'materials' not in result, "Materials handling removed"
        assert len(result['labor']) == 1
        assert result['labor'][0]['name'] == 'Smith, John'
        assert result['labor'][0]['unit_price'] == 100.00


class TestComprehendProcessor:
    """Test cases for ComprehendProcessor class."""
    
    def test_extract_entities(self):
        """Test entity extraction using Comprehend."""
        processor = ComprehendProcessor()
        
        text = "Smith, John worked as RS labor for 55 hours at $100 per hour."
        
        # Mock Comprehend response
        mock_response = {
            'Entities': [
                {
                    'Text': 'Smith, John',
                    'Type': 'PERSON',
                    'Score': 0.95,
                    'BeginOffset': 0,
                    'EndOffset': 11
                },
                {
                    'Text': 'RS',
                    'Type': 'OTHER',
                    'Score': 0.88,
                    'BeginOffset': 22,
                    'EndOffset': 24
                },
                {
                    'Text': '55',
                    'Type': 'QUANTITY',
                    'Score': 0.92,
                    'BeginOffset': 35,
                    'EndOffset': 37
                }
            ]
        }
        
        with patch.object(processor.comprehend_client, 'detect_entities', return_value=mock_response):
            result = processor.extract_entities(text)
        
        # Assertions
        assert result is not None
        assert len(result) == 3
        
        person_entity = next(e for e in result if e['Type'] == 'PERSON')
        assert person_entity['Text'] == 'Smith, John'
        assert person_entity['Score'] == 0.95
        
        labor_type_entity = next(e for e in result if e['Text'] == 'RS')
        assert labor_type_entity['Type'] == 'OTHER'
    
    def test_comprehend_error_handling(self):
        """Test Comprehend error handling."""
        processor = ComprehendProcessor()
        
        text = "Test text"
        
        # Mock Comprehend client to raise an exception
        with patch.object(processor.comprehend_client, 'detect_entities', side_effect=Exception("Comprehend error")):
            result = processor.extract_entities(text)
        
        # Should return empty list on error
        assert result == []


class TestIntelligentExtractor:
    """Test cases for IntelligentExtractor class."""
    
    @mock_s3
    def test_process_document_intelligently(self):
        """Test intelligent document processing with all components."""
        extractor = IntelligentExtractor()
        
        bucket = "test-bucket"
        key = "intelligent-test.pdf"
        file_size = 100 * 1024
        
        # Setup S3 mock
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=key, Body=b"mock pdf content")
        
        # Mock basic extraction result
        basic_extraction = {
            'text_blocks': [
                {'text': 'Rate: $100.00 for Smith, John (RS)', 'confidence': 95.0, 'page': 1}
            ],
            'tables': [],
            'forms': [],
            'processing_metadata': {'is_async': False}
        }
        
        # Mock entity extraction
        entities = [
            {'Text': 'Smith, John', 'Type': 'PERSON', 'Score': 0.95},
            {'Text': 'RS', 'Type': 'OTHER', 'Score': 0.88}
        ]
        
        # Mock normalized data
        normalized_data = {
            'labor': [
                {'name': 'Smith, John', 'type': 'RS', 'unit_price': 100.00, 'total_hours': 55.0}
            ],
            'materials': []
        }
        
        with patch.object(extractor.textract_processor, 'process_document', return_value=basic_extraction), \
             patch.object(extractor.comprehend_processor, 'extract_entities', return_value=entities), \
             patch.object(extractor.bedrock_processor, 'normalize_extracted_data', return_value=normalized_data):
            
            result = extractor.process_document_intelligently(bucket, key, file_size)
        
        # Assertions
        assert result is not None
        assert 'basic_extraction' in result
        assert 'entities' in result
        assert 'normalized_data' in result
        assert 'semantic_chunks' in result
        
        # Check normalized data structure
        assert 'labor' in result['normalized_data']
        assert len(result['normalized_data']['labor']) == 1
        assert result['normalized_data']['labor'][0]['name'] == 'Smith, John'
        assert result['normalized_data']['labor'][0]['type'] == 'RS'
    
    def test_intelligent_extraction_fallback(self):
        """Test fallback to basic extraction when intelligent processing fails."""
        extractor = IntelligentExtractor()
        
        bucket = "test-bucket"
        key = "fallback-test.pdf"
        file_size = 100 * 1024
        
        # Mock basic extraction result
        basic_extraction = {
            'text_blocks': [{'text': 'Basic extraction result', 'confidence': 95.0}],
            'processing_metadata': {'is_async': False}
        }
        
        with patch.object(extractor.textract_processor, 'process_document', return_value=basic_extraction), \
             patch.object(extractor.comprehend_processor, 'extract_entities', side_effect=Exception("Comprehend failed")), \
             patch.object(extractor.bedrock_processor, 'normalize_extracted_data', side_effect=Exception("Bedrock failed")):
            
            result = extractor.process_document_intelligently(bucket, key, file_size)
        
        # Should still return basic extraction
        assert result is not None
        assert 'basic_extraction' in result
        assert result['basic_extraction']['text_blocks'][0]['text'] == 'Basic extraction result'
        
        # Intelligent features should be None or empty due to failures
        assert result.get('entities') == []
        assert result.get('normalized_data') is None


class TestTerminologyVariations:
    """Test cases for handling various vendor terminology."""
    
    @mock_s3
    @mock_textract
    def test_terminology_mapping_variations(self):
        """Test extraction with different vendor terminology variations."""
        # Test data with various terminology
        terminology_variations = [
            {'vendor_term': 'Rate', 'expected_normalized': 'unit_price'},
            {'vendor_term': 'Hourly Rate', 'expected_normalized': 'unit_price'},
            {'vendor_term': 'Cost per Hour', 'expected_normalized': 'unit_price'},
            {'vendor_term': 'Consumables', 'expected_normalized': 'materials'},
            {'vendor_term': 'Supplies', 'expected_normalized': 'materials'},
            {'vendor_term': 'Materials', 'expected_normalized': 'materials'}
        ]
        
        for variation in terminology_variations:
            # Setup test event
            event = {
                'task': 'extract',
                'input': {
                    'file_info': {
                        'key': f'test-{variation["vendor_term"].lower().replace(" ", "-")}.pdf',
                        'size': 100000,
                        'extension': '.pdf'
                    },
                    'bucket': 'test-bucket'
                }
            }
            
            # Mock Textract response with vendor terminology
            mock_response = {
                'Blocks': [
                    {
                        'Id': 'block-1',
                        'BlockType': 'LINE',
                        'Text': f'{variation["vendor_term"]}: $100.00',
                        'Confidence': 95.0,
                        'Page': 1,
                        'Geometry': {}
                    }
                ]
            }
            
            # Setup S3 mock
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.create_bucket(Bucket='test-bucket')
            s3_client.put_object(
                Bucket='test-bucket', 
                Key=event['input']['file_info']['key'], 
                Body=b"mock pdf content"
            )
            
            with patch('extraction_lambda.textract_client') as mock_textract:
                mock_textract.analyze_document.return_value = mock_response
                
                result = lambda_handler(event, {})
            
            # Verify the terminology was captured
            assert result['extraction_status'] == 'completed'
            text_blocks = result['extracted_data']['text_blocks']
            assert any(variation['vendor_term'] in block['text'] for block in text_blocks)
    
    def test_labor_type_recognition(self):
        """Test recognition of different labor types (RS, US, SS, SU, EN)."""
        labor_types = ['RS', 'US', 'SS', 'SU', 'EN']
        
        processor = ComprehendProcessor()
        
        for labor_type in labor_types:
            text = f"Smith, John worked as {labor_type} labor for the project."
            
            # Mock Comprehend response
            mock_response = {
                'Entities': [
                    {
                        'Text': 'Smith, John',
                        'Type': 'PERSON',
                        'Score': 0.95,
                        'BeginOffset': 0,
                        'EndOffset': 11
                    },
                    {
                        'Text': labor_type,
                        'Type': 'OTHER',
                        'Score': 0.88,
                        'BeginOffset': text.find(labor_type),
                        'EndOffset': text.find(labor_type) + len(labor_type)
                    }
                ]
            }
            
            with patch.object(processor.comprehend_client, 'detect_entities', return_value=mock_response):
                entities = processor.extract_entities(text)
            
            # Verify labor type was detected
            labor_entity = next((e for e in entities if e['Text'] == labor_type), None)
            assert labor_entity is not None
            assert labor_entity['Type'] == 'OTHER'
            assert labor_entity['Score'] >= 0.8


if __name__ == '__main__':
    pytest.main([__file__])
