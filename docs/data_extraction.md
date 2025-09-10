# Data Extraction Component

## Overview

The data extraction component is an intelligent serverless solution that integrates Amazon Textract, Amazon Bedrock, Amazon Comprehend, and pandas to process PDFs, images, and Excel files uploaded to S3. It provides structured JSON output with semantic chunking optimized for Bedrock token limits, featuring adaptive field mapping and terminology normalization for handling vendor variations.

## Key Features

### Intelligent Processing Pipeline
- **Amazon Textract**: OCR and document analysis for PDFs/images
- **Amazon Bedrock**: Semantic mapping and field normalization using Claude model
- **Amazon Comprehend**: Entity recognition for custom labor types (RS, US, SS, SU, EN)
- **Adaptive Field Mapping**: Handles vendor terminology variations without hardcoding
- **Fallback Mechanisms**: Graceful degradation when intelligent processing fails

### Terminology Normalization
- Maps vendor-specific terms to standardized fields:
  - "Rate" / "Hourly Rate" / "Cost per Hour" → "unit_price"
  - "Consumables" / "Supplies" → "materials"
  - "Worker" / "Employee" → "name"
- Supports custom entity recognition for labor classifications
- LLM-powered field inference for unknown terminology

## Architecture

### Components

1. **Extraction Lambda Function** (`extraction-lambda`)
   - Handles document data extraction using Amazon Textract for PDFs/images
   - Processes Excel files using pandas
   - Implements semantic chunking for Bedrock compatibility
   - Supports both synchronous and asynchronous processing

2. **Step Functions Integration**
   - Updated workflow includes data extraction step
   - Orchestrates the complete file processing pipeline
   - Handles error recovery and retry logic

3. **S3 Storage**
   - Stores original files and extraction results
   - Large extraction results (>200KB) are stored in S3 with references

### Processing Flow

```
S3 Upload → Ingestion Lambda → Step Functions Workflow
                                      ↓
                              File Validation
                                      ↓
                              Metadata Extraction
                                      ↓
                              Data Extraction (NEW)
                                      ↓
                              File Processing
                                      ↓
                              Success/Failure
```

## Features

### Document Processing

#### PDF and Image Processing (Amazon Textract)
- **Synchronous Processing**: Files ≤ 500KB
- **Asynchronous Processing**: Files > 500KB (e.g., 22-page invoices)
- **Feature Extraction**: Tables, forms, and layout analysis
- **Confidence Filtering**: OCR results below 80% confidence are filtered out

#### Excel Processing (pandas)
- Multi-sheet support
- Automatic data type detection
- Summary statistics for numeric columns
- JSON serialization of complex data types

### Semantic Chunking

Optimized for Bedrock token limits (4000 tokens per chunk):

- **Text Chunks**: Grouped by page with size limits
- **Table Chunks**: Individual tables as separate chunks
- **Form Chunks**: Key-value pairs grouped by page
- **Excel Chunks**: Individual sheets with row limits

Each chunk includes:
- Content type (text, table, form, excel)
- Source metadata (file name, page numbers)
- Confidence scores
- Token estimation
- Unique chunk identifiers

### Error Handling

- **Low Confidence OCR**: Results below threshold are excluded
- **Async Job Timeouts**: 5-minute timeout with polling
- **Unsupported File Types**: Clear error messages
- **Large Response Handling**: Automatic S3 storage for large results

## Configuration

### Environment Variables

```bash
BUCKET_NAME=audit-files-bucket
LOG_LEVEL=INFO
ASYNC_THRESHOLD_BYTES=512000  # 500KB
MAX_CHUNK_SIZE=4000          # Bedrock token limit
CONFIDENCE_THRESHOLD=0.8     # 80% minimum confidence
```

### IAM Permissions

The extraction Lambda requires:
- S3: GetObject, PutObject, GetObjectTagging, PutObjectTagging
- Textract: AnalyzeDocument, StartDocumentAnalysis, GetDocumentAnalysis
- Bedrock: InvokeModel, InvokeModelWithResponseStream (for semantic mapping)
- Comprehend: DetectEntities, DetectKeyPhrases, DetectSentiment, DetectSyntax (for entity recognition)
- Step Functions: SendTaskSuccess, SendTaskFailure, SendTaskHeartbeat

## API Reference

### Lambda Handler

```python
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]
```

**Input Event Structure:**
```json
{
  "task": "extract",
  "input": {
    "file_info": {
      "key": "invoice.pdf",
      "size": 1048576,
      "extension": ".pdf"
    },
    "bucket": "audit-files-bucket"
  }
}
```

**Output Structure:**
```json
{
  "extraction_status": "completed",
  "file_info": { ... },
  "extracted_data": {
    "text_blocks": [...],
    "tables": [...],
    "forms": [...],
    "page_count": 22,
    "processing_metadata": {
      "is_async": true,
      "job_id": "textract-job-123",
      "timestamp": "2025-01-09T20:50:00Z"
    }
  },
  "semantic_chunks": [
    {
      "type": "text",
      "content": "Invoice content...",
      "metadata": {
        "source_file": "invoice.pdf",
        "pages": [1, 2],
        "confidence_scores": [95.2, 92.1]
      },
      "chunk_metadata": {
        "chunk_id": "invoice.pdf_1",
        "estimated_tokens": 1250,
        "created_at": "2025-01-09T20:50:00Z"
      }
    }
  ],
  "processing_summary": {
    "total_chunks": 5,
    "processing_method": "async",
    "confidence_threshold": 0.8
  }
}
```

### Classes

#### TextractProcessor
Handles Amazon Textract document processing.

**Methods:**
- `process_document(bucket, key, file_size)`: Main processing method
- `_process_document_sync(bucket, key)`: Synchronous processing
- `_process_document_async(bucket, key)`: Asynchronous processing
- `_parse_textract_response(response)`: Parse Textract blocks into structured data

#### ExcelProcessor
Handles Excel file processing using pandas.

**Methods:**
- `process_excel_file(bucket, key)`: Process Excel file from S3
- `_process_sheet(sheet_name, df)`: Process individual worksheet

#### SemanticChunker
Handles chunking of extracted data for Bedrock compatibility.

**Methods:**
- `chunk_extracted_data(extracted_data, file_metadata)`: Main chunking method
- `_chunk_text_blocks(text_blocks, file_metadata)`: Chunk text by page/size
- `_chunk_tables(tables, file_metadata)`: Chunk tables individually
- `_estimate_tokens(text)`: Estimate token count

## Sample Output

### Invoice Processing Example

For a 22-page invoice with total labor cost of $76,160.00:

```json
{
  "extraction_status": "completed",
  "extracted_data": {
    "text_blocks": [
      {
        "page": 1,
        "text": "INVOICE",
        "confidence": 98.5,
        "geometry": {...}
      },
      {
        "page": 22,
        "text": "Total Labor: $76,160.00",
        "confidence": 95.2,
        "geometry": {...}
      }
    ],
    "tables": [
      {
        "page": 2,
        "table_id": "table-1",
        "confidence": 92.0,
        "rows": [
          [
            {"text": "Description", "confidence": 95},
            {"text": "Amount", "confidence": 93}
          ],
          [
            {"text": "Labor Services", "confidence": 90},
            {"text": "$76,160.00", "confidence": 92}
          ]
        ]
      }
    ],
    "forms": [
      {
        "page": 1,
        "key": "Invoice Number:",
        "value": "INV-2025-001",
        "key_confidence": 89.0
      }
    ],
    "page_count": 22,
    "processing_metadata": {
      "is_async": true,
      "job_id": "textract-job-abc123",
      "total_blocks": 1247,
      "timestamp": "2025-01-09T20:50:00Z"
    }
  },
  "semantic_chunks": [
    {
      "type": "text",
      "content": "INVOICE\nInvoice Number: INV-2025-001\n...",
      "metadata": {
        "source_file": "invoice.pdf",
        "pages": [1],
        "confidence_scores": [98.5, 89.0]
      },
      "chunk_metadata": {
        "chunk_id": "invoice.pdf_1",
        "chunk_index": 1,
        "total_chunks": 5,
        "estimated_tokens": 850,
        "created_at": "2025-01-09T20:50:00Z"
      }
    },
    {
      "type": "table",
      "content": "Table 1 (Page 2):\nRow 1: Description | Amount\nRow 2: Labor Services | $76,160.00\n",
      "metadata": {
        "source_file": "invoice.pdf",
        "page": 2,
        "table_index": 1,
        "confidence": 92.0,
        "dimensions": {"rows": 2, "columns": 2}
      },
      "chunk_metadata": {
        "chunk_id": "invoice.pdf_2",
        "chunk_index": 2,
        "total_chunks": 5,
        "estimated_tokens": 45,
        "created_at": "2025-01-09T20:50:00Z"
      }
    }
  ],
  "processing_summary": {
    "total_chunks": 5,
    "processing_method": "async",
    "confidence_threshold": 0.8,
    "timestamp": "2025-01-09T20:50:00Z"
  }
}
```

## Testing

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-cov moto

# Run extraction tests
pytest tests/test_extraction.py -v

# Run with coverage
pytest tests/test_extraction.py --cov=lambda/extraction_lambda --cov-report=html
```

### Test Coverage

The test suite includes:
- **Unit Tests**: Individual class and method testing
- **Integration Tests**: Complete workflow testing
- **Mock Tests**: S3 and Textract service mocking
- **Edge Cases**: Error handling and unsupported file types
- **Sample Data**: 22-page invoice with $76,160.00 labor total

### Sample Test Data

Tests validate extraction of specific invoice data:
- Invoice totals: $76,160.00
- Multi-page documents (22 pages)
- Table extraction with labor costs
- Form field extraction (invoice numbers)
- Excel processing with summary statistics

## Deployment

### Prerequisites

1. AWS CDK installed and configured
2. Python 3.11 runtime
3. Required dependencies in `requirements.txt`

### Deploy Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Deploy infrastructure
cdk deploy

# Verify deployment
aws lambda list-functions --query 'Functions[?FunctionName==`extraction-lambda`]'
```

### Monitoring

- **CloudWatch Logs**: `/aws/lambda/extraction-lambda`
- **Step Functions**: Monitor workflow executions
- **S3 Events**: Track file uploads and processing
- **Textract Jobs**: Monitor async job completion

## Performance Considerations

### File Size Thresholds

- **Small files (≤500KB)**: Synchronous processing (~2-5 seconds)
- **Large files (>500KB)**: Asynchronous processing (~30-300 seconds)
- **Very large files (>10MB)**: Consider pre-processing or splitting

### Memory and Timeout

- **Memory**: 1024MB (optimized for pandas operations)
- **Timeout**: 15 minutes (accommodates large async jobs)
- **Concurrent Executions**: Limited by Textract service quotas

### Cost Optimization

- **Textract Costs**: ~$1.50 per 1000 pages for AnalyzeDocument
- **Lambda Costs**: ~$0.20 per 1M requests + compute time
- **S3 Storage**: Lifecycle policies for cost management

## Troubleshooting

### Common Issues

1. **Textract Timeout**
   - Increase Lambda timeout
   - Check Textract service limits
   - Verify S3 object accessibility

2. **Low Confidence Results**
   - Adjust `CONFIDENCE_THRESHOLD` environment variable
   - Improve document quality before upload
   - Review OCR results in CloudWatch logs

3. **Memory Issues**
   - Increase Lambda memory allocation
   - Optimize pandas operations
   - Consider chunking large Excel files

4. **Step Functions Failures**
   - Check IAM permissions
   - Review CloudWatch logs
   - Verify input data format

### Debug Mode

Enable detailed logging:
```bash
export LOG_LEVEL=DEBUG
```

This provides:
- Detailed Textract responses
- Chunk creation process
- Performance metrics
- Error stack traces

## Future Enhancements

### Planned Features

1. **Additional File Types**
   - Word documents (.docx)
   - PowerPoint presentations (.pptx)
   - CSV files with intelligent parsing

2. **Enhanced Processing**
   - OCR quality improvement
   - Custom entity recognition
   - Multi-language support

3. **Performance Optimizations**
   - Parallel processing for multi-page documents
   - Caching for repeated extractions
   - Streaming for very large files

4. **Integration Enhancements**
   - Direct Bedrock integration
   - Real-time processing webhooks
   - Batch processing capabilities

## Support

For issues and questions:
1. Check CloudWatch logs for detailed error messages
2. Review test cases for expected behavior
3. Consult AWS Textract documentation for service limits
4. Monitor Step Functions execution history for workflow issues
