# Report Generation Component

## Overview

The Report Generation component is responsible for creating comprehensive audit reports from MSA compliance analysis results. It generates reports in multiple formats (Markdown, Excel, PDF) using AI-powered content generation and template-based Excel processing.

## Architecture

### Components

1. **BedrockReportGenerator**: Uses Amazon Bedrock to generate intelligent Markdown reports
2. **ExcelReportGenerator**: Creates Excel reports using XXXI templates with multiple tabs
3. **PDFConverter**: Converts Markdown reports to PDF format
4. **ReportManager**: Orchestrates the complete report generation workflow

### Integration Points

- **Input**: Receives flags data from comparison Lambda, metadata from extraction, and extracted data
- **Output**: Generates reports in S3 buckets (Markdown, Excel, PDF formats)
- **Trigger**: Invoked by Step Functions after agent analysis completion

## Report Formats

### 1. Markdown Report (Bedrock-Generated)

**Features:**
- AI-generated executive summary
- Detailed findings analysis
- Financial impact assessment
- Actionable recommendations
- MSA compliance evaluation

**Sample Content:**
```markdown
# MSA Audit Report

## Executive Summary
Overcharge: $375 on RS labor; Savings: 10% of $160k

## Detailed Findings
### Rate Variances (2 found)
- **Smith, John (RS)**: Charged $77.00 vs MSA standard $70.00 (10% variance)
```

### 2. Excel Report (XXXI Template)

**Tabs:**
- **Project Information**: PDF metadata including Date of Loss (2/12/2025)
- **Project Summary**: As Presented $77k labor vs As Analyzed (reduced amounts)
- **Labor Export**: Detailed PDF rows with worker data, rates, and variances

**Key Metrics:**
- Total Project: $148,478.04 (as presented) → $148,103.04 (as analyzed)
- Labor Costs: $77,000.00 → $76,625.00
- Material Costs: $71,478.04 (unchanged)
- Total Savings: $375.00

### 3. PDF Report

**Features:**
- Professional styling with CSS
- Tables and formatted content
- Converted from Markdown using wkhtmltopdf
- Suitable for stakeholder distribution

## Configuration

### Environment Variables

```bash
# Bedrock Configuration
BEDROCK_MODEL_ID=anthropic.claude-3-5-sonnet-20241022-v2:0

# S3 Buckets
REPORTS_BUCKET=msa-audit-reports-{account}-{region}
TEMPLATE_BUCKET=msa-audit-templates-{account}-{region}
TEMPLATE_KEY=XXXI_Template.xlsx

# Database
MSA_RATES_TABLE=msa-rates

# Logging
LOG_LEVEL=INFO
```

### Lambda Configuration

```yaml
Function: report-lambda
Runtime: python3.11
Memory: 2048 MB  # Higher memory for Excel/PDF processing
Timeout: 15 minutes
Layers:
  - wkhtmltopdf-layer  # For PDF generation
```

## Usage

### Step Functions Integration

The report Lambda is automatically triggered as the final step in the invoice processing workflow:

```json
{
  "task": "generate_report",
  "flags_data": {
    "total_savings": 375.00,
    "rate_variances": [...],
    "overtime_violations": [...],
    "anomalies": [...]
  },
  "metadata": {
    "invoice_number": "INV-2024-001",
    "vendor": "ABC Construction",
    "date_of_loss": "2/12/2025",
    "invoice_total": 148478.04,
    "labor_total": 77000.00,
    "material_total": 71478.04
  },
  "extracted_data": {
    "normalized_data": {
      "labor": [...]
    }
  }
}
```

### Direct Invocation

```python
from report_lambda import handle_report_generation

result = handle_report_generation(
    flags_data=comparison_results,
    metadata=invoice_metadata,
    extracted_data=extraction_results
)
```

## Template Management

### XXXI Template Structure

The Excel template should contain the following sheets:

1. **Project Information**
   - Field/Value pairs for metadata
   - Invoice details and processing information

2. **Project Summary**
   - Financial comparison table
   - As Presented vs As Analyzed columns
   - Savings calculations

3. **Labor Export**
   - Detailed worker information
   - Rate comparisons and variances
   - MSA compliance indicators

### Template Upload

Upload the XXXI template to the templates bucket:

```bash
aws s3 cp XXXI_Template.xlsx s3://msa-audit-templates-{account}-{region}/
```

## Error Handling

### Fallback Mechanisms

1. **Bedrock Unavailable**: Falls back to template-based Markdown generation
2. **Template Missing**: Creates Excel report programmatically
3. **PDF Generation Fails**: Returns empty PDF (report still available in other formats)

### Error Responses

```json
{
  "report_id": "error-20250109-145030",
  "generation_status": "failed",
  "error": "Detailed error message",
  "timestamp": "2025-01-09T21:50:30Z"
}
```

## Monitoring and Logging

### CloudWatch Metrics

- Report generation duration
- Success/failure rates
- File sizes generated
- S3 upload performance

### Log Events

```python
# Successful generation
logger.info(f"Successfully generated comprehensive report: {report_id}")

# Error handling
logger.error(f"Error generating Excel report: {str(e)}")

# Performance tracking
logger.info(f"Report generation completed: {result['generation_status']}")
```

## Security

### IAM Permissions

The report Lambda requires:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": "arn:aws:bedrock:*::foundation-model/anthropic.claude-3-5-sonnet-*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::msa-audit-reports-*",
        "arn:aws:s3:::msa-audit-reports-*/*",
        "arn:aws:s3:::msa-audit-templates-*",
        "arn:aws:s3:::msa-audit-templates-*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:Query"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/msa-rates"
    }
  ]
}
```

### Data Protection

- All S3 buckets use server-side encryption
- Reports contain sensitive financial data
- Access controlled through IAM policies
- Lifecycle policies for automatic archival

## Testing

### Unit Tests

```bash
# Run report generation tests
python -m pytest tests/test_report.py -v

# Test specific components
python -m pytest tests/test_report.py::TestBedrockReportGenerator -v
python -m pytest tests/test_report.py::TestExcelReportGenerator -v
```

### Integration Tests

```python
# Test complete workflow
def test_complete_report_generation_workflow():
    # Verify Bedrock report generation
    # Verify Excel template processing
    # Verify PDF conversion
    # Verify S3 uploads
    # Assert final totals match requirements
```

### Test Data Validation

Key test assertions:
- Excel summary total: $148,478.04 adjusted to $148,103.04
- Savings calculation: $375.00
- Rate variance detection: RS labor $77 vs MSA $70
- Template tab creation: Project Information, Project Summary, Labor Export

## Performance Optimization

### Memory Management

- 2048 MB memory allocation for large Excel processing
- Streaming S3 uploads for large files
- Temporary file cleanup

### Caching

- Template caching for repeated use
- Bedrock response optimization
- S3 multipart uploads for large reports

## Troubleshooting

### Common Issues

1. **Template Not Found**
   ```
   Error: Could not download template
   Solution: Verify template exists in S3 bucket
   ```

2. **PDF Generation Fails**
   ```
   Error: wkhtmltopdf not found
   Solution: Ensure wkhtmltopdf layer is attached
   ```

3. **Bedrock Timeout**
   ```
   Error: Bedrock invocation timeout
   Solution: Falls back to template-based generation
   ```

### Debug Commands

```bash
# Check S3 bucket contents
aws s3 ls s3://msa-audit-reports-{account}-{region}/reports/

# View Lambda logs
aws logs tail /aws/lambda/report-lambda --follow

# Test template download
aws s3 cp s3://msa-audit-templates-{account}-{region}/XXXI_Template.xlsx ./
```

## Future Enhancements

### Planned Features

1. **Custom Templates**: Support for multiple template formats
2. **Interactive Reports**: Web-based dashboard integration
3. **Automated Distribution**: Email delivery of reports
4. **Advanced Analytics**: Trend analysis across multiple invoices
5. **Multi-language Support**: Internationalization for global use

### API Extensions

```python
# Future API endpoints
POST /reports/generate
GET /reports/{report_id}
GET /reports/{report_id}/download/{format}
POST /reports/batch
```

## Related Documentation

- [Architecture Overview](architecture.md)
- [Agent Setup](agent_setup.md)
- [Comparison Component](comparison_component.md)
- [Data Extraction](data_extraction.md)
- [Deployment Guide](deployment.md)
