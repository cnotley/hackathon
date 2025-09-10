# Comparison and Discrepancy Flagging Component

## Overview

The comparison and discrepancy flagging component is a critical part of the invoice auditing system that analyzes extracted invoice data against Master Services Agreement (MSA) standards to identify discrepancies, flag violations, and calculate potential savings.

## Architecture

### Components

1. **Comparison Lambda (`comparison-lambda`)**
   - Main processing function for discrepancy analysis
   - Integrates with DynamoDB, Bedrock, and SageMaker
   - Handles rate variance detection, overtime violations, and anomaly detection

2. **MSA Rates Comparator**
   - Manages DynamoDB lookups for standard rates
   - Implements caching for performance optimization
   - Calculates rate variances and overtime thresholds

3. **Bedrock Analyzer**
   - Uses Claude 3.5 Sonnet for intelligent discrepancy analysis
   - Processes chunked context from Knowledge Base
   - Generates detailed explanations for flagged items

4. **Anomaly Detector**
   - Primary: SageMaker JumpStart isolation forest model
   - Fallback: Statistical z-score analysis
   - Identifies unusual cost patterns and outliers

5. **Discrepancy Flagging Engine**
   - Orchestrates complete analysis pipeline
   - Generates checklist flags and savings calculations
   - Produces structured JSON output

## Key Features

### Rate Variance Detection
- Compares charged rates against MSA standard rates
- Flags variances exceeding 5% threshold
- Calculates potential savings from rate corrections
- Supports different labor types (RS, US, SS, SU, EN)

### Overtime Violation Detection
- Monitors weekly hours against thresholds (default: 40 hours)
- Identifies excessive overtime patterns
- Generates checklist flags for documentation requirements
- Supports labor-type-specific thresholds

### Anomaly Detection
- **SageMaker Integration**: Uses isolation forest for outlier detection
- **Statistical Fallback**: Z-score analysis when SageMaker unavailable
- Identifies unusual cost items (e.g., "$6,313 respirators")
- Configurable sensitivity thresholds

### Duplicate Detection
- Identifies duplicate labor entries for same worker/week
- Flags duplicate material purchases
- Prevents double-billing scenarios

## Configuration

### Environment Variables

```bash
MSA_RATES_TABLE=msa-rates                    # DynamoDB table name
KNOWLEDGE_BASE_ID=kb-xxxxx                   # Bedrock Knowledge Base ID
SAGEMAKER_ENDPOINT=invoice-anomaly-detection # SageMaker endpoint name
BEDROCK_MODEL_ID=anthropic.claude-3-5-sonnet-20241022-v2:0
VARIANCE_THRESHOLD=0.05                      # 5% rate variance threshold
OVERTIME_THRESHOLD=40.0                      # 40 hours/week overtime threshold
LOG_LEVEL=INFO
```

### DynamoDB Schema

**MSA Rates Table (`msa-rates`)**
```json
{
  "labor_type": "RS",           // Partition key
  "location": "default",        // Sort key
  "standard_rate": 70.00,       // Standard hourly rate
  "description": "Regular Skilled Labor"
}
```

**Overtime Rules**
```json
{
  "labor_type": "default",
  "location": "overtime_rules",
  "weekly_threshold": 40.0,
  "description": "Standard overtime threshold"
}
```

## Input/Output Format

### Input (from Extraction Lambda)
```json
{
  "invoice_number": "INV-2024-001",
  "vendor": "ABC Construction",
  "total_amount": 76160.00,
  "labor_entries": [
    {
      "worker_name": "John Smith",
      "labor_type": "RS",
      "hours": 45.0,
      "rate": 73.50,
      "total": 3307.50,
      "week": "2024-W01"
    }
  ],
  "material_entries": [
    {
      "description": "Safety respirators",
      "quantity": 10,
      "unit_price": 631.30,
      "total": 6313.00
    }
  ]
}
```

### Output (Discrepancy Analysis)
```json
{
  "rate_variances": [
    {
      "worker": "John Smith",
      "labor_type": "RS",
      "charged_rate": 73.50,
      "standard_rate": 70.00,
      "variance_percent": 5.0,
      "discrepancy": "RS rate overcharge $157.50",
      "savings": 157.50,
      "flag": "Rate variance exceeds 5% threshold"
    }
  ],
  "overtime_violations": [
    {
      "worker": "John Smith",
      "hours": 45.0,
      "threshold": 40.0,
      "excess_hours": 5.0,
      "flag": "Overtime: support with time sheets"
    }
  ],
  "anomalies": [
    {
      "item": "Safety respirators",
      "amount": 6313.00,
      "anomaly_score": 0.8,
      "z_score": 2.5,
      "flag": "Statistical anomaly detected"
    }
  ],
  "duplicates": [],
  "total_savings": 157.50,
  "summary": {
    "total_discrepancies": 2,
    "high_priority_flags": 1,
    "potential_savings": 157.50
  }
}
```

## Integration with Step Functions

The comparison Lambda is integrated into the invoice processing workflow:

1. **Extraction** → Extract data from PDF/Excel
2. **Comparison** → Analyze discrepancies (this component)
3. **Agent Analysis** → Generate final audit report

### Step Functions Payload
```json
{
  "extraction_data": "$.extraction_result.Payload.extracted_data",
  "bucket": "$.bucket",
  "key": "$.key"
}
```

## Performance Considerations

### Caching Strategy
- **DynamoDB**: Rate lookups cached for 5 minutes
- **Bedrock**: Context chunks cached per session
- **SageMaker**: Model predictions cached for similar data patterns

### Timeout Configuration
- **Lambda Timeout**: 15 minutes (handles large invoices)
- **Memory**: 1024 MB (supports pandas/numpy operations)
- **Concurrent Executions**: Configurable based on load

### Cost Optimization
- **DynamoDB**: On-demand billing for variable workloads
- **SageMaker**: Single t2.medium instance with auto-scaling
- **Bedrock**: Pay-per-token model usage

## Error Handling

### Graceful Degradation
1. **SageMaker Unavailable**: Falls back to statistical anomaly detection
2. **Bedrock Timeout**: Uses cached analysis patterns
3. **DynamoDB Throttling**: Implements exponential backoff
4. **Invalid Data**: Returns partial analysis with error flags

### Error Response Format
```json
{
  "statusCode": 500,
  "body": {
    "error": "Service unavailable",
    "partial_results": {...},
    "retry_recommended": true
  }
}
```

## Testing

### Unit Tests
- **MSA Rates Comparator**: Rate lookups and variance calculations
- **Bedrock Analyzer**: Mock API responses and context processing
- **Anomaly Detector**: Statistical analysis and SageMaker integration
- **End-to-End**: Complete pipeline with sample invoice data

### Test Data
```python
# Sample test case expecting specific flags
sample_data = {
    "labor_entries": [
        {
            "worker_name": "John Smith",
            "labor_type": "RS", 
            "hours": 45.0,        # Overtime violation
            "rate": 73.50,        # 5% over standard $70.00
            "total": 3307.50
        }
    ]
}

expected_flags = [
    "Rate variance exceeds 5% threshold",
    "Overtime: support with time sheets"
]
```

### Running Tests
```bash
# Run comparison component tests
pytest tests/test_comparison.py -v

# Run with coverage
pytest tests/test_comparison.py --cov=lambda.comparison_lambda

# Run integration tests
pytest tests/test_comparison.py::TestComparisonLambda::test_lambda_handler
```

## Monitoring and Logging

### CloudWatch Metrics
- **Invocation Count**: Number of comparison analyses
- **Error Rate**: Failed analysis percentage
- **Duration**: Processing time per invoice
- **Savings Identified**: Total potential savings flagged

### Log Structure
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "invoice_id": "INV-2024-001",
  "discrepancies_found": 3,
  "total_savings": 157.50,
  "processing_time_ms": 2500
}
```

### Alarms
- **High Error Rate**: >5% failures in 5-minute window
- **Long Duration**: >10 minutes processing time
- **SageMaker Failures**: Endpoint unavailability

## Deployment

### CDK Infrastructure
The comparison component is deployed as part of the `InvoiceAuditAgentStack`:

```python
# Comparison Lambda
self.comparison_lambda = self._create_comparison_lambda()

# SageMaker Endpoint
self.sagemaker_endpoint = self._create_sagemaker_endpoint()

# Step Functions Integration
self.step_function = self._create_step_function()
```

### Dependencies
- **boto3**: AWS SDK for Python
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing
- **scikit-learn**: Statistical analysis and anomaly detection

### Deployment Commands
```bash
# Deploy infrastructure
cdk deploy InvoiceAuditAgentStack

# Update Lambda code only
aws lambda update-function-code \
  --function-name comparison-lambda \
  --zip-file fileb://lambda-package.zip
```

## Security Considerations

### IAM Permissions
- **DynamoDB**: Read access to MSA rates table
- **Bedrock**: Model invocation and Knowledge Base retrieval
- **SageMaker**: Endpoint invocation permissions
- **S3**: Read/write access to processing bucket

### Data Protection
- **Encryption**: All data encrypted in transit and at rest
- **Access Logging**: All API calls logged to CloudTrail
- **Network Security**: VPC endpoints for service communication

## Troubleshooting

### Common Issues

1. **Rate Variance False Positives**
   - Check MSA rates table for correct standard rates
   - Verify labor type mappings in extraction data
   - Review variance threshold configuration

2. **SageMaker Endpoint Errors**
   - Verify endpoint is in service
   - Check model data availability
   - Review instance capacity and scaling

3. **Bedrock Timeout Issues**
   - Reduce context chunk size
   - Implement request batching
   - Use cached analysis for similar patterns

### Debug Commands
```bash
# Check Lambda logs
aws logs tail /aws/lambda/comparison-lambda --follow

# Test SageMaker endpoint
aws sagemaker-runtime invoke-endpoint \
  --endpoint-name invoice-anomaly-detection \
  --body '[[1000, 1100, 6313]]' \
  output.json

# Query MSA rates
aws dynamodb get-item \
  --table-name msa-rates \
  --key '{"labor_type":{"S":"RS"},"location":{"S":"default"}}'
```

## Future Enhancements

### Planned Features
1. **Machine Learning Improvements**
   - Custom anomaly detection models
   - Historical pattern learning
   - Predictive cost analysis

2. **Enhanced Analytics**
   - Trend analysis across invoices
   - Vendor performance scoring
   - Cost optimization recommendations

3. **Integration Expansions**
   - Real-time processing capabilities
   - External MSA system integration
   - Advanced reporting dashboards

### Performance Optimizations
- **Parallel Processing**: Multi-threaded analysis for large invoices
- **Caching Enhancements**: Redis integration for cross-invocation caching
- **Model Optimization**: Quantized models for faster inference
