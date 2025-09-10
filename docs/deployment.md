# MSA Invoice Auditing System - Deployment Guide

## Overview

This guide provides comprehensive instructions for deploying the complete MSA (Master Services Agreement) Invoice Auditing System using AWS CDK. The system now supports both **Full Stack** and **Modular** deployment modes.

## System Architecture

The system supports two deployment architectures:

### Full Stack Deployment (Recommended)
- **MSAInvoiceAuditFullStack** - Complete integrated deployment with all components
  - S3 buckets (ingestion, reports, templates, knowledge base)
  - Lambda functions (extraction, agent, comparison, report)
  - Step Functions workflow orchestration
  - Bedrock Agent with Knowledge Base and OpenSearch
  - DynamoDB table for MSA rates
  - SageMaker endpoint for anomaly detection
  - Streamlit UI on AWS App Runner
  - Complete IAM roles and cross-service integrations

### Modular Deployment (Development)
- **MSAInvoiceIngestionStack** - File processing and data extraction
- **MSAInvoiceAuditAgentStack** - AI analysis, comparison, and report generation
- **MSAInvoiceUIStack** - Streamlit web interface

## Prerequisites

### 1. AWS Environment Setup

```bash
# Install AWS CLI
pip install awscli

# Configure AWS credentials
aws configure

# Install AWS CDK
npm install -g aws-cdk

# Bootstrap CDK (one-time setup per account/region)
cdk bootstrap
```

### 2. Python Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. wkhtmltopdf Binary Setup

The report generation system requires wkhtmltopdf for PDF conversion:

```bash
# Navigate to the wkhtmltopdf layer directory
cd lambda/layers/wkhtmltopdf

# Download and extract wkhtmltopdf for Amazon Linux 2
wget https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6-1/wkhtmltox-0.12.6-1.amazonlinux2.x86_64.rpm
rpm2cpio wkhtmltox-0.12.6-1.amazonlinux2.x86_64.rpm | cpio -idmv
cp usr/local/bin/wkhtmltopdf ./bin/
chmod +x bin/wkhtmltopdf

# Clean up
rm -rf usr wkhtmltox-0.12.6-1.amazonlinux2.x86_64.rpm
```

## Deployment Steps

### 1. Set CDK Context

Create or update `cdk.json` with your account and region:

```json
{
  "app": "python app.py",
  "context": {
    "account": "123456789012",
    "region": "us-east-1",
    "deployment": "full"
  }
}
```

### 2. Choose Deployment Mode

#### Full Stack Deployment (Recommended for Production)

```bash
# Synthesize CloudFormation templates (optional - for review)
cdk synth --context deployment=full

# Deploy the complete integrated stack
cdk deploy MSAInvoiceAuditFullStack --context deployment=full

# Or use default (full is now default)
cdk deploy MSAInvoiceAuditFullStack
```

#### Modular Deployment (Development/Testing)

```bash
# Synthesize modular stacks
cdk synth --context deployment=modular

# Deploy all modular stacks
cdk deploy --all --context deployment=modular

# Or deploy individually with dependencies
cdk deploy MSAInvoiceIngestionStack --context deployment=modular
cdk deploy MSAInvoiceAuditAgentStack --context deployment=modular
cdk deploy MSAInvoiceUIStack --context deployment=modular
```

### 3. Verify Deployment

#### For Full Stack Deployment

```bash
# List deployed stacks
cdk list

# Check full stack outputs
aws cloudformation describe-stacks --stack-name MSAInvoiceAuditFullStack

# Get key resource information
aws cloudformation describe-stacks --stack-name MSAInvoiceAuditFullStack \
  --query 'Stacks[0].Outputs[?contains(OutputKey, `Bucket`) || contains(OutputKey, `Url`)].{Key:OutputKey,Value:OutputValue}' \
  --output table
```

#### For Modular Deployment

```bash
# List deployed stacks
cdk list

# Check individual stack outputs
aws cloudformation describe-stacks --stack-name MSAInvoiceIngestionStack
aws cloudformation describe-stacks --stack-name MSAInvoiceAuditAgentStack
aws cloudformation describe-stacks --stack-name MSAInvoiceUIStack
```

## Post-Deployment Configuration

### 1. Upload MSA Rate Standards

Upload your MSA rate standards to the DynamoDB table:

```python
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('msa-rates-table')

# Example rate data
rates = [
    {
        'rate_id': 'RS_LABOR_STANDARD',
        'category': 'Labor',
        'subcategory': 'RS Labor',
        'rate_per_hour': 85.00,
        'effective_date': '2024-01-01'
    },
    {
        'rate_id': 'EQUIPMENT_RENTAL',
        'category': 'Equipment',
        'subcategory': 'Heavy Machinery',
        'rate_per_hour': 150.00,
        'effective_date': '2024-01-01'
    }
]

for rate in rates:
    table.put_item(Item=rate)
```

### 2. Upload Excel Templates

Upload your XXXI Excel templates to the S3 templates bucket:

#### For Full Stack Deployment

```bash
# Get the templates bucket name from full stack outputs
TEMPLATES_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name MSAInvoiceAuditFullStack \
  --query 'Stacks[0].Outputs[?OutputKey==`TemplatesBucketName`].OutputValue' \
  --output text)

# Upload your Excel template
aws s3 cp your-template.xlsx s3://$TEMPLATES_BUCKET/templates/
```

#### For Modular Deployment

```bash
# Get the templates bucket name from agent stack outputs
TEMPLATES_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name MSAInvoiceAuditAgentStack \
  --query 'Stacks[0].Outputs[?OutputKey==`TemplatesBucketName`].OutputValue' \
  --output text)

# Upload your Excel template
aws s3 cp your-template.xlsx s3://$TEMPLATES_BUCKET/templates/
```

### 3. Configure Bedrock Agent Knowledge Base

The system automatically creates a Knowledge Base. Upload your MSA documents:

#### For Full Stack Deployment

```bash
# Get the knowledge base bucket name
KB_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name MSAInvoiceAuditFullStack \
  --query 'Stacks[0].Outputs[?OutputKey==`KnowledgeBaseBucketName`].OutputValue' \
  --output text)

# Upload MSA documents
aws s3 cp msa-documents/ s3://$KB_BUCKET/ --recursive
```

#### For Modular Deployment

```bash
# Get the knowledge base bucket name
KB_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name MSAInvoiceAuditAgentStack \
  --query 'Stacks[0].Outputs[?OutputKey==`KnowledgeBaseBucketName`].OutputValue' \
  --output text)

# Upload MSA documents
aws s3 cp msa-documents/ s3://$KB_BUCKET/ --recursive
```

### 4. Access the Web Interface

#### For Full Stack Deployment

```bash
# Get the Streamlit UI URL
UI_URL=$(aws cloudformation describe-stacks \
  --stack-name MSAInvoiceAuditFullStack \
  --query 'Stacks[0].Outputs[?OutputKey==`StreamlitUIUrl`].OutputValue' \
  --output text)

echo "Access the MSA Invoice Auditing System at: $UI_URL"
```

#### For Modular Deployment

```bash
# Get the Streamlit UI URL
UI_URL=$(aws cloudformation describe-stacks \
  --stack-name MSAInvoiceUIStack \
  --query 'Stacks[0].Outputs[?OutputKey==`StreamlitUIUrl`].OutputValue' \
  --output text)

echo "Access the MSA Invoice Auditing System at: $UI_URL"
```

## Testing the System

### 1. Upload Test Invoice

#### For Full Stack Deployment

```bash
# Get the ingestion bucket name
INGESTION_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name MSAInvoiceAuditFullStack \
  --query 'Stacks[0].Outputs[?OutputKey==`IngestionBucketName`].OutputValue' \
  --output text)

# Upload a test invoice
aws s3 cp test-invoice.pdf s3://$INGESTION_BUCKET/
```

#### For Modular Deployment

```bash
# Get the ingestion bucket name
INGESTION_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name MSAInvoiceIngestionStack \
  --query 'Stacks[0].Outputs[?OutputKey==`IngestionBucketName`].OutputValue' \
  --output text)

# Upload a test invoice
aws s3 cp test-invoice.pdf s3://$INGESTION_BUCKET/
```

#### Using the Web Interface

1. Access the Streamlit UI using the URL from step 4 above
2. Use the file uploader to select your PDF or Excel invoice
3. Enter any specific queries (e.g., "Compare to MSA")
4. Click "Start Audit" to begin processing
5. Monitor progress and download results when complete

### 2. Monitor Execution

```bash
# Get Step Functions execution ARN from CloudWatch logs or AWS Console
# Monitor the execution
aws stepfunctions describe-execution --execution-arn <execution-arn>
```

### 3. Check Results

#### For Full Stack Deployment

```bash
# Get the reports bucket name
REPORTS_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name MSAInvoiceAuditFullStack \
  --query 'Stacks[0].Outputs[?OutputKey==`ReportsBucketName`].OutputValue' \
  --output text)

# List generated reports
aws s3 ls s3://$REPORTS_BUCKET/reports/
```

#### For Modular Deployment

```bash
# Get the reports bucket name
REPORTS_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name MSAInvoiceAuditAgentStack \
  --query 'Stacks[0].Outputs[?OutputKey==`ReportsBucketName`].OutputValue' \
  --output text)

# List generated reports
aws s3 ls s3://$REPORTS_BUCKET/reports/
```

#### Via Web Interface

Results are automatically displayed in the Streamlit UI:
- **Flags Dashboard**: Shows discrepancies like "Overcharge: $375 on RS labor"
- **Report Download**: Access filled Excel templates and PDF reports
- **Analysis Summary**: View detailed findings and recommendations

## Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test modules
python -m pytest tests/test_deployment.py -v  # New deployment tests
python -m pytest tests/test_report.py -v
python -m pytest tests/test_agent.py -v
python -m pytest tests/test_comparison.py -v
python -m pytest tests/test_ui.py -v

# Run deployment-specific tests
python -m pytest tests/test_deployment.py::TestCDKSynthesis -v
python -m pytest tests/test_deployment.py::TestDeploymentSimulation -v

# Run with coverage
python -m pytest tests/ --cov=lambda --cov=infrastructure --cov=ui --cov-report=html
```

## Monitoring and Logging

### CloudWatch Logs

- `/aws/lambda/extraction-lambda`
- `/aws/lambda/agent-lambda`
- `/aws/lambda/comparison-lambda`
- `/aws/lambda/report-lambda`
- `/aws/stepfunctions/MSAInvoiceAuditWorkflow`

### CloudWatch Metrics

Monitor key metrics:
- Lambda function duration and errors
- Step Functions execution success/failure rates
- S3 bucket object counts
- DynamoDB read/write capacity

### Alarms

Set up CloudWatch alarms for:
- Lambda function errors
- Step Functions failed executions
- High DynamoDB throttling
- S3 bucket access errors

## Troubleshooting

### Common Issues

1. **wkhtmltopdf not found**
   - Ensure the binary is properly placed in `lambda/layers/wkhtmltopdf/bin/`
   - Verify executable permissions: `chmod +x bin/wkhtmltopdf`

2. **Bedrock model access denied**
   - Enable model access in Bedrock console
   - Verify IAM permissions for Bedrock service

3. **Knowledge Base sync failures**
   - Check S3 bucket permissions
   - Verify OpenSearch domain is accessible

4. **Step Functions timeout**
   - Increase Lambda function timeout settings
   - Check for large file processing issues

### Logs Analysis

```bash
# View recent Lambda logs
aws logs tail /aws/lambda/report-lambda --follow

# Search for errors
aws logs filter-log-events \
  --log-group-name /aws/lambda/report-lambda \
  --filter-pattern "ERROR"
```

## Cleanup

### For Full Stack Deployment

```bash
# Delete the full stack
cdk destroy MSAInvoiceAuditFullStack

# Clean up S3 buckets if needed (CDK may retain them)
aws s3 rm s3://<ingestion-bucket-name> --recursive
aws s3 rm s3://<reports-bucket-name> --recursive
aws s3 rm s3://<templates-bucket-name> --recursive
aws s3 rm s3://<knowledge-base-bucket-name> --recursive
aws s3 rb s3://<bucket-name>  # Repeat for each bucket
```

### For Modular Deployment

```bash
# Delete stacks (in reverse dependency order)
cdk destroy MSAInvoiceUIStack
cdk destroy MSAInvoiceAuditAgentStack
cdk destroy MSAInvoiceIngestionStack

# Clean up S3 buckets if needed (CDK may retain them)
aws s3 rm s3://<bucket-name> --recursive
aws s3 rb s3://<bucket-name>
```

## Security Considerations

1. **IAM Roles**: All Lambda functions use least-privilege IAM roles
2. **S3 Encryption**: All S3 buckets use server-side encryption
3. **VPC**: Consider deploying in VPC for additional network security
4. **Secrets**: Use AWS Secrets Manager for sensitive configuration
5. **Access Logging**: Enable CloudTrail for API call auditing

## Cost Optimization

1. **S3 Lifecycle**: Configure lifecycle policies for old reports
2. **Lambda Memory**: Optimize memory allocation based on usage patterns
3. **DynamoDB**: Use on-demand billing for variable workloads
4. **CloudWatch**: Set log retention periods appropriately

## Support

For issues and questions:
1. Check CloudWatch logs for error details
2. Review the troubleshooting section above
3. Consult AWS documentation for service-specific issues
4. Contact the development team for application-specific problems
