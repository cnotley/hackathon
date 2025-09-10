# Invoice Auditing File Ingestion Module

A comprehensive AI-powered serverless AWS solution for automated invoice file ingestion, processing, and intelligent auditing for Master Services Agreement (MSA) compliance. This system combines advanced document processing with AI agents to provide automated invoice auditing capabilities.

## 🏗️ Complete Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   File Upload   │───▶│   S3 Bucket      │───▶│  S3 Event       │
│   (External)    │    │ audit-files-     │    │  Notification   │
│                 │    │ bucket           │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Ingestion Step  │◀───│  Ingestion       │◀───│  Lambda         │
│ Functions       │    │  Lambda          │    │  Trigger        │
│ (Basic Workflow)│    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                                               │
         ▼                                               ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ AI Processing   │◀───│  Extraction      │    │  Comparison     │
│ Step Functions  │    │  Lambda          │    │  Lambda         │
│ (Full Workflow) │    │ (Textract+AI)    │    │ (MSA Analysis)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Bedrock       │    │   Knowledge      │    │   DynamoDB      │
│   Agent         │◀───│   Base           │    │   MSA Rates     │
│ (AI Analysis)   │    │ (OpenSearch)     │    │   Table         │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  SageMaker      │    │   Audit Report   │    │   CloudWatch    │
│  Anomaly        │    │   Generation     │    │   Monitoring    │
│  Detection      │    │   (S3 Storage)   │    │   & Logging     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 🚀 Features

### Core Processing Capabilities
- **Multi-format Support**: PDF, Excel (.xlsx, .xls), and image files (.png, .jpg, .jpeg)
- **Event-driven Processing**: Automatic triggering via S3 event notifications
- **Dual Workflow Architecture**: Basic ingestion + AI-powered analysis workflows
- **Advanced Document Processing**: Amazon Textract, Bedrock, and Comprehend integration

### AI-Powered Invoice Auditing
- **Intelligent Data Extraction**: Semantic mapping and vendor terminology normalization
- **MSA Compliance Checking**: Automated comparison against Master Services Agreement standards
- **Labor Rate Analysis**: 5% variance threshold detection and overtime violation flagging
- **AI Agent Analysis**: Bedrock Agent with Claude 3.5 Sonnet for intelligent audit reports
- **Anomaly Detection**: SageMaker-powered statistical outlier identification
- **Knowledge Base Integration**: Vector-based storage and retrieval for audit context

### Enterprise Features
- **Security First**: Encryption at rest and in transit, IAM least privilege access
- **Comprehensive Logging**: Structured audit trails for compliance
- **Cost Optimized**: Serverless architecture with lifecycle policies
- **Scalable**: Auto-scaling Lambda functions and unlimited S3 storage
- **Monitoring & Observability**: CloudWatch metrics, alarms, and dashboards

## 📋 Requirements

### Prerequisites

- **AWS Account** with appropriate permissions
- **AWS CLI** v2.x configured with credentials
- **Node.js** 18.x or later (for CDK)
- **Python** 3.11 or later
- **AWS CDK** 2.100.0 or later

#### Local CDK toolchain setup

```bash
# macOS (Homebrew)
brew install node

# Windows (PowerShell / winget)
winget install OpenJS.NodeJS.LTS

# Linux (Debian/Ubuntu)
sudo apt-get update && sudo apt-get install -y nodejs npm

# Install the AWS CDK CLI globally (all platforms)
npm install -g aws-cdk

# Python dependencies (inside your virtual environment)
pip install -r requirements.txt

# Verify the CLI toolchain
cdk --version
```

Configure AWS credentials and default CDK environment variables:

```bash
aws configure
export CDK_DEFAULT_ACCOUNT="YOUR_AWS_ACCOUNT_ID"
export CDK_DEFAULT_REGION="us-east-1"
```

The repository expects the CDK entry point to be defined in `cdk.json` as:

```json
{
  "app": "python app.py"
}
```

After installing the prerequisites, validate the toolchain from the project root:

```bash
cdk ls
cdk synth > synth-output.yaml
```

### Supported File Types

| Format | Extensions | Max Size | Priority |
|--------|------------|----------|----------|
| PDF | `.pdf` | 100MB | High |
| Excel | `.xlsx`, `.xls` | 100MB | Medium |
| Images | `.png`, `.jpg`, `.jpeg` | 100MB | Low |

## 🛠️ Installation

### 1. Clone and Setup

```bash
git clone <repository-url>
cd invoice-auditing-ingestion

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Install CDK (if not already installed)

```bash
npm install -g aws-cdk@2.100.0
```

### 3. Configure AWS Credentials

```bash
aws configure
# Enter your AWS Access Key ID, Secret Access Key, and default region
```

### 4. Bootstrap CDK (one-time setup per region)

```bash
cdk bootstrap aws://ACCOUNT-NUMBER/REGION
```

## 🚀 Quick Start

### Automated Deployment

Use the provided deployment script for a complete setup:

```bash
chmod +x deploy.sh
./deploy.sh
```

### Manual Deployment

```bash
# Synthesize CloudFormation template
cdk synth

# Deploy the stack
cdk deploy
```

### Verify Deployment

```bash
# Check deployed resources
aws cloudformation describe-stacks --stack-name InvoiceIngestionStack

# Test file upload
echo "Test invoice content" > test-invoice.pdf
aws s3 cp test-invoice.pdf s3://audit-files-bucket/test/
```

## 🔄 Complete Workflow

### 1. File Upload and Initial Processing
When an invoice file is uploaded to the S3 bucket, the following process begins:

```
File Upload → S3 Event → Ingestion Lambda → Basic Step Functions
```

**Basic Ingestion Workflow (`invoice-audit-workflow`)**:
1. **Validate File**: Checks file type, size (max 100MB), and format
2. **Extract Basic Metadata**: File name, size, upload timestamp, content type
3. **Process File**: Apply processing tags and prepare for AI analysis

### 2. AI-Powered Analysis Pipeline
After basic processing, the AI analysis workflow begins:

```
AI Step Functions → Extraction → Comparison → Agent Analysis
```

**AI Processing Workflow (`invoice-processing-workflow`)**:

#### Step 1: Advanced Data Extraction (`extraction-lambda`)
- **Amazon Textract**: OCR and document structure analysis
  - Synchronous processing for files <500KB
  - Asynchronous processing for larger files
  - Confidence-based filtering (80% threshold)
- **Amazon Bedrock**: Semantic mapping and vendor terminology normalization
  - Claude 3.5 Sonnet for intelligent field mapping
  - Handles variations like "Rate" → "unit_price", "Consumables" → "materials"
- **Amazon Comprehend**: Entity recognition for labor types and personnel
- **Pandas**: Excel file processing with statistical analysis

#### Step 2: MSA Compliance Analysis (`comparison-lambda`)
- **Rate Comparison**: Compare extracted labor rates against DynamoDB MSA standards
  - RS (Regular Skilled): $70.00/hour
  - US (Unskilled Supervisor): $85.00/hour
  - SS (Skilled Supervisor): $95.00/hour
  - SU (Senior Supervisor): $110.00/hour
  - EN (Engineer): $125.00/hour
- **Variance Detection**: Flag rates exceeding 5% variance threshold
- **Overtime Analysis**: Detect violations >40 hours/week per worker
- **Anomaly Detection**: SageMaker Isolation Forest model for statistical outliers

#### Step 3: AI Agent Analysis (`agent-lambda` + Bedrock Agent)
- **Knowledge Base Query**: Vector search through processed invoice data
- **Intelligent Analysis**: Claude 3.5 Sonnet agent provides:
  - Detailed compliance assessment
  - Labor classification validation
  - Cost variance explanations
  - Actionable recommendations
- **Report Generation**: Comprehensive audit report with findings and suggestions

### 3. Knowledge Base Integration
Processed data is automatically ingested into the Knowledge Base:

```
Extracted Data → Semantic Chunking → Vector Embeddings → OpenSearch Storage
```

- **Chunking Strategy**: Intelligent segmentation by document type and content
- **Vector Embeddings**: Amazon Titan Embed Text v1 for semantic search
- **Storage**: OpenSearch domain with encryption and access controls

### 4. Audit Report Output
Final audit reports include:

- **Executive Summary**: High-level findings and total cost impact
- **Labor Rate Analysis**: Detailed breakdown of rate variances
- **Overtime Violations**: Workers exceeding hour thresholds
- **Classification Issues**: Incorrect labor type assignments
- **Anomaly Flags**: Statistical outliers requiring investigation
- **Recommendations**: Specific actions to address findings

## 📖 Usage

### File Upload Methods

#### 1. AWS CLI
```bash
aws s3 cp invoice.pdf s3://audit-files-bucket/invoices/
```

#### 2. AWS SDK (Python)
```python
import boto3

s3_client = boto3.client('s3')
s3_client.upload_file('invoice.pdf', 'audit-files-bucket', 'invoices/invoice.pdf')
```

#### 3. Pre-signed URLs
```python
import boto3

s3_client = boto3.client('s3')
url = s3_client.generate_presigned_url(
    'put_object',
    Params={'Bucket': 'audit-files-bucket', 'Key': 'invoices/invoice.pdf'},
    ExpiresIn=3600
)
```

### Monitoring Processing

#### CloudWatch Logs
```bash
# View Lambda logs
aws logs tail /aws/lambda/ingestion-lambda --follow

# View Step Functions logs
aws logs tail /aws/stepfunctions/invoice-audit-workflow --follow
```

#### Step Functions Console
```bash
# List recent executions
aws stepfunctions list-executions \
  --state-machine-arn $(aws stepfunctions list-state-machines \
  --query 'stateMachines[?name==`invoice-audit-workflow`].stateMachineArn' \
  --output text)
```

## 🔧 Configuration

### Environment Variables

The Lambda function uses the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `BUCKET_NAME` | S3 bucket name | `audit-files-bucket` |
| `STATE_MACHINE_ARN` | Step Functions ARN | Auto-generated |
| `LOG_LEVEL` | Logging level | `INFO` |
| `MAX_FILE_SIZE` | Maximum file size (bytes) | `104857600` (100MB) |

### Customization

#### Modify File Size Limits
Edit `lambda/ingestion/handler.py`:
```python
MAX_FILE_SIZE = 200 * 1024 * 1024  # 200MB
```

#### Add New File Types
Update `SUPPORTED_EXTENSIONS` in `handler.py`:
```python
SUPPORTED_EXTENSIONS = {
    '.pdf': 'application/pdf',
    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    # Add new types here
}
```

#### Customize Processing Logic
Modify the Step Functions workflow in `step_functions/audit_workflow.json` or update the CDK stack definition.

## 🧪 Testing

### Run Unit Tests

```bash
# Install test dependencies
pip install pytest pytest-cov moto

# Run all tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=lambda/ingestion --cov-report=html
```

### Integration Testing

```bash
# Test with real AWS resources (requires deployment)
python -m pytest tests/test_integration.py -v
```

### Load Testing

```bash
# Upload multiple test files
for i in {1..10}; do
  echo "Test content $i" > test-file-$i.pdf
  aws s3 cp test-file-$i.pdf s3://audit-files-bucket/load-test/
done
```

## 📊 Monitoring and Observability

### CloudWatch Metrics

Key metrics to monitor:

- **Lambda Duration**: Function execution time
- **Lambda Errors**: Function error rate
- **Step Functions Executions**: Workflow success/failure rates
- **S3 Requests**: Upload and processing volumes

### Alarms Setup

```bash
# Lambda error rate alarm
aws cloudwatch put-metric-alarm \
  --alarm-name "IngestionLambdaErrors" \
  --alarm-description "Lambda function error rate" \
  --metric-name Errors \
  --namespace AWS/Lambda \
  --statistic Sum \
  --period 300 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --dimensions Name=FunctionName,Value=ingestion-lambda
```

### Dashboard Creation

Create a CloudWatch dashboard to monitor system health:

```bash
aws cloudwatch put-dashboard \
  --dashboard-name "InvoiceIngestionDashboard" \
  --dashboard-body file://monitoring/dashboard.json
```

## 🔒 Security

### Data Protection

- **Encryption at Rest**: S3 server-side encryption (AES-256)
- **Encryption in Transit**: HTTPS/TLS for all communications
- **Access Control**: IAM roles with least privilege principles
- **Network Security**: VPC endpoints for AWS service communication

### Compliance Features

- **Audit Logging**: Comprehensive CloudTrail integration
- **Data Retention**: Configurable lifecycle policies
- **Access Monitoring**: CloudWatch logs for all operations
- **Compliance Reports**: Automated compliance checking

### Security Best Practices

1. **Regular Updates**: Keep dependencies and runtime versions current
2. **Access Reviews**: Periodic IAM permission audits
3. **Monitoring**: Real-time security event monitoring
4. **Backup**: Cross-region replication for critical data

## 💰 Cost Optimization

### Pricing Components

- **S3 Storage**: ~$0.023/GB/month (Standard tier)
- **Lambda Execution**: $0.20/1M requests + $0.0000166667/GB-second
- **Step Functions**: $0.025/1K state transitions
- **CloudWatch**: $0.50/million API requests

### Cost Optimization Strategies

1. **S3 Lifecycle Policies**: Automatic transition to cheaper storage classes
2. **Lambda Optimization**: Right-size memory allocation
3. **Log Retention**: Set appropriate CloudWatch log retention periods
4. **Reserved Capacity**: Use for predictable workloads

### Estimated Monthly Costs

| Volume | Files/Month | Estimated Cost |
|--------|-------------|----------------|
| Low | 1,000 | $10-20 |
| Medium | 10,000 | $50-100 |
| High | 100,000 | $200-500 |

*Costs vary based on file sizes, processing complexity, and retention policies.*

## 🚨 Troubleshooting

### Common Issues

#### 1. Deployment Failures

**Issue**: CDK bootstrap required
```bash
# Solution
cdk bootstrap aws://ACCOUNT-NUMBER/REGION
```

**Issue**: Insufficient permissions
```bash
# Solution: Ensure your AWS user has the necessary IAM permissions
aws iam list-attached-user-policies --user-name YOUR-USERNAME
```

#### 2. Processing Failures

**Issue**: File validation errors
- Check file format and size limits
- Verify S3 object permissions
- Review CloudWatch logs for details

**Issue**: Step Functions timeout
- Increase Lambda timeout settings
- Check for large file processing issues
- Review Step Functions execution history

#### 3. Performance Issues

**Issue**: Slow processing
- Increase Lambda memory allocation
- Check for cold start issues
- Review file size and complexity

### Debug Commands

```bash
# Check Lambda function status
aws lambda get-function --function-name ingestion-lambda

# View recent Step Functions executions
aws stepfunctions list-executions \
  --state-machine-arn YOUR-STATE-MACHINE-ARN \
  --max-items 10

# Check S3 bucket notifications
aws s3api get-bucket-notification-configuration \
  --bucket audit-files-bucket
```

## 🔄 Maintenance

### Regular Tasks

1. **Monitor Costs**: Review AWS billing dashboard monthly
2. **Update Dependencies**: Keep CDK and Lambda runtime versions current
3. **Review Logs**: Check CloudWatch logs for errors or warnings
4. **Performance Tuning**: Optimize based on usage patterns

### Backup and Recovery

```bash
# Export CloudFormation template
aws cloudformation get-template \
  --stack-name InvoiceIngestionStack \
  --template-stage Processed > backup-template.json

# Create cross-region replication (if needed)
aws s3api put-bucket-replication \
  --bucket audit-files-bucket \
  --replication-configuration file://replication-config.json
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run linting
flake8 lambda/ tests/
black lambda/ tests/

# Run type checking
mypy lambda/
```

## 📚 Additional Resources

- [AWS CDK Documentation](https://docs.aws.amazon.com/cdk/)
- [AWS Lambda Best Practices](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html)
- [AWS Step Functions Developer Guide](https://docs.aws.amazon.com/step-functions/latest/dg/)
- [S3 Event Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For support and questions:

1. Check the [troubleshooting section](#-troubleshooting)
2. Review [CloudWatch logs](#monitoring-processing)
3. Open an issue in the repository
4. Contact the development team

---

**Built with ❤️ for secure and scalable invoice processing**
