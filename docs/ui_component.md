# MSA Invoice Auditing System - User Interface Component

## Overview

The MSA Invoice Auditing System includes a comprehensive web-based user interface built with Streamlit and deployed using AWS App Runner. This interface provides an intuitive way for users to upload invoices, trigger analysis workflows, and view results.

## Architecture

### Components

1. **Streamlit Application** (`ui/app.py`)
   - Web-based interface for file uploads and workflow management
   - Real-time status monitoring and report viewing
   - Direct Bedrock Agent interaction for queries

2. **AWS App Runner Deployment** (`infrastructure/ui_stack.py`)
   - Containerized deployment using ECR and App Runner
   - Auto-scaling and health monitoring
   - Integrated with existing AWS infrastructure

3. **Docker Container** (`ui/Dockerfile`)
   - Python 3.11 slim base image
   - Streamlit server configuration
   - Security hardening with non-root user

## Features

### File Upload and Processing
- **Multi-format Support**: PDF, Excel (.xlsx, .xls), and image files (.png, .jpg, .jpeg)
- **S3 Integration**: Direct upload to ingestion bucket with timestamp-based naming
- **Workflow Triggering**: Automatic Step Functions execution initiation
- **Query Support**: Optional analysis queries (e.g., "Compare to MSA", "Check for overcharges")

### Real-time Monitoring
- **Execution Status**: Live monitoring of Step Functions workflow progress
- **Progress Indicators**: Visual feedback for running, succeeded, and failed executions
- **Execution Details**: Comprehensive execution metadata and timing information

### Report Management
- **Multi-format Reports**: Excel, PDF, and Markdown report generation
- **Download Interface**: Direct download links for generated reports
- **Report Metadata**: File size, modification dates, and type information
- **Automatic Discovery**: Reports are automatically linked to uploaded files

### Analysis Results Display
- **Flags Dashboard**: Interactive table showing discrepancies and findings
- **Sample Flags**:
  - Overcharge: "$375.00 on RS Labor rate exceeds MSA"
  - Duplicate: "Safety Gloves" (Count: 2)
  - Savings: "10% of $160,000 in Equipment Rental"
- **Metrics Summary**: Total overcharges, potential savings, compliance scores

### Direct Agent Interaction
- **Bedrock Agent Queries**: Direct communication with the MSA knowledge agent
- **Session Management**: Persistent conversation sessions
- **Knowledge Base Access**: Query MSA standards, rates, and compliance information

## User Interface Layout

### Header Section
- Application title and description
- Navigation and configuration sidebar

### Main Content Area

#### Left Column - Input
1. **File Upload Section**
   - Drag-and-drop file uploader
   - Supported file type indicators
   - Analysis query text input
   - Upload and start analysis button

2. **Direct Agent Query Section**
   - Text area for MSA-related questions
   - Ask Agent button
   - Response display area

#### Right Column - Results
1. **Analysis Status Section**
   - Current execution status
   - Refresh status button
   - Progress indicators
   - Execution details expandable section

2. **Generated Reports Section**
   - List of available reports
   - Download buttons for each report
   - Report metadata (size, date, type)

3. **Analysis Flags Section**
   - Interactive data table with findings
   - Flag types: Overcharge, Duplicate, Savings
   - Summary metrics cards

### Sidebar Configuration
- Current AWS configuration display
- Bucket names and ARNs
- Session state management
- Connection status indicators

## Configuration

### Environment Variables
The application uses the following configuration sources:

1. **Streamlit Secrets** (`.streamlit/secrets.toml`)
```toml
INGESTION_BUCKET = "msa-invoice-ingestion-bucket"
REPORTS_BUCKET = "msa-invoice-reports-bucket"
STEP_FUNCTION_ARN = "arn:aws:states:region:account:stateMachine:workflow-name"
BEDROCK_AGENT_ID = "agent-id"
BEDROCK_AGENT_ALIAS_ID = "TSTALIASID"
```

2. **Environment Variables** (App Runner deployment)
- `AWS_DEFAULT_REGION`: AWS region for services
- `INGESTION_BUCKET`: S3 bucket for file uploads
- `REPORTS_BUCKET`: S3 bucket for generated reports
- `STEP_FUNCTION_ARN`: Step Functions state machine ARN
- `BEDROCK_AGENT_ID`: Bedrock Agent identifier
- `BEDROCK_AGENT_ALIAS_ID`: Bedrock Agent alias

### AWS Permissions
The UI requires the following AWS permissions:

#### S3 Permissions
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::ingestion-bucket",
        "arn:aws:s3:::ingestion-bucket/*",
        "arn:aws:s3:::reports-bucket",
        "arn:aws:s3:::reports-bucket/*"
      ]
    }
  ]
}
```

#### Step Functions Permissions
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "states:StartExecution",
        "states:DescribeExecution",
        "states:ListExecutions"
      ],
      "Resource": [
        "arn:aws:states:region:account:stateMachine:workflow-name",
        "arn:aws:states:region:account:execution:workflow-name:*"
      ]
    }
  ]
}
```

#### Bedrock Permissions
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeAgent",
        "bedrock:GetAgent",
        "bedrock:ListAgents"
      ],
      "Resource": [
        "arn:aws:bedrock:region:account:agent/agent-id",
        "arn:aws:bedrock:region:account:agent-alias/agent-id/alias-id"
      ]
    }
  ]
}
```

## Deployment

### Local Development

1. **Setup Environment**
```bash
cd ui/
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. **Configure Secrets**
Create `.streamlit/secrets.toml`:
```toml
INGESTION_BUCKET = "your-ingestion-bucket"
REPORTS_BUCKET = "your-reports-bucket"
STEP_FUNCTION_ARN = "your-step-function-arn"
BEDROCK_AGENT_ID = "your-agent-id"
BEDROCK_AGENT_ALIAS_ID = "TSTALIASID"
```

3. **Run Application**
```bash
streamlit run app.py
```

### Production Deployment (App Runner)

1. **Build and Push Docker Image**
```bash
# Build the Docker image
docker build -t msa-invoice-audit-ui .

# Tag for ECR
docker tag msa-invoice-audit-ui:latest <account>.dkr.ecr.<region>.amazonaws.com/msa-invoice-audit-ui:latest

# Push to ECR
docker push <account>.dkr.ecr.<region>.amazonaws.com/msa-invoice-audit-ui:latest
```

2. **Deploy with CDK**
```bash
# Deploy all stacks including UI
cdk deploy --all

# Or deploy UI stack specifically
cdk deploy MSAInvoiceAuditUIStack
```

3. **Access Application**
The App Runner service URL will be provided in the CDK outputs:
```
MSAInvoiceAuditUIStack.AppRunnerServiceURL = https://xxxxxxxxxx.us-east-1.awsapprunner.com
```

## Usage Workflows

### Basic Invoice Analysis

1. **Upload Invoice**
   - Click "Choose an invoice file"
   - Select PDF, Excel, or image file
   - Optionally add analysis query
   - Click "Upload & Start Analysis"

2. **Monitor Progress**
   - Click "Refresh Status" to check progress
   - Wait for "Analysis completed successfully" message
   - View execution details if needed

3. **Review Results**
   - Check generated reports section
   - Download Excel, PDF, or Markdown reports
   - Review analysis flags table
   - Check summary metrics

### Direct Agent Queries

1. **Ask Questions**
   - Enter MSA-related question in text area
   - Examples:
     - "What are the standard labor rates for RS work?"
     - "How do I identify overcharges in equipment rental?"
     - "What compliance requirements apply to this invoice?"

2. **Get Responses**
   - Click "Ask Agent"
   - Review AI-generated response
   - Continue conversation in same session

### Troubleshooting Analysis

1. **Check Execution Status**
   - Use "Refresh Status" for real-time updates
   - Review execution details for errors
   - Check CloudWatch logs if needed

2. **Verify Configuration**
   - Check sidebar configuration display
   - Ensure all ARNs and bucket names are correct
   - Verify AWS credentials are properly configured

## Error Handling

### Common Issues

1. **File Upload Failures**
   - **Cause**: S3 permissions or bucket access issues
   - **Solution**: Verify IAM roles and bucket policies
   - **Indicator**: Error message in upload section

2. **Step Functions Execution Failures**
   - **Cause**: Invalid state machine ARN or permissions
   - **Solution**: Check Step Functions configuration and IAM roles
   - **Indicator**: Execution status shows "FAILED"

3. **Bedrock Agent Errors**
   - **Cause**: Agent not deployed or access permissions
   - **Solution**: Verify Bedrock Agent deployment and IAM permissions
   - **Indicator**: Error message in agent query section

4. **Report Generation Issues**
   - **Cause**: Lambda function errors or S3 write permissions
   - **Solution**: Check Lambda logs and S3 bucket permissions
   - **Indicator**: No reports appear after successful execution

### Error Messages

The UI provides user-friendly error messages for common issues:
- "AWS credentials not configured"
- "Error uploading file to S3"
- "Error starting Step Functions execution"
- "Error invoking Bedrock Agent"
- "No reports found yet"

## Security Considerations

### Authentication and Authorization
- **AWS IAM**: All AWS service access controlled by IAM roles
- **Least Privilege**: UI role has minimal required permissions
- **Resource-based Policies**: S3 buckets and other resources have restrictive policies

### Data Protection
- **Encryption in Transit**: All AWS API calls use HTTPS
- **Encryption at Rest**: S3 buckets use server-side encryption
- **Temporary Storage**: No sensitive data stored locally in container

### Network Security
- **App Runner VPC**: Consider deploying in VPC for additional isolation
- **Security Groups**: Restrict inbound traffic to necessary ports only
- **WAF Integration**: Consider AWS WAF for additional protection

## Monitoring and Logging

### Application Logs
- **CloudWatch Logs**: `/aws/apprunner/msa-invoice-audit-ui`
- **Log Retention**: 30 days (configurable)
- **Log Levels**: INFO, WARNING, ERROR

### Metrics
- **App Runner Metrics**: CPU, memory, request count, response time
- **Custom Metrics**: File upload count, analysis success rate
- **CloudWatch Dashboards**: Operational visibility

### Health Checks
- **App Runner Health Check**: HTTP GET to `/` endpoint
- **Streamlit Health**: `/_stcore/health` endpoint
- **Dependency Checks**: AWS service connectivity

## Performance Optimization

### Caching Strategies
- **Session State**: Streamlit session state for UI persistence
- **Report Caching**: S3-based report storage and retrieval
- **Agent Sessions**: Persistent Bedrock Agent sessions

### Resource Management
- **Memory Usage**: 2GB allocated for App Runner instance
- **CPU Usage**: 1 vCPU for standard workloads
- **Auto-scaling**: App Runner handles traffic spikes automatically

### File Size Limits
- **Upload Limits**: Streamlit default 200MB file size limit
- **Processing Limits**: Lambda function timeout considerations
- **Storage Limits**: S3 bucket lifecycle policies for cleanup

## Testing

### Unit Tests
Run the comprehensive test suite:
```bash
# Run all UI tests
python -m pytest tests/test_ui.py -v

# Run specific test classes
python -m pytest tests/test_ui.py::TestMSAInvoiceAuditor -v
python -m pytest tests/test_ui.py::TestUIIntegration -v

# Run with coverage
python -m pytest tests/test_ui.py --cov=ui --cov-report=html
```

### Integration Tests
- **End-to-end Workflow**: Upload → Analysis → Report Generation
- **AWS Service Integration**: S3, Step Functions, Bedrock Agent
- **Error Handling**: Network failures, permission errors, service timeouts

### Load Testing
- **Concurrent Users**: Test multiple simultaneous uploads
- **File Size Variations**: Test with different file sizes and types
- **Extended Sessions**: Test long-running analysis workflows

## Future Enhancements

### Planned Features
1. **Real-time Notifications**: WebSocket-based status updates
2. **Batch Processing**: Multiple file upload and processing
3. **Advanced Filtering**: Report filtering and search capabilities
4. **User Management**: Authentication and user-specific workspaces
5. **API Integration**: REST API for programmatic access

### Performance Improvements
1. **Caching Layer**: Redis for session and report caching
2. **CDN Integration**: CloudFront for static asset delivery
3. **Database Integration**: RDS for user preferences and history
4. **Async Processing**: Background job processing for large files

### Security Enhancements
1. **Multi-factor Authentication**: Enhanced user security
2. **Audit Logging**: Comprehensive user action logging
3. **Data Loss Prevention**: Sensitive data detection and protection
4. **Compliance Reporting**: SOC 2, GDPR compliance features

## Support and Maintenance

### Troubleshooting Guide
1. Check CloudWatch logs for detailed error information
2. Verify AWS service quotas and limits
3. Test individual components (S3, Step Functions, Bedrock)
4. Review IAM permissions and resource policies

### Maintenance Tasks
1. **Regular Updates**: Keep dependencies and base images updated
2. **Log Rotation**: Monitor and manage log retention
3. **Performance Monitoring**: Track metrics and optimize as needed
4. **Security Patches**: Apply security updates promptly

### Contact Information
- **Development Team**: GRT Hackathon Team 8
- **Documentation**: See `docs/` directory for additional resources
- **Issue Tracking**: Use project issue tracker for bug reports and feature requests
