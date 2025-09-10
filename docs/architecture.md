# Architecture Documentation

## Overview

The Invoice Auditing File Ingestion Module is a comprehensive AI-powered serverless AWS solution designed to automatically process, extract, analyze, and audit invoice files for Master Services Agreement (MSA) compliance. The system combines advanced document processing with intelligent AI agents to provide automated invoice auditing capabilities.

## Complete Architecture Diagram

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

## Components

### Ingestion Stack Components

### 1. S3 Bucket (`audit-files-bucket`)

**Purpose**: Secure storage for uploaded invoice files and processed results

**Features**:
- Server-side encryption (AES-256)
- Versioning enabled
- Public access blocked
- Lifecycle policies for cost optimization
- Event notifications for object creation

**Security**:
- Bucket policy denies insecure connections
- IAM-based access control
- CloudTrail logging for audit trail

### 2. Ingestion Lambda (`ingestion-lambda`)

**Purpose**: Initial file processing and workflow orchestration

**Runtime**: Python 3.11
**Memory**: 512MB
**Timeout**: 5 minutes

**Responsibilities**:
- Handle S3 event notifications
- Validate file types and sizes
- Extract basic file metadata
- Process files and apply tags
- Orchestrate basic Step Functions workflow

**Supported File Types**:
- PDF (.pdf)
- Excel (.xlsx, .xls)
- Images (.png, .jpg, .jpeg)

### 3. Extraction Lambda (`extraction-lambda`)

**Purpose**: Advanced document data extraction using AI services

**Runtime**: Python 3.11
**Memory**: 1024MB
**Timeout**: 15 minutes

**AI Services Integration**:
- **Amazon Textract**: OCR and document analysis
- **Amazon Bedrock**: Semantic mapping and normalization
- **Amazon Comprehend**: Entity recognition
- **Pandas**: Excel file processing

**Capabilities**:
- Intelligent field mapping and vendor terminology normalization
- Adaptive processing for different document formats
- Semantic chunking for Knowledge Base ingestion
- Confidence-based quality filtering

### 4. Basic Step Functions (`invoice-audit-workflow`)

**Purpose**: Orchestrate initial file processing workflow

**Workflow Steps**:
1. **Validate File**: Check file type, size, and format
2. **Extract Metadata**: Extract basic file metadata
3. **Process File**: Apply processing tags

### AI Agent Stack Components

### 5. AI Processing Step Functions (`invoice-processing-workflow`)

**Purpose**: Orchestrate complete AI-powered invoice analysis

**Workflow Steps**:
1. **Extract Invoice Data**: Advanced extraction using Textract + AI
2. **Compare and Flag Discrepancies**: Analyze against MSA standards
3. **Agent Analysis**: Generate intelligent audit reports

### 6. Comparison Lambda (`comparison-lambda`)

**Purpose**: Compare extracted data against MSA standards and flag discrepancies

**Runtime**: Python 3.11
**Memory**: 1024MB
**Timeout**: 15 minutes

**Capabilities**:
- MSA rate comparison with configurable variance thresholds (5%)
- Overtime violation detection (>40 hours/week)
- Labor classification validation
- SageMaker anomaly detection integration

### 7. Agent Lambda (`agent-lambda`)

**Purpose**: Coordinate with Bedrock Agent for intelligent analysis

**Runtime**: Python 3.11
**Memory**: 512MB
**Timeout**: 10 minutes

**Responsibilities**:
- Interface with Bedrock Agent
- Coordinate Knowledge Base queries
- Generate comprehensive audit reports
- Handle agent conversation flows

### 8. DynamoDB MSA Rates Table (`msa-rates`)

**Purpose**: Store Master Services Agreement standard rates and rules

**Schema**:
- **Partition Key**: `labor_type` (RS, US, SS, SU, EN)
- **Sort Key**: `location` (default, specific locations)
- **Attributes**: `standard_rate`, `description`, `weekly_threshold`

**Sample Data**:
- RS (Regular Skilled): $70.00/hour
- US (Unskilled Supervisor): $85.00/hour
- SS (Skilled Supervisor): $95.00/hour
- SU (Senior Supervisor): $110.00/hour
- EN (Engineer): $125.00/hour

### 9. OpenSearch Domain (`invoice-audit-kb`)

**Purpose**: Vector storage for Bedrock Knowledge Base

**Configuration**:
- Engine: OpenSearch 2.5
- Instance: t3.small.search
- Storage: 20GB GP3
- Encryption: At rest and in transit

### 10. Bedrock Knowledge Base (`invoice-audit-knowledge-base`)

**Purpose**: Store and retrieve invoice data and MSA standards for AI analysis

**Features**:
- Vector embeddings using Titan Embed Text v1
- S3 data source integration
- Semantic chunking and retrieval
- Automated data ingestion from processed invoices

### 11. Bedrock Agent (`invoice-audit-agent`)

**Purpose**: AI agent for intelligent invoice auditing

**Model**: Claude 3.5 Sonnet
**Capabilities**:
- Analyze extracted invoice data for MSA compliance
- Compare labor rates against established standards
- Identify overtime violations and classification errors
- Generate detailed audit reports with recommendations

**Action Groups**:
- **Extraction Actions**: Interface with extraction Lambda for data processing

### 12. SageMaker Endpoint (`invoice-anomaly-detection`)

**Purpose**: Machine learning-based anomaly detection

**Model**: Isolation Forest (XGBoost)
**Instance**: ml.t2.medium
**Use Cases**:
- Detect unusual spending patterns
- Identify potential fraud indicators
- Flag statistical outliers in labor costs

### 13. Lambda Layer (`CommonUtilsLayer`)

**Purpose**: Shared utilities and common functions

**Contents**:
- File processing utilities
- Audit logging functions
- S3 helper functions
- Step Functions integration utilities
- Bedrock and AI service helpers

### 14. IAM Roles and Policies

**Ingestion Lambda Role**:
- S3 read/write access to audit bucket
- Step Functions execution permissions
- CloudWatch logging permissions

**Extraction Lambda Role**:
- S3 read/write access
- Textract analysis permissions
- Bedrock model invocation
- Comprehend entity detection

**Agent Lambda Role**:
- Bedrock Agent invocation
- DynamoDB read access to MSA rates
- Knowledge Base query permissions

**Comparison Lambda Role**:
- DynamoDB read access
- SageMaker endpoint invocation
- Bedrock model access for analysis

**Bedrock Agent Role**:
- Knowledge Base access
- Lambda function invocation
- S3 read access for data sources

**Knowledge Base Role**:
- S3 read access to data sources
- OpenSearch read/write access
- Bedrock model invocation for embeddings

## Complete Data Flow

### 1. File Upload and Initial Processing
```
External System → S3 Bucket (audit-files-bucket) → S3 Event Notification → Ingestion Lambda
```

### 2. Basic Ingestion Workflow
```
Ingestion Lambda → Basic Step Functions (invoice-audit-workflow)
├── Validate File (type, size, format)
├── Extract Basic Metadata
└── Process File (apply tags)
```

### 3. AI-Powered Analysis Workflow
```
File Ready → AI Processing Step Functions (invoice-processing-workflow)
├── Extract Invoice Data (Extraction Lambda)
│   ├── Textract (OCR/Document Analysis)
│   ├── Bedrock (Semantic Mapping)
│   ├── Comprehend (Entity Recognition)
│   └── Pandas (Excel Processing)
├── Compare and Flag Discrepancies (Comparison Lambda)
│   ├── Query MSA Rates (DynamoDB)
│   ├── Rate Variance Analysis (5% threshold)
│   ├── Overtime Detection (>40 hours/week)
│   └── Anomaly Detection (SageMaker)
└── Agent Analysis (Agent Lambda + Bedrock Agent)
    ├── Knowledge Base Query (OpenSearch)
    ├── AI-Powered Audit Analysis
    └── Generate Audit Report
```

### 4. Knowledge Base Integration
```
Extracted Data → Semantic Chunking → Knowledge Base Ingestion
├── Vector Embeddings (Titan Embed Text v1)
├── OpenSearch Storage
└── Available for Agent Queries
```

### 5. Audit Report Generation
```
Agent Analysis → Comprehensive Audit Report → S3 Storage
├── Labor Rate Discrepancies
├── Overtime Violations
├── Classification Errors
├── Anomaly Flags
└── Recommendations
```

### 6. Error Handling and Monitoring
```
Any Step Failure → Error State → CloudWatch Logs → Monitoring Alerts
├── Retry Logic (transient failures)
├── Fallback Mechanisms (AI service failures)
└── Dead Letter Queues (permanent failures)
```

### 7. Complete End-to-End Flow
```
1. File Upload → S3 Event → Ingestion Lambda
2. Basic Validation → Ingestion Step Functions
3. AI Processing Trigger → AI Step Functions
4. Data Extraction → Textract + Bedrock + Comprehend
5. MSA Comparison → DynamoDB + SageMaker Analysis
6. AI Agent Analysis → Bedrock Agent + Knowledge Base
7. Report Generation → S3 Storage + Notifications
8. Monitoring → CloudWatch Metrics + Alarms
```

## Security Architecture

### 1. Network Security
- All resources deployed in AWS managed infrastructure
- No public endpoints exposed
- VPC endpoints for AWS service communication

### 2. Data Encryption
- **At Rest**: S3 server-side encryption (AES-256)
- **In Transit**: HTTPS/TLS for all communications
- **Logs**: CloudWatch logs encrypted

### 3. Access Control
- **IAM**: Least privilege access principles
- **S3**: Bucket policies and ACLs
- **Lambda**: Execution role with minimal permissions

### 4. Monitoring and Auditing
- **CloudTrail**: API call logging
- **CloudWatch**: Metrics and alarms
- **VPC Flow Logs**: Network traffic monitoring

## Scalability and Performance

### 1. Auto Scaling
- **Lambda**: Automatic scaling based on demand
- **S3**: Unlimited storage capacity
- **Step Functions**: Concurrent execution support

### 2. Performance Optimization
- **Lambda**: Optimized memory allocation
- **S3**: Transfer acceleration available
- **Step Functions**: Parallel processing capabilities

### 3. Cost Optimization
- **S3**: Lifecycle policies for storage class transitions
- **Lambda**: Pay-per-execution model
- **CloudWatch**: Log retention policies

## Monitoring and Observability

### 1. CloudWatch Metrics
- Lambda execution duration and errors
- S3 request metrics
- Step Functions execution metrics

### 2. CloudWatch Logs
- Lambda function logs
- Step Functions execution logs
- Structured logging for audit trails

### 3. CloudWatch Alarms
- Lambda error rates
- Step Functions failures
- S3 access patterns

### 4. X-Ray Tracing (Optional)
- End-to-end request tracing
- Performance bottleneck identification
- Service map visualization

## Disaster Recovery

### 1. Backup Strategy
- **S3**: Cross-region replication available
- **Infrastructure**: Infrastructure as Code (CDK)
- **Configuration**: Version-controlled deployment

### 2. Recovery Procedures
- **RTO**: < 1 hour (infrastructure recreation)
- **RPO**: Near-zero (S3 durability: 99.999999999%)
- **Automation**: CDK-based infrastructure recreation

## Compliance and Governance

### 1. Data Governance
- File retention policies
- Data classification and tagging
- Access logging and monitoring

### 2. Compliance Features
- Audit trail maintenance
- Data encryption standards
- Access control enforcement

### 3. Regulatory Considerations
- GDPR: Data processing transparency
- SOX: Financial data controls
- HIPAA: Healthcare data protection (if applicable)

## Integration Points

### 1. External Systems
- **File Upload**: REST API or direct S3 upload
- **Monitoring**: CloudWatch integration
- **Notifications**: SNS/SES integration (future)

### 2. AWS Services
- **S3**: Primary storage
- **Lambda**: Compute layer
- **Step Functions**: Workflow orchestration
- **CloudWatch**: Monitoring and logging
- **IAM**: Security and access control

## Future Enhancements

### 1. Advanced Processing
- OCR for scanned documents
- AI/ML-based content analysis
- Automated data extraction

### 2. Integration Expansion
- Database integration for metadata storage
- API Gateway for external access
- SQS for decoupled processing

### 3. Enhanced Security
- KMS customer-managed keys
- VPC deployment option
- Advanced threat detection

## Operational Considerations

### 1. Deployment
- Blue/green deployment strategy
- Canary releases for updates
- Rollback procedures

### 2. Maintenance
- Regular security updates
- Performance optimization
- Cost analysis and optimization

### 3. Support
- Runbook documentation
- Troubleshooting guides
- Escalation procedures

## Cost Analysis

### 1. Cost Components
- **S3 Storage**: $0.023/GB/month (Standard)
- **Lambda**: $0.20/1M requests + $0.0000166667/GB-second
- **Step Functions**: $0.025/1K state transitions
- **CloudWatch**: $0.50/million API requests

### 2. Cost Optimization Strategies
- S3 lifecycle policies
- Lambda memory optimization
- CloudWatch log retention policies
- Reserved capacity for predictable workloads

### 3. Estimated Monthly Costs
- **Low Volume** (1K files): ~$10-20/month
- **Medium Volume** (10K files): ~$50-100/month
- **High Volume** (100K files): ~$200-500/month

*Note: Costs vary based on file sizes, processing complexity, and retention policies.*

## Conclusion

The Invoice Auditing File Ingestion Module provides a robust, scalable, and secure solution for processing invoice files in a serverless architecture. The design emphasizes security, cost-effectiveness, and operational simplicity while maintaining the flexibility to scale and evolve with changing requirements.

The modular architecture allows for easy extension and modification, while the comprehensive monitoring and logging capabilities ensure operational visibility and compliance with audit requirements.
