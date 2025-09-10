# AI Agent Setup for Invoice Auditing

## Overview

The AI Agent component provides intelligent invoice auditing capabilities using Amazon Bedrock Agent with Claude 3.5 Sonnet. It integrates with the existing ingestion and extraction pipeline to automatically audit invoices against Master Services Agreement (MSA) standards, detecting discrepancies in labor rates and overtime violations.

## Architecture

### Components

1. **Bedrock Agent** (`invoice-audit-agent`)
   - Uses Claude 3.5 Sonnet for intelligent analysis
   - Configured with specialized audit instructions
   - Integrates with Knowledge Base for historical data
   - Provides natural language audit reports

2. **Knowledge Base** (`invoice-audit-knowledge-base`)
   - OpenSearch vector database for semantic search
   - S3 data source with extracted invoice data
   - 20% overlap chunking for context preservation
   - Metadata preservation for page/section references

3. **Agent Lambda Function** (`agent-lambda`)
   - Orchestrates audit workflow
   - Calls extraction Lambda for data processing
   - Performs MSA compliance checks
   - Invokes Bedrock Agent for analysis

4. **MSA Rates DynamoDB Table** (`msa-rates`)
   - Stores standard labor rates by type and location
   - Overtime thresholds and rules
   - Supports location-specific rate variations

5. **OpenSearch Domain** (`invoice-audit-kb`)
   - Vector storage for Knowledge Base
   - Encrypted at rest and in transit
   - Single-node configuration for cost optimization

## Key Features

### Intelligent Auditing
- **Rate Variance Detection**: Compares actual rates against MSA standards with 5% tolerance
- **Overtime Violation Detection**: Flags workers exceeding 40 hours/week threshold
- **Labor Type Classification**: Validates proper use of RS, US, SS, SU, EN classifications
- **Contextual Analysis**: Uses Knowledge Base for historical comparisons

### Natural Language Interface
- **Query Processing**: "Audit this invoice against MSA standards"
- **Detailed Reports**: Comprehensive audit findings with recommendations
- **Conversational Responses**: Natural language explanations of discrepancies

### Integration Capabilities
- **Extraction Pipeline**: Seamlessly calls existing extraction Lambda
- **Knowledge Base**: Ingests processed invoice data for future reference
- **Guardrails**: Built-in accuracy controls and response validation

## Configuration

### Environment Variables

```bash
# Agent Lambda Environment
BEDROCK_AGENT_ID=<agent-id>
BEDROCK_AGENT_ALIAS_ID=TSTALIASID
KNOWLEDGE_BASE_ID=<kb-id>
MSA_RATES_TABLE=msa-rates
EXTRACTION_LAMBDA_NAME=extraction-lambda
BUCKET_NAME=audit-files-bucket
LOG_LEVEL=INFO
```

### MSA Rates Configuration

The system includes pre-configured standard rates:

| Labor Type | Description | Standard Rate | Overtime Threshold |
|------------|-------------|---------------|-------------------|
| RS | Regular Skilled Labor | $70.00/hour | 40 hours/week |
| US | Unskilled Supervisor | $85.00/hour | 40 hours/week |
| SS | Skilled Supervisor | $95.00/hour | 40 hours/week |
| SU | Senior Supervisor | $110.00/hour | 40 hours/week |
| EN | Engineer | $125.00/hour | 45 hours/week |

### IAM Permissions

The agent components require:

**Agent Lambda:**
- Bedrock: InvokeAgent, InvokeModel
- DynamoDB: GetItem, Query, Scan (msa-rates table)
- Lambda: InvokeFunction (extraction-lambda)
- S3: GetObject, HeadObject

**Bedrock Agent:**
- Bedrock: InvokeModel (Claude 3.5 Sonnet)
- Bedrock: Retrieve, RetrieveAndGenerate (Knowledge Base)
- Lambda: InvokeFunction (extraction-lambda)

**Knowledge Base:**
- S3: GetObject, ListBucket (ingestion bucket)
- OpenSearch: ESHttp* operations
- Bedrock: InvokeModel (Titan embeddings)

## API Reference

### Agent Lambda Handler

```python
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]
```

**Audit Action:**
```json
{
  "action": "audit",
  "bucket": "audit-files-bucket",
  "key": "invoice.pdf",
  "query": "Audit this invoice against MSA standards"
}
```

**Query Action:**
```json
{
  "action": "query",
  "query": "What are the standard MSA rates for RS labor?"
}
```

**Response Structure:**
```json
{
  "audit_id": "uuid",
  "timestamp": "2025-01-09T20:50:00Z",
  "status": "completed",
  "file_info": {
    "bucket": "audit-files-bucket",
    "key": "invoice.pdf"
  },
  "extraction_summary": {
    "status": "completed",
    "page_count": 22,
    "processing_method": "async"
  },
  "audit_results": {
    "discrepancies": [
      {
        "type": "rate_variance",
        "severity": "medium",
        "worker": "Smith, John",
        "labor_type": "RS",
        "actual_rate": 77.00,
        "msa_rate": 70.00,
        "variance_percentage": 10.0,
        "description": "Rate variance for Smith, John (RS): $77 vs MSA $70"
      }
    ],
    "summary": {
      "total_discrepancies": 1,
      "rate_variances": 1,
      "overtime_violations": 0,
      "total_labor_cost": 4235.00,
      "expected_labor_cost": 3850.00
    }
  },
  "recommendations": "Audit completed. Found rate variance: RS labor at $77 vs MSA standard $70. Recommend reviewing labor rate justification.",
  "agent_response": {
    "status": "success",
    "session_id": "session-uuid"
  }
}
```

### Classes

#### MSARatesManager
Manages MSA rate lookups from DynamoDB.

**Methods:**
- `get_rate_for_labor_type(labor_type, location='default')`: Get standard rate
- `get_overtime_threshold(labor_type='default')`: Get overtime threshold

#### InvoiceAuditor
Performs compliance auditing against MSA standards.

**Methods:**
- `audit_extracted_data(extracted_data)`: Main audit method
- `_audit_labor_costs(labor_data, audit_results)`: Audit labor rates and hours
- `_audit_materials(materials_data, audit_results)`: Audit materials (future)

#### BedrockAgentManager
Manages Bedrock Agent interactions.

**Methods:**
- `invoke_agent(query, session_id=None)`: Invoke agent with query
- `_process_agent_response(response)`: Process streaming response

## Sample Audit Scenarios

### Scenario 1: Rate Variance Detection

**Input Invoice:**
- Worker: Smith, John (RS)
- Rate: $77.00/hour (vs MSA standard $70.00)
- Hours: 35 hours

**Expected Output:**
```json
{
  "discrepancies": [
    {
      "type": "rate_variance",
      "severity": "medium",
      "worker": "Smith, John",
      "labor_type": "RS",
      "actual_rate": 77.00,
      "msa_rate": 70.00,
      "variance_amount": 7.00,
      "variance_percentage": 10.0
    }
  ]
}
```

### Scenario 2: Overtime Violation

**Input Invoice:**
- Worker: Doe, Jane (RS)
- Rate: $70.00/hour (matches MSA)
- Hours: 45 hours (exceeds 40-hour threshold)

**Expected Output:**
```json
{
  "discrepancies": [
    {
      "type": "overtime_violation",
      "severity": "medium",
      "worker": "Doe, Jane",
      "total_hours": 45.0,
      "overtime_hours": 5.0,
      "threshold": 40.0
    }
  ]
}
```

### Scenario 3: Compliant Invoice

**Input Invoice:**
- Worker: Johnson, Bob (RS)
- Rate: $70.00/hour (matches MSA)
- Hours: 35 hours (within threshold)

**Expected Output:**
```json
{
  "discrepancies": [],
  "summary": {
    "total_discrepancies": 0,
    "rate_variances": 0,
    "overtime_violations": 0
  }
}
```

## Testing

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-cov moto

# Run agent tests
pytest tests/test_agent.py -v

# Run with coverage
pytest tests/test_agent.py --cov=lambda/agent_lambda --cov-report=html
```

### Test Coverage

The test suite includes:
- **Unit Tests**: Individual class and method testing
- **Integration Tests**: Complete audit workflow testing
- **Mock Services**: DynamoDB, S3, Lambda, and Bedrock mocking
- **Scenario Tests**: Rate variance and overtime violation detection
- **Error Handling**: Exception and fallback testing

### Key Test Cases

1. **MSA Rate Retrieval**: Validates DynamoDB rate lookups
2. **Audit Logic**: Tests discrepancy detection algorithms
3. **Bedrock Integration**: Mocks agent invocation and response processing
4. **Lambda Integration**: Tests extraction Lambda calls
5. **End-to-End Workflows**: Complete audit scenarios with expected outcomes

## Deployment

### Prerequisites

1. AWS CDK v2 installed and configured
2. Python 3.11 runtime
3. Bedrock model access (Claude 3.5 Sonnet, Titan embeddings)
4. Required IAM permissions for Bedrock services

### Deploy Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Deploy both stacks
cdk deploy --all

# Deploy agent stack only (after ingestion stack exists)
cdk deploy InvoiceAuditAgentStack

# Verify deployment
aws bedrock-agent list-agents
aws opensearch list-domain-names
aws dynamodb list-tables --query 'TableNames[?contains(@, `msa-rates`)]'
```

### Post-Deployment Setup

1. **Knowledge Base Sync**: Trigger initial data source synchronization
```bash
aws bedrock-agent start-ingestion-job \
  --knowledge-base-id <kb-id> \
  --data-source-id <ds-id>
```

2. **Agent Testing**: Test agent functionality
```bash
aws lambda invoke \
  --function-name agent-lambda \
  --payload '{"action":"query","query":"What are MSA rates?"}' \
  response.json
```

3. **Rate Table Verification**: Confirm MSA rates are populated
```bash
aws dynamodb scan --table-name msa-rates
```

## Monitoring

### CloudWatch Logs

- **Agent Lambda**: `/aws/lambda/agent-lambda`
- **Bedrock Agent**: `/aws/bedrock/agents/<agent-id>`
- **Knowledge Base**: `/aws/bedrock/knowledgebases/<kb-id>`
- **OpenSearch**: `/aws/opensearch/domains/invoice-audit-kb`

### Key Metrics

- **Agent Invocations**: Lambda invocation count and duration
- **Audit Success Rate**: Percentage of successful audits
- **Discrepancy Detection**: Rate of discrepancies found
- **Knowledge Base Queries**: Vector search performance
- **OpenSearch Health**: Cluster status and query latency

### Alarms

Recommended CloudWatch alarms:
- Agent Lambda error rate > 5%
- Agent Lambda duration > 5 minutes
- OpenSearch cluster status != Green
- DynamoDB throttling events

## Troubleshooting

### Common Issues

1. **Agent Not Responding**
   - Check Bedrock model access permissions
   - Verify agent alias is deployed
   - Review CloudWatch logs for errors

2. **Knowledge Base Empty Results**
   - Trigger data source synchronization
   - Check S3 bucket permissions
   - Verify embedding model access

3. **Rate Lookup Failures**
   - Confirm DynamoDB table exists and is populated
   - Check IAM permissions for table access
   - Verify table key schema matches code

4. **Extraction Integration Issues**
   - Test extraction Lambda independently
   - Check cross-Lambda invocation permissions
   - Verify S3 object accessibility

### Debug Mode

Enable detailed logging:
```bash
export LOG_LEVEL=DEBUG
```

This provides:
- Detailed audit calculations
- Bedrock agent request/response logs
- DynamoDB query details
- Knowledge Base search results

## Cost Optimization

### Estimated Costs (Monthly)

- **Bedrock Agent**: ~$50-100 (based on usage)
- **Claude 3.5 Sonnet**: ~$15 per 1M tokens
- **OpenSearch**: ~$25 (t3.small.search)
- **DynamoDB**: ~$5 (pay-per-request)
- **Lambda**: ~$10 (based on invocations)

**Total Estimated**: ~$105-155/month

### Cost Reduction Strategies

1. **OpenSearch Optimization**
   - Use reserved instances for predictable workloads
   - Implement data lifecycle policies
   - Consider OpenSearch Serverless for variable workloads

2. **Bedrock Usage**
   - Implement response caching for common queries
   - Optimize prompt length and complexity
   - Use batch processing for multiple invoices

3. **Lambda Optimization**
   - Right-size memory allocation
   - Implement connection pooling
   - Use provisioned concurrency only if needed

## Security Considerations

### Data Protection

- **Encryption**: All data encrypted at rest and in transit
- **Access Control**: Least privilege IAM policies
- **Network Security**: VPC endpoints for service communication
- **Audit Logging**: Comprehensive CloudTrail logging

### Compliance

- **Data Residency**: Configurable AWS region deployment
- **Retention Policies**: Automated data lifecycle management
- **Access Auditing**: CloudTrail integration for compliance reporting

### Best Practices

1. **Secrets Management**: Use AWS Secrets Manager for sensitive data
2. **Network Isolation**: Deploy in private subnets with NAT Gateway
3. **Monitoring**: Enable GuardDuty and Security Hub
4. **Backup**: Implement automated backup strategies

## Future Enhancements

### Planned Features

1. **Advanced Analytics**
   - Trend analysis across multiple invoices
   - Predictive modeling for rate variance patterns
   - Dashboard for audit metrics visualization

2. **Enhanced Integrations**
   - Direct ERP system integration
   - Real-time notification systems
   - Automated approval workflows

3. **Machine Learning Improvements**
   - Custom entity recognition models
   - Anomaly detection algorithms
   - Automated rate adjustment recommendations

4. **User Interface**
   - Web-based audit dashboard
   - Mobile application for field auditing
   - API gateway for third-party integrations

## Support

For issues and questions:
1. Check CloudWatch logs for detailed error messages
2. Review test cases for expected behavior patterns
3. Consult AWS Bedrock documentation for service limits
4. Monitor agent performance metrics for optimization opportunities
5. Use AWS Support for service-specific issues

## API Examples

### Audit Invoice via CLI

```bash
aws lambda invoke \
  --function-name agent-lambda \
  --payload '{
    "action": "audit",
    "bucket": "audit-files-bucket",
    "key": "invoices/contractor-invoice-2025.pdf",
    "query": "Audit this invoice against MSA standards and flag any discrepancies"
  }' \
  audit-result.json

cat audit-result.json | jq '.audit_results.summary'
```

### Query MSA Standards

```bash
aws lambda invoke \
  --function-name agent-lambda \
  --payload '{
    "action": "query",
    "query": "What are the current MSA rates for all labor types?"
  }' \
  rates-query.json

cat rates-query.json | jq '.response'
```

### Batch Audit Processing

```bash
# Process multiple invoices
for invoice in invoices/*.pdf; do
  aws lambda invoke \
    --function-name agent-lambda \
    --payload "{
      \"action\": \"audit\",
      \"bucket\": \"audit-files-bucket\",
      \"key\": \"$invoice\",
      \"query\": \"Audit against MSA standards\"
    }" \
    "results/$(basename $invoice .pdf)-audit.json"
done
