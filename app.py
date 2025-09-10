#!/usr/bin/env python3
"""
MSA Invoice Auditing System - CDK Application Entry Point

This module defines the main CDK application that deploys the complete serverless
MSA (Master Services Agreement) invoice auditing system including:

- File ingestion and processing (PDF, Excel, images)
- AI-powered data extraction using Amazon Textract
- Intelligent analysis with Bedrock Agent and Claude 3.5 Sonnet
- MSA compliance comparison and anomaly detection
- Comprehensive report generation (Markdown, Excel, PDF)
- Step Functions orchestration workflow

Architecture:
1. Ingestion Stack: S3 buckets, Lambda triggers, Textract processing
2. Agent Stack: Bedrock Agent, Knowledge Base, DynamoDB, SageMaker, Step Functions
3. Report Generation: AI reports, Excel templates, PDF conversion

The system processes vendor invoices against Master Services Agreements (MSA)
to identify overcharges, compliance issues, and potential savings opportunities.
"""

import aws_cdk as cdk
from infrastructure.full_stack import MSAInvoiceAuditFullStack


def main() -> None:
    app = cdk.App()

    env = cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region"),
    )

    full_stack = MSAInvoiceAuditFullStack(
        app,
        "MSAInvoiceAuditFullStack",
        description="Minimal infrastructure for the invoice auditing prototype",
        env=env,
    )

    cdk.Tags.of(full_stack).add("Project", "MSA-Invoice-Auditing")
    cdk.Tags.of(full_stack).add("Environment", "Prototype")
    cdk.Tags.of(full_stack).add("Owner", "GRT-Hackathon-Team8")

    app.synth()


if __name__ == "__main__":
    main()
