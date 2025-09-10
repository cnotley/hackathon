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
from infrastructure.ingestion_stack import InvoiceIngestionStack
from infrastructure.agent_stack import InvoiceAuditAgentStack
from infrastructure.ui_stack import MSAInvoiceAuditUIStack
from infrastructure.full_stack import MSAInvoiceAuditFullStack


def main():
    """
    Main function to create and deploy the complete MSA Invoice Auditing System.
    
    Provides two deployment options:
    1. Full Stack - Single integrated deployment (recommended for production)
    2. Modular Stacks - Separate stacks for development and testing
    
    Use CDK context to choose deployment mode:
    - cdk deploy --context deployment=full (default)
    - cdk deploy --context deployment=modular
    """
    app = cdk.App()
    
    # Define common environment from CDK context
    env = cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region")
    )
    
    # Get deployment mode from context (default to full)
    deployment_mode = app.node.try_get_context("deployment") or "full"
    
    if deployment_mode == "full":
        # Deploy as single integrated stack (recommended)
        full_stack = MSAInvoiceAuditFullStack(
            app,
            "MSAInvoiceAuditFullStack",
            description="Complete MSA Invoice Auditing System - All components integrated",
            env=env
        )
        
        # Add stack tags
        cdk.Tags.of(full_stack).add("Project", "MSA-Invoice-Auditing")
        cdk.Tags.of(full_stack).add("Environment", "Production")
        cdk.Tags.of(full_stack).add("Owner", "GRT-Hackathon-Team8")
        cdk.Tags.of(full_stack).add("DeploymentMode", "Full")
        
    else:
        # Deploy as separate modular stacks (for development/testing)
        
        # Create the file ingestion and processing stack
        ingestion_stack = InvoiceIngestionStack(
            app, 
            "MSAInvoiceIngestionStack",
            description="MSA Invoice Auditing - File ingestion and data extraction components",
            env=env
        )
        
        # Create the AI agent stack with comprehensive analysis and reporting
        agent_stack = InvoiceAuditAgentStack(
            app,
            "MSAInvoiceAuditAgentStack",
            ingestion_bucket=ingestion_stack.bucket,
            extraction_lambda=ingestion_stack.extraction_lambda,
            description="MSA Invoice Auditing - AI agent, analysis, comparison, and report generation",
            env=env
        )
        
        # Ensure ingestion stack is deployed before agent stack
        agent_stack.add_dependency(ingestion_stack)
        
        # Create the UI stack with Streamlit application
        ui_stack = MSAInvoiceAuditUIStack(
            app,
            "MSAInvoiceAuditUIStack",
            ingestion_bucket_name=ingestion_stack.bucket.bucket_name,
            reports_bucket_name=agent_stack.reports_bucket.bucket_name,
            step_function_arn=agent_stack.step_function.state_machine_arn,
            bedrock_agent_id=agent_stack.bedrock_agent.attr_agent_id,
            bedrock_agent_alias_id="TSTALIASID",
            description="MSA Invoice Auditing - Streamlit web interface",
            env=env
        )
        
        # Ensure UI stack is deployed after both ingestion and agent stacks
        ui_stack.add_dependency(ingestion_stack)
        ui_stack.add_dependency(agent_stack)
        
        # Add stack tags for modular deployment
        for stack in [ingestion_stack, agent_stack, ui_stack]:
            cdk.Tags.of(stack).add("Project", "MSA-Invoice-Auditing")
            cdk.Tags.of(stack).add("Environment", "Development")
            cdk.Tags.of(stack).add("Owner", "GRT-Hackathon-Team8")
            cdk.Tags.of(stack).add("DeploymentMode", "Modular")
    
    app.synth()


if __name__ == "__main__":
    main()
