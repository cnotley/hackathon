"""
AI Agent Stack for Invoice Auditing

This module defines the CDK stack for the AI agent components including
Bedrock Agent, Knowledge Base, DynamoDB table for MSA rates, and related infrastructure.
"""

from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_s3 as s3,
    aws_opensearch as opensearch,
    aws_bedrock as bedrock,
    aws_logs as logs,
    aws_sagemaker as sagemaker,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks
)
from constructs import Construct
import json


class InvoiceAuditAgentStack(Stack):
    """CDK Stack for AI Agent components."""

    def __init__(self, scope: Construct, construct_id: str, 
                 ingestion_bucket: s3.Bucket, extraction_lambda: _lambda.Function, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Store references to shared resources
        self.ingestion_bucket = ingestion_bucket
        self.extraction_lambda = extraction_lambda

        # Create MSA rates DynamoDB table
        self.msa_rates_table = self._create_msa_rates_table()
        
        # Create OpenSearch domain for Knowledge Base
        self.opensearch_domain = self._create_opensearch_domain()
        
        # Create Knowledge Base
        self.knowledge_base = self._create_knowledge_base()
        
        # Create Bedrock Agent
        self.bedrock_agent = self._create_bedrock_agent()
        
        # Create Agent Lambda function
        self.agent_lambda = self._create_agent_lambda()
        
        # Create SageMaker endpoint for anomaly detection
        self.sagemaker_endpoint = self._create_sagemaker_endpoint()
        
        # Create Comparison Lambda function
        self.comparison_lambda = self._create_comparison_lambda()
        
        # Create Report Lambda function
        self.report_lambda = self._create_report_lambda()
        
        # Create Step Functions workflow
        self.step_function = self._create_step_function()
        
        # Populate MSA rates table with sample data
        self._populate_msa_rates()
        
        # Create CloudFormation outputs
        self._create_outputs()

    def _create_msa_rates_table(self) -> dynamodb.Table:
        """Create DynamoDB table for MSA rates."""
        table = dynamodb.Table(
            self,
            "MSARatesTable",
            table_name="msa-rates",
            partition_key=dynamodb.Attribute(
                name="labor_type",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="location",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,  # For demo purposes
            point_in_time_recovery=True,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES
        )
        
        # Add GSI for location-based queries
        table.add_global_secondary_index(
            index_name="LocationIndex",
            partition_key=dynamodb.Attribute(
                name="location",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="labor_type",
                type=dynamodb.AttributeType.STRING
            )
        )
        
        return table

    def _create_opensearch_domain(self) -> opensearch.Domain:
        """Create OpenSearch domain for Knowledge Base vector storage."""
        # Create OpenSearch domain
        domain = opensearch.Domain(
            self,
            "KnowledgeBaseOpenSearch",
            domain_name="invoice-audit-kb",
            version=opensearch.EngineVersion.OPENSEARCH_2_5,
            capacity=opensearch.CapacityConfig(
                data_nodes=1,
                data_node_instance_type="t3.small.search",
                master_nodes=0
            ),
            ebs=opensearch.EbsOptions(
                volume_size=20,
                volume_type=opensearch.EbsDeviceVolumeType.GP3
            ),
            zone_awareness=opensearch.ZoneAwarenessConfig(
                enabled=False
            ),
            logging=opensearch.LoggingOptions(
                slow_search_log_enabled=True,
                app_log_enabled=True,
                slow_index_log_enabled=True
            ),
            node_to_node_encryption=True,
            encryption_at_rest=opensearch.EncryptionAtRestOptions(
                enabled=True
            ),
            enforce_https=True,
            removal_policy=RemovalPolicy.DESTROY  # For demo purposes
        )
        
        # Add access policy for Bedrock
        domain.add_access_policies(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal("bedrock.amazonaws.com")],
                actions=["es:*"],
                resources=[f"{domain.domain_arn}/*"]
            )
        )
        
        return domain

    def _create_knowledge_base(self) -> bedrock.CfnKnowledgeBase:
        """Create Bedrock Knowledge Base."""
        # Create IAM role for Knowledge Base
        kb_role = iam.Role(
            self,
            "KnowledgeBaseRole",
            assumed_by=iam.ServicePrincipal("bedrock.amazonaws.com"),
            inline_policies={
                "KnowledgeBasePolicy": iam.PolicyDocument(
                    statements=[
                        # S3 permissions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:GetObject",
                                "s3:ListBucket"
                            ],
                            resources=[
                                self.ingestion_bucket.bucket_arn,
                                f"{self.ingestion_bucket.bucket_arn}/*"
                            ]
                        ),
                        # OpenSearch permissions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "es:ESHttpPost",
                                "es:ESHttpPut",
                                "es:ESHttpGet",
                                "es:ESHttpDelete",
                                "es:ESHttpHead"
                            ],
                            resources=[f"{self.opensearch_domain.domain_arn}/*"]
                        ),
                        # Bedrock model permissions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "bedrock:InvokeModel"
                            ],
                            resources=[
                                "arn:aws:bedrock:*::foundation-model/amazon.titan-embed-text-v1"
                            ]
                        )
                    ]
                )
            }
        )
        
        # Create Knowledge Base
        knowledge_base = bedrock.CfnKnowledgeBase(
            self,
            "InvoiceAuditKnowledgeBase",
            name="invoice-audit-knowledge-base",
            description="Knowledge base for invoice auditing with extracted data and MSA standards",
            role_arn=kb_role.role_arn,
            knowledge_base_configuration=bedrock.CfnKnowledgeBase.KnowledgeBaseConfigurationProperty(
                type="VECTOR",
                vector_knowledge_base_configuration=bedrock.CfnKnowledgeBase.VectorKnowledgeBaseConfigurationProperty(
                    embedding_model_arn="arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
                )
            ),
            storage_configuration=bedrock.CfnKnowledgeBase.StorageConfigurationProperty(
                type="OPENSEARCH_DOMAIN",
                opensearch_configuration=bedrock.CfnKnowledgeBase.OpenSearchConfigurationProperty(
                    domain_endpoint=self.opensearch_domain.domain_endpoint,
                    index_name="invoice-audit-index",
                    field_mapping=bedrock.CfnKnowledgeBase.OpenSearchFieldMappingProperty(
                        vector_field="vector",
                        text_field="text",
                        metadata_field="metadata"
                    )
                )
            )
        )
        
        return knowledge_base

    def _create_bedrock_agent(self) -> bedrock.CfnAgent:
        """Create Bedrock Agent for invoice auditing."""
        # Create IAM role for Agent
        agent_role = iam.Role(
            self,
            "BedrockAgentRole",
            assumed_by=iam.ServicePrincipal("bedrock.amazonaws.com"),
            inline_policies={
                "AgentPolicy": iam.PolicyDocument(
                    statements=[
                        # Bedrock model permissions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "bedrock:InvokeModel"
                            ],
                            resources=[
                                "arn:aws:bedrock:*::foundation-model/anthropic.claude-3-5-sonnet-20241022-v2:0"
                            ]
                        ),
                        # Knowledge Base permissions
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "bedrock:Retrieve",
                                "bedrock:RetrieveAndGenerate"
                            ],
                            resources=[f"arn:aws:bedrock:*:*:knowledge-base/{self.knowledge_base.attr_knowledge_base_id}"]
                        ),
                        # Lambda permissions for action groups
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "lambda:InvokeFunction"
                            ],
                            resources=[self.extraction_lambda.function_arn]
                        )
                    ]
                )
            }
        )
        
        # Define agent instruction
        agent_instruction = """
        You are an expert invoice audit assistant specializing in Master Services Agreement (MSA) compliance.
        
        Your primary responsibilities:
        1. Analyze extracted invoice data for compliance with MSA standards
        2. Compare labor rates against established MSA rate tables
        3. Identify overtime violations (>40 hours/week per worker)
        4. Flag discrepancies in labor classifications and rates
        5. Provide detailed audit reports with actionable recommendations
        
        When auditing invoices:
        - Focus on labor rate variances exceeding 5% tolerance
        - Check for proper labor type classifications (RS, US, SS, SU, EN)
        - Verify overtime calculations and thresholds
        - Cross-reference against historical MSA data in the knowledge base
        - Provide specific recommendations for each discrepancy found
        
        Always be thorough, accurate, and provide clear explanations for your findings.
        Use the knowledge base to access relevant MSA standards and historical data.
        """
        
        # Create Bedrock Agent
        agent = bedrock.CfnAgent(
            self,
            "InvoiceAuditAgent",
            agent_name="invoice-audit-agent",
            description="AI agent for auditing invoices against MSA standards",
            foundation_model="anthropic.claude-3-5-sonnet-20241022-v2:0",
            instruction=agent_instruction,
            agent_resource_role_arn=agent_role.role_arn,
            idle_session_ttl_in_seconds=1800,  # 30 minutes
            knowledge_bases=[
                bedrock.CfnAgent.AgentKnowledgeBaseProperty(
                    knowledge_base_id=self.knowledge_base.attr_knowledge_base_id,
                    description="Knowledge base containing extracted invoice data and MSA standards",
                    knowledge_base_state="ENABLED"
                )
            ],
            action_groups=[
                bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="extraction-actions",
                    description="Actions for extracting and processing invoice data",
                    action_group_state="ENABLED",
                    action_group_executor=bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=self.extraction_lambda.function_arn
                    ),
                    api_schema=bedrock.CfnAgent.APISchemaProperty(
                        payload=json.dumps({
                            "openapi": "3.0.0",
                            "info": {
                                "title": "Invoice Extraction API",
                                "version": "1.0.0",
                                "description": "API for extracting data from invoices"
                            },
                            "paths": {
                                "/extract": {
                                    "post": {
                                        "summary": "Extract data from invoice",
                                        "description": "Extract structured data from PDF or Excel invoice",
                                        "operationId": "extractInvoiceData",
                                        "requestBody": {
                                            "required": True,
                                            "content": {
                                                "application/json": {
                                                    "schema": {
                                                        "type": "object",
                                                        "properties": {
                                                            "bucket": {"type": "string"},
                                                            "key": {"type": "string"},
                                                            "task": {"type": "string", "default": "extract"}
                                                        },
                                                        "required": ["bucket", "key"]
                                                    }
                                                }
                                            }
                                        },
                                        "responses": {
                                            "200": {
                                                "description": "Extraction completed successfully",
                                                "content": {
                                                    "application/json": {
                                                        "schema": {
                                                            "type": "object",
                                                            "properties": {
                                                                "extraction_status": {"type": "string"},
                                                                "extracted_data": {"type": "object"},
                                                                "normalized_data": {"type": "object"}
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        })
                    )
                )
            ],
            guardrail_configuration=bedrock.CfnAgent.GuardrailConfigurationProperty(
                guardrail_identifier="NONE"  # Can be enhanced with custom guardrails
            )
        )
        
        # Create Agent Alias and store identifier for downstream consumers
        self.agent_alias = bedrock.CfnAgentAlias(
            self,
            "InvoiceAuditAgentAlias",
            agent_id=agent.attr_agent_id,
            agent_alias_name="production",
            description="Production alias for invoice audit agent"
        )
        
        return agent

    def _create_agent_lambda(self) -> _lambda.Function:
        """Create Lambda function for agent interactions."""
        # Create IAM role for Agent Lambda
        agent_lambda_role = iam.Role(
            self,
            "AgentLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )
        
        # Add permissions for agent Lambda
        agent_lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    # Bedrock Agent permissions
                    "bedrock:InvokeAgent",
                    "bedrock:InvokeModel",
                    # DynamoDB permissions
                    "dynamodb:GetItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    # Lambda permissions
                    "lambda:InvokeFunction",
                    # S3 permissions
                    "s3:GetObject",
                    "s3:HeadObject"
                ],
                resources=["*"]
            )
        )
        
        # Create Agent Lambda function
        agent_lambda = _lambda.Function(
            self,
            "AgentLambda",
            function_name="agent-lambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="agent_lambda.lambda_handler",
            code=_lambda.Code.from_asset(
                "lambda",
                bundling=_lambda.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r agent_lambda_requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            ),
            role=agent_lambda_role,
            timeout=Duration.minutes(10),
            memory_size=512,
            environment={
                "BEDROCK_AGENT_ID": self.bedrock_agent.attr_agent_id,
                "BEDROCK_AGENT_ALIAS_ID": self.agent_alias.attr_agent_alias_id,
                "KNOWLEDGE_BASE_ID": self.knowledge_base.attr_knowledge_base_id,
                "MSA_RATES_TABLE": self.msa_rates_table.table_name,
                "EXTRACTION_LAMBDA_NAME": self.extraction_lambda.function_name,
                "BUCKET_NAME": self.ingestion_bucket.bucket_name,
                "LOG_LEVEL": "INFO"
            },
            log_retention=logs.RetentionDays.ONE_WEEK
        )
        
        # Grant permissions to access DynamoDB table
        self.msa_rates_table.grant_read_data(agent_lambda)
        
        return agent_lambda

    def _create_sagemaker_endpoint(self) -> sagemaker.CfnEndpoint:
        """Create SageMaker endpoint for anomaly detection."""
        # Create IAM role for SageMaker
        sagemaker_role = iam.Role(
            self,
            "SageMakerRole",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess")
            ]
        )
        
        # Create SageMaker model for isolation forest
        model = sagemaker.CfnModel(
            self,
            "AnomalyDetectionModel",
            model_name="invoice-anomaly-detection",
            execution_role_arn=sagemaker_role.role_arn,
            primary_container=sagemaker.CfnModel.ContainerDefinitionProperty(
                image="382416733822.dkr.ecr.us-east-1.amazonaws.com/xgboost:latest",
                model_data_url="s3://sagemaker-sample-files/datasets/tabular/synthetic/isolation_forest_model.tar.gz"
            )
        )
        
        # Create endpoint configuration
        endpoint_config = sagemaker.CfnEndpointConfig(
            self,
            "AnomalyDetectionEndpointConfig",
            endpoint_config_name="invoice-anomaly-detection-config",
            production_variants=[
                sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                    variant_name="primary",
                    model_name=model.model_name,
                    initial_instance_count=1,
                    instance_type="ml.t2.medium",
                    initial_variant_weight=1.0
                )
            ]
        )
        
        # Create endpoint
        endpoint = sagemaker.CfnEndpoint(
            self,
            "AnomalyDetectionEndpoint",
            endpoint_name="invoice-anomaly-detection",
            endpoint_config_name=endpoint_config.endpoint_config_name
        )
        
        endpoint.add_dependency(model)
        endpoint.add_dependency(endpoint_config)
        
        return endpoint

    def _create_comparison_lambda(self) -> _lambda.Function:
        """Create Lambda function for comparison and discrepancy flagging."""
        # Create IAM role for Comparison Lambda
        comparison_lambda_role = iam.Role(
            self,
            "ComparisonLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )
        
        # Add permissions for comparison Lambda
        comparison_lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    # Bedrock permissions
                    "bedrock:InvokeModel",
                    "bedrock:Retrieve",
                    "bedrock:RetrieveAndGenerate",
                    # DynamoDB permissions
                    "dynamodb:GetItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    # SageMaker permissions
                    "sagemaker:InvokeEndpoint",
                    # S3 permissions
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:HeadObject"
                ],
                resources=["*"]
            )
        )
        
        # Create Comparison Lambda function
        comparison_lambda = _lambda.Function(
            self,
            "ComparisonLambda",
            function_name="comparison-lambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="comparison_lambda.lambda_handler",
            code=_lambda.Code.from_asset(
                "lambda",
                bundling=_lambda.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r comparison_lambda_requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            ),
            role=comparison_lambda_role,
            timeout=Duration.minutes(15),
            memory_size=1024,
            environment={
                "MSA_RATES_TABLE": self.msa_rates_table.table_name,
                "KNOWLEDGE_BASE_ID": self.knowledge_base.attr_knowledge_base_id,
                "SAGEMAKER_ENDPOINT": self.sagemaker_endpoint.endpoint_name,
                "BUCKET_NAME": self.ingestion_bucket.bucket_name,
                "BEDROCK_MODEL_ID": "anthropic.claude-3-5-sonnet-20241022-v2:0",
                "LOG_LEVEL": "INFO",
                "VARIANCE_THRESHOLD": "0.05",  # 5% threshold
                "OVERTIME_THRESHOLD": "40.0"   # 40 hours/week
            },
            log_retention=logs.RetentionDays.ONE_WEEK
        )
        
        # Grant permissions to access DynamoDB table
        self.msa_rates_table.grant_read_data(comparison_lambda)
        
        return comparison_lambda

    def _create_report_lambda(self) -> _lambda.Function:
        """Create Lambda function for report generation."""
        # Create S3 buckets for reports and templates
        reports_bucket = s3.Bucket(
            self,
            "ReportsBucket",
            bucket_name=f"msa-audit-reports-{self.account}-{self.region}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,  # For demo purposes
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ReportsLifecycle",
                    enabled=True,
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30)
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90)
                        )
                    ]
                )
            ]
        )
        
        templates_bucket = s3.Bucket(
            self,
            "TemplatesBucket",
            bucket_name=f"msa-audit-templates-{self.account}-{self.region}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY  # For demo purposes
        )
        
        # Create IAM role for Report Lambda
        report_lambda_role = iam.Role(
            self,
            "ReportLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )
        
        # Add permissions for report Lambda
        report_lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    # Bedrock permissions
                    "bedrock:InvokeModel",
                    # S3 permissions for reports and templates
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:HeadObject",
                    "s3:ListBucket",
                    # DynamoDB permissions (for metadata)
                    "dynamodb:GetItem",
                    "dynamodb:Query"
                ],
                resources=[
                    reports_bucket.bucket_arn,
                    f"{reports_bucket.bucket_arn}/*",
                    templates_bucket.bucket_arn,
                    f"{templates_bucket.bucket_arn}/*",
                    self.ingestion_bucket.bucket_arn,
                    f"{self.ingestion_bucket.bucket_arn}/*",
                    self.msa_rates_table.table_arn
                ]
            )
        )
        
        # Create Report Lambda function
        report_lambda = _lambda.Function(
            self,
            "ReportLambda",
            function_name="report-lambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="report_lambda.lambda_handler",
            code=_lambda.Code.from_asset(
                "lambda",
                bundling=_lambda.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r report_lambda_requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            ),
            role=report_lambda_role,
            timeout=Duration.minutes(15),
            memory_size=2048,  # Higher memory for Excel/PDF processing
            environment={
                "BEDROCK_MODEL_ID": "anthropic.claude-3-5-sonnet-20241022-v2:0",
                "REPORTS_BUCKET": reports_bucket.bucket_name,
                "TEMPLATE_BUCKET": templates_bucket.bucket_name,
                "TEMPLATE_KEY": "XXXI_Template.xlsx",
                "MSA_RATES_TABLE": self.msa_rates_table.table_name,
                "LOG_LEVEL": "INFO"
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
            layers=[
                # Add wkhtmltopdf layer for PDF generation
                _lambda.LayerVersion(
                    self,
                    "WkhtmltopdfLayer",
                    code=_lambda.Code.from_asset("lambda/layers/wkhtmltopdf"),
                    compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
                    description="wkhtmltopdf binary for PDF generation"
                )
            ]
        )
        
        # Grant permissions to access buckets and DynamoDB
        reports_bucket.grant_read_write(report_lambda)
        templates_bucket.grant_read(report_lambda)
        self.msa_rates_table.grant_read_data(report_lambda)
        
        # Store bucket references for outputs
        self.reports_bucket = reports_bucket
        self.templates_bucket = templates_bucket
        
        return report_lambda

    def _create_step_function(self) -> sfn.StateMachine:
        """Create Step Functions workflow for invoice processing."""
        # Define extraction task
        extraction_task = sfn_tasks.LambdaInvoke(
            self,
            "ExtractInvoiceData",
            lambda_function=self.extraction_lambda,
            payload=sfn.TaskInput.from_object({
                "bucket.$": "$.bucket",
                "key.$": "$.key",
                "task": "extract"
            }),
            result_path="$.extraction_result"
        )
        
        # Define comparison task
        comparison_task = sfn_tasks.LambdaInvoke(
            self,
            "CompareAndFlagDiscrepancies",
            lambda_function=self.comparison_lambda,
            payload=sfn.TaskInput.from_object({
                "extraction_data.$": "$.extraction_result.Payload.extracted_data",
                "bucket.$": "$.bucket",
                "key.$": "$.key"
            }),
            result_path="$.comparison_result"
        )
        
        # Define HITL Choice state for approval workflow
        hitl_choice = sfn.Choice(
            self,
            "HITLApprovalChoice",
            comment="Check if human approval is required based on discrepancy threshold"
        )
        
        # Define Wait state for human approval
        hitl_wait = sfn.Wait(
            self,
            "WaitForHumanApproval",
            comment="Waiting for human approval of high-value discrepancies",
            time=sfn.WaitTime.duration(Duration.hours(24))  # 24 hour timeout
        )
        
        # Define Task state for human approval
        hitl_approval_task = sfn_tasks.LambdaInvoke(
            self,
            "HumanApprovalTask",
            lambda_function=self.agent_lambda,
            payload=sfn.TaskInput.from_object({
                "action": "hitl_approval",
                "comparison_result.$": "$.comparison_result.Payload",
                "bucket.$": "$.bucket",
                "key.$": "$.key"
            }),
            result_path="$.hitl_result"
        )
        
        # Define agent analysis task
        agent_task = sfn_tasks.LambdaInvoke(
            self,
            "AgentAnalysis",
            lambda_function=self.agent_lambda,
            payload=sfn.TaskInput.from_object({
                "extraction_data.$": "$.extraction_result.Payload.extracted_data",
                "comparison_result.$": "$.comparison_result.Payload",
                "bucket.$": "$.bucket",
                "key.$": "$.key",
                "task": "analyze_discrepancies"
            }),
            result_path="$.agent_result"
        )
        
        # Define report generation task
        report_task = sfn_tasks.LambdaInvoke(
            self,
            "GenerateReport",
            lambda_function=self.report_lambda,
            payload=sfn.TaskInput.from_object({
                "task": "generate_report",
                "flags_data.$": "$.comparison_result.Payload.discrepancy_analysis",
                "metadata": {
                    "invoice_number.$": "$.extraction_result.Payload.extracted_data.invoice_number",
                    "vendor.$": "$.extraction_result.Payload.extracted_data.vendor",
                    "date_of_loss": "2/12/2025",
                    "invoice_total.$": "$.extraction_result.Payload.extracted_data.total_amount",
                    "labor_total": 77000,
                    "material_total": 71478.04,
                    "page_count.$": "$.extraction_result.Payload.extracted_data.page_count",
                    "file_size.$": "$.extraction_result.Payload.processing_summary.file_size"
                },
                "extracted_data.$": "$.extraction_result.Payload"
            }),
            result_path="$.report_result"
        )
        
        # Define success state
        success_state = sfn.Succeed(
            self,
            "ProcessingComplete",
            comment="Invoice processing and report generation completed successfully"
        )
        
        # Define failure state
        failure_state = sfn.Fail(
            self,
            "ProcessingFailed",
            comment="Invoice processing failed"
        )
        
        # Create error handling
        extraction_task.add_catch(
            failure_state,
            errors=["States.ALL"],
            result_path="$.error"
        )
        
        comparison_task.add_catch(
            failure_state,
            errors=["States.ALL"],
            result_path="$.error"
        )
        
        agent_task.add_catch(
            failure_state,
            errors=["States.ALL"],
            result_path="$.error"
        )
        
        report_task.add_catch(
            failure_state,
            errors=["States.ALL"],
            result_path="$.error"
        )
        
        # Add HITL choice logic
        hitl_choice.when(
            sfn.Condition.number_greater_than(
                "$.comparison_result.Payload.discrepancy_analysis.summary.total_potential_savings", 1000
            ),
            hitl_wait.next(hitl_approval_task).next(agent_task)
        ).otherwise(agent_task)
        
        # Chain the tasks
        definition = extraction_task.next(
            comparison_task.next(
                hitl_choice.next(
                    agent_task.next(
                        report_task.next(success_state)
                    )
                )
            )
        )
        
        # Create IAM role for Step Functions
        step_function_role = iam.Role(
            self,
            "StepFunctionRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            inline_policies={
                "StepFunctionPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "lambda:InvokeFunction"
                            ],
                            resources=[
                                self.extraction_lambda.function_arn,
                                self.comparison_lambda.function_arn,
                                self.agent_lambda.function_arn,
                                self.report_lambda.function_arn
                            ]
                        )
                    ]
                )
            }
        )
        
        # Create Step Functions state machine
        state_machine = sfn.StateMachine(
            self,
            "InvoiceProcessingWorkflow",
            state_machine_name="invoice-processing-workflow",
            definition=definition,
            role=step_function_role,
            timeout=Duration.minutes(30),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self,
                    "StepFunctionLogs",
                    log_group_name="/aws/stepfunctions/invoice-processing",
                    retention=logs.RetentionDays.ONE_WEEK,
                    removal_policy=RemovalPolicy.DESTROY
                ),
                level=sfn.LogLevel.ALL
            )
        )
        
        return state_machine

    def _populate_msa_rates(self) -> None:
        """Populate MSA rates table with sample data."""
        # Sample MSA rates data
        sample_rates = [
            {"labor_type": "RS", "location": "default", "standard_rate": 70.00, "description": "Regular Skilled Labor"},
            {"labor_type": "US", "location": "default", "standard_rate": 85.00, "description": "Unskilled Supervisor"},
            {"labor_type": "SS", "location": "default", "standard_rate": 95.00, "description": "Skilled Supervisor"},
            {"labor_type": "SU", "location": "default", "standard_rate": 110.00, "description": "Senior Supervisor"},
            {"labor_type": "EN", "location": "default", "standard_rate": 125.00, "description": "Engineer"},
            # Overtime rules
            {"labor_type": "default", "location": "overtime_rules", "weekly_threshold": 40.0, "description": "Standard overtime threshold"},
            {"labor_type": "EN", "location": "overtime_rules", "weekly_threshold": 45.0, "description": "Engineer overtime threshold"},
        ]
        
        # Create custom resource to populate table
        populate_function = _lambda.Function(
            self,
            "PopulateMSARatesFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=_lambda.Code.from_inline(f"""
import boto3
import json

def handler(event, context):
    if event['RequestType'] == 'Delete':
        return {{'Status': 'SUCCESS', 'PhysicalResourceId': 'populate-msa-rates'}}
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('{self.msa_rates_table.table_name}')
    
    sample_rates = {json.dumps(sample_rates)}
    
    try:
        for rate in sample_rates:
            table.put_item(Item=rate)
        
        return {{
            'Status': 'SUCCESS',
            'PhysicalResourceId': 'populate-msa-rates',
            'Data': {{'Message': 'MSA rates populated successfully'}}
        }}
    except Exception as e:
        return {{
            'Status': 'FAILED',
            'PhysicalResourceId': 'populate-msa-rates',
            'Reason': str(e)
        }}
            """),
            timeout=Duration.minutes(5)
        )
        
        # Grant permissions to populate function
        self.msa_rates_table.grant_write_data(populate_function)
        
        # Create custom resource
        # CustomResource(
        #     self,
        #     "PopulateMSARates",
        #     service_token=populate_function.function_arn,
        #     properties={
        #         "TableName": self.msa_rates_table.table_name
        #     }
        # )

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs for important resources."""
        CfnOutput(
            self,
            "BedrockAgentId",
            value=self.bedrock_agent.attr_agent_id,
            description="Bedrock Agent ID for invoice auditing"
        )
        
        CfnOutput(
            self,
            "KnowledgeBaseId",
            value=self.knowledge_base.attr_knowledge_base_id,
            description="Knowledge Base ID for invoice data"
        )
        
        CfnOutput(
            self,
            "MSARatesTableName",
            value=self.msa_rates_table.table_name,
            description="DynamoDB table name for MSA rates"
        )
        
        CfnOutput(
            self,
            "AgentLambdaFunctionName",
            value=self.agent_lambda.function_name,
            description="Agent Lambda function name"
        )
        
        CfnOutput(
            self,
            "AgentLambdaFunctionArn",
            value=self.agent_lambda.function_arn,
            description="Agent Lambda function ARN"
        )
        
        CfnOutput(
            self,
            "OpenSearchDomainEndpoint",
            value=self.opensearch_domain.domain_endpoint,
            description="OpenSearch domain endpoint for Knowledge Base"
        )
        
        CfnOutput(
            self,
            "ComparisonLambdaFunctionName",
            value=self.comparison_lambda.function_name,
            description="Comparison Lambda function name"
        )
        
        CfnOutput(
            self,
            "ComparisonLambdaFunctionArn",
            value=self.comparison_lambda.function_arn,
            description="Comparison Lambda function ARN"
        )
        
        CfnOutput(
            self,
            "SageMakerEndpointName",
            value=self.sagemaker_endpoint.endpoint_name,
            description="SageMaker endpoint name for anomaly detection"
        )
        
        CfnOutput(
            self,
            "StepFunctionArn",
            value=self.step_function.state_machine_arn,
            description="Step Functions state machine ARN for invoice processing workflow"
        )
        
        CfnOutput(
            self,
            "StepFunctionName",
            value=self.step_function.state_machine_name,
            description="Step Functions state machine name"
        )
        
        CfnOutput(
            self,
            "ReportLambdaFunctionName",
            value=self.report_lambda.function_name,
            description="Report Lambda function name"
        )
        
        CfnOutput(
            self,
            "ReportLambdaFunctionArn",
            value=self.report_lambda.function_arn,
            description="Report Lambda function ARN"
        )
        
        CfnOutput(
            self,
            "ReportsBucketName",
            value=self.reports_bucket.bucket_name,
            description="S3 bucket name for generated reports"
        )
        
        CfnOutput(
            self,
            "TemplatesBucketName",
            value=self.templates_bucket.bucket_name,
            description="S3 bucket name for Excel templates"
        )
