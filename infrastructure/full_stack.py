"""
MSA Invoice Auditing System - Full Integrated Stack

This module defines a comprehensive CDK stack that deploys the complete
MSA Invoice Auditing System with all components integrated.
"""

from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_iam as iam,
    aws_dynamodb as dynamodb,
    aws_bedrock as bedrock,
    aws_opensearch as opensearch,
    aws_sagemaker as sagemaker,
    aws_apprunner as apprunner,
    aws_ecr as ecr,
    aws_ec2 as ec2,
    aws_logs as logs,
    custom_resources as cr,
    CfnOutput,
    Duration,
    RemovalPolicy
)
from constructs import Construct
import json


class MSAInvoiceAuditFullStack(Stack):
    """
    Complete MSA Invoice Auditing System Stack.
    
    Deploys all components in a single, integrated stack:
    - S3 buckets for ingestion, reports, templates, and knowledge base
    - Lambda functions for extraction, agent, comparison, and reporting
    - Step Functions workflow orchestrating the complete process
    - Bedrock Agent with Knowledge Base and OpenSearch
    - DynamoDB table for MSA rates
    - SageMaker endpoint for anomaly detection
    - Streamlit UI on App Runner
    """
    
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Create S3 buckets
        self._create_s3_buckets()
        
        # Create DynamoDB table
        self._create_dynamodb_table()
        
        # Create Lambda layers
        self._create_lambda_layers()
        
        # Create Lambda functions
        self._create_lambda_functions()
        
        # Create SageMaker endpoint
        self._create_sagemaker_endpoint()
        
        # Create Bedrock components
        self._create_bedrock_components()
        
        # Create Step Functions workflow
        self._create_step_functions_workflow()
        
        # Create UI components
        self._create_ui_components()
        
        # Create outputs
        self._create_outputs()
    
    def _bucket_name(self, purpose: str) -> str:
        stack_id = self.stack_name.lower().replace("_", "-")
        return f"{stack_id}-{purpose}-{self.account}-{self.region}"

    def _create_bucket(self, logical_id: str, purpose: str) -> s3.Bucket:
        return s3.Bucket(
            self,
            logical_id,
            bucket_name=self._bucket_name(purpose),
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(90)
                )
            ]
        )

    def _create_s3_buckets(self) -> None:
        """Create all required S3 buckets."""
        self.ingestion_bucket = self._create_bucket("MSAIngestionBucket", "ingestion")
        self.reports_bucket = self._create_bucket("MSAReportsBucket", "reports")
        self.templates_bucket = self._create_bucket("MSATemplatesBucket", "templates")
        self.knowledge_base_bucket = self._create_bucket("MSAKnowledgeBaseBucket", "knowledge")
    
    def _create_dynamodb_table(self) -> None:
        """Create DynamoDB table for MSA rates."""
        table_name = f"{self._bucket_name('msa-rates')}"
        self.msa_rates_table = dynamodb.Table(
            self,
            "MSARatesTable",
            table_name=table_name,
            partition_key=dynamodb.Attribute(
                name="rate_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="effective_date",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery=True
        )

        self._seed_msa_rates()

    def _seed_msa_rates(self) -> None:
        """Seed DynamoDB table with baseline rates using a custom resource."""
        seed_function = _lambda.Function(
            self,
            "MSARateSeedFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=_lambda.Code.from_inline(
                """
import boto3
from decimal import Decimal

DEFAULT_DATE = "2024-01-01"

def handler(event, _context):
    physical_id = "msa-rate-seed"
    if event.get("RequestType") == "Delete":
        return {"PhysicalResourceId": physical_id}

    table_name = event["ResourceProperties"]["TableName"]
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    sample_rates = [
        {"rate_id": "RS#default", "effective_date": DEFAULT_DATE, "labor_type": "RS", "location": "default", "standard_rate": "70.00", "description": "Regular Skilled Labor"},
        {"rate_id": "US#default", "effective_date": DEFAULT_DATE, "labor_type": "US", "location": "default", "standard_rate": "85.00", "description": "Unskilled Supervisor"},
        {"rate_id": "SS#default", "effective_date": DEFAULT_DATE, "labor_type": "SS", "location": "default", "standard_rate": "95.00", "description": "Skilled Supervisor"},
        {"rate_id": "SU#default", "effective_date": DEFAULT_DATE, "labor_type": "SU", "location": "default", "standard_rate": "110.00", "description": "Senior Supervisor"},
        {"rate_id": "EN#default", "effective_date": DEFAULT_DATE, "labor_type": "EN", "location": "default", "standard_rate": "125.00", "description": "Engineer"},
        {"rate_id": "default#overtime_rules", "effective_date": DEFAULT_DATE, "labor_type": "default", "location": "overtime_rules", "weekly_threshold": "40", "description": "Standard overtime threshold"},
        {"rate_id": "SU#ratio_rules", "effective_date": DEFAULT_DATE, "labor_type": "SU", "location": "ratio_rules", "max_ratio": "6"}
    ]

    for item in sample_rates:
        formatted = {k: (Decimal(str(v)) if k in {"standard_rate", "weekly_threshold", "max_ratio"} else v) for k, v in item.items()}
        table.put_item(Item=formatted)

    return {"PhysicalResourceId": physical_id}
                """
            ),
            timeout=Duration.minutes(5)
        )

        self.msa_rates_table.grant_write_data(seed_function)

        provider = cr.Provider(
            self,
            "MSARateSeedProvider",
            on_event_handler=seed_function
        )

        cr.CustomResource(
            self,
            "MSARateSeedResource",
            service_token=provider.service_token,
            properties={
                "TableName": self.msa_rates_table.table_name
            }
        )
    
    def _create_lambda_layers(self) -> None:
        """Create Lambda layers for shared dependencies."""
        # Common utilities layer
        self.common_layer = _lambda.LayerVersion(
            self,
            "MSACommonLayer",
            code=_lambda.Code.from_asset(
                "lambda/layers/common",
                bundling=_lambda.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "set -euo pipefail; pip install --platform manylinux2014_x86_64 --only-binary=:all: -r ../../requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            ),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
            description="Common utilities for MSA Invoice Auditing System"
        )
        
        # Layers now rely on root requirements.txt during bundling
    
    def _create_lambda_functions(self) -> None:
        """Create all Lambda functions."""

        def create_lambda_role() -> iam.Role:
            role = iam.Role(
                self,
                "MSALambdaBaseRole",
                assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
                ]
            )

            role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
                    resources=[
                        self.ingestion_bucket.bucket_arn,
                        f"{self.ingestion_bucket.bucket_arn}/*",
                        self.reports_bucket.bucket_arn,
                        f"{self.reports_bucket.bucket_arn}/*",
                        self.templates_bucket.bucket_arn,
                        f"{self.templates_bucket.bucket_arn}/*",
                        self.knowledge_base_bucket.bucket_arn,
                        f"{self.knowledge_base_bucket.bucket_arn}/*"
                    ]
                )
            )

            role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:Query",
                        "dynamodb:Scan"
                    ],
                    resources=[self.msa_rates_table.table_arn]
                )
            )

            role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "textract:DetectDocumentText",
                        "textract:AnalyzeDocument",
                        "textract:StartDocumentAnalysis",
                        "textract:GetDocumentAnalysis"
                    ],
                    resources=["*"]
                )
            )

            role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "bedrock:InvokeAgent",
                        "bedrock:InvokeModel",
                        "bedrock:GetAgent",
                        "bedrock:ListAgents"
                    ],
                    resources=["*"]
                )
            )

            return role

        lambda_base_role = create_lambda_role()

        bundling = _lambda.BundlingOptions(
            image=_lambda.Runtime.PYTHON_3_11.bundling_image,
            command=[
                "bash",
                "-c",
                "set -eux; pip install --platform manylinux2014_x86_64 --only-binary=:all: -r ../requirements.txt -t /asset-output; cp -au . /asset-output"
            ]
        )

        def create_function(logical_id: str, handler: str, env: dict, memory: int = 1024, timeout_minutes: int = 15) -> _lambda.Function:
            return _lambda.Function(
                self,
                logical_id,
                runtime=_lambda.Runtime.PYTHON_3_11,
                handler=handler,
                code=_lambda.Code.from_asset("lambda", bundling=bundling),
                role=lambda_base_role,
                timeout=Duration.minutes(timeout_minutes),
                memory_size=memory,
                layers=[self.common_layer],
                environment=env
            )

        default_effective_date = "2024-01-01"

        self.ingestion_lambda = create_function(
            "MSAIngestionLambda",
            "ingestion_lambda.lambda_handler",
            {
                "BUCKET_NAME": self.ingestion_bucket.bucket_name
            }
        )

        self.agent_lambda = create_function(
            "MSAAgentLambda",
            "agent_lambda.lambda_handler",
            {
                "MSA_RATES_TABLE": self.msa_rates_table.table_name,
                "KNOWLEDGE_BASE_BUCKET": self.knowledge_base_bucket.bucket_name,
                "MSA_DEFAULT_EFFECTIVE_DATE": default_effective_date
            },
            memory=2048
        )

        self.comparison_lambda = create_function(
            "MSAComparisonLambda",
            "comparison_lambda.lambda_handler",
            {
                "MSA_RATES_TABLE": self.msa_rates_table.table_name,
                "MSA_DEFAULT_EFFECTIVE_DATE": default_effective_date
            },
            timeout_minutes=10
        )

        self.report_lambda = create_function(
            "MSAReportLambda",
            "report_lambda.lambda_handler",
            {
                "REPORTS_BUCKET": self.reports_bucket.bucket_name,
                "TEMPLATES_BUCKET": self.templates_bucket.bucket_name
            },
            memory=2048
        )
    
    def _create_sagemaker_endpoint(self) -> None:
        """Create SageMaker endpoint for anomaly detection."""
        # SageMaker execution role
        sagemaker_role = iam.Role(
            self,
            "MSASageMakerRole",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess")
            ]
        )
        
        # SageMaker model (using built-in algorithm for anomaly detection)
        self.sagemaker_model = sagemaker.CfnModel(
            self,
            "MSAAnomalyDetectionModel",
            execution_role_arn=sagemaker_role.role_arn,
            model_name="msa-anomaly-detection-model",
            primary_container=sagemaker.CfnModel.ContainerDefinitionProperty(
                image="382416733822.dkr.ecr.us-east-1.amazonaws.com/xgboost:latest",
                mode="SingleModel"
            )
        )
        
        # SageMaker endpoint configuration
        self.sagemaker_endpoint_config = sagemaker.CfnEndpointConfig(
            self,
            "MSAAnomalyDetectionEndpointConfig",
            endpoint_config_name="msa-anomaly-detection-endpoint-config",
            production_variants=[
                sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                    model_name=self.sagemaker_model.model_name,
                    variant_name="primary",
                    initial_instance_count=1,
                    instance_type="ml.t2.medium",
                    initial_variant_weight=1.0
                )
            ]
        )
        
        # SageMaker endpoint
        self.sagemaker_endpoint = sagemaker.CfnEndpoint(
            self,
            "MSAAnomalyDetectionEndpoint",
            endpoint_name="msa-anomaly-detection-endpoint",
            endpoint_config_name=self.sagemaker_endpoint_config.endpoint_config_name
        )
        
        # Add SageMaker permissions to Lambda role
        self.agent_lambda.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "sagemaker:InvokeEndpoint"
                ],
                resources=[
                    f"arn:aws:sagemaker:{self.region}:{self.account}:endpoint/{self.sagemaker_endpoint.endpoint_name}"
                ]
            )
        )
    
    def _create_bedrock_components(self) -> None:
        """Create Bedrock Agent and Knowledge Base."""
        # OpenSearch domain for Knowledge Base
        self.opensearch_domain = opensearch.Domain(
            self,
            "MSAKnowledgeBaseOpenSearch",
            version=opensearch.EngineVersion.OPENSEARCH_2_5,
            capacity=opensearch.CapacityConfig(
                data_nodes=1,
                data_node_instance_type="t3.small.search"
            ),
            ebs=opensearch.EbsOptions(
                volume_size=10,
                volume_type=ec2.EbsDeviceVolumeType.GP3
            ),
            zone_awareness=opensearch.ZoneAwarenessConfig(
                enabled=False
            ),
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # Bedrock Agent execution role
        bedrock_agent_role = iam.Role(
            self,
            "MSABedrockAgentRole",
            assumed_by=iam.ServicePrincipal("bedrock.amazonaws.com"),
            inline_policies={
                "BedrockAgentPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "bedrock:InvokeModel",
                                "bedrock:GetFoundationModel"
                            ],
                            resources=["*"]
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "lambda:InvokeFunction"
                            ],
                            resources=[
                                self.agent_lambda.function_arn,
                                self.comparison_lambda.function_arn
                            ]
                        )
                    ]
                )
            }
        )
        
        # Bedrock Knowledge Base
        self.knowledge_base = bedrock.CfnKnowledgeBase(
            self,
            "MSAKnowledgeBase",
            name="msa-invoice-knowledge-base",
            description="Knowledge base for MSA invoice auditing standards and procedures",
            role_arn=bedrock_agent_role.role_arn,
            knowledge_base_configuration=bedrock.CfnKnowledgeBase.KnowledgeBaseConfigurationProperty(
                type="VECTOR",
                vector_knowledge_base_configuration=bedrock.CfnKnowledgeBase.VectorKnowledgeBaseConfigurationProperty(
                    embedding_model_arn=f"arn:aws:bedrock:{self.region}::foundation-model/amazon.titan-embed-text-v1"
                )
            ),
            storage_configuration=bedrock.CfnKnowledgeBase.StorageConfigurationProperty(
                type="OPENSEARCH_DOMAIN",
                opensearch_configuration=bedrock.CfnKnowledgeBase.OpenSearchConfigurationProperty(
                    domain_endpoint=self.opensearch_domain.domain_endpoint,
                    index_name="msa-knowledge-index",
                    field_mapping=bedrock.CfnKnowledgeBase.OpenSearchFieldMappingProperty(
                        vector_field="vector",
                        text_field="text",
                        metadata_field="metadata"
                    )
                )
            )
        )
        
        # Bedrock Agent
        self.bedrock_agent = bedrock.CfnAgent(
            self,
            "MSABedrockAgent",
            agent_name="msa-invoice-audit-agent",
            description="AI agent for MSA invoice auditing and compliance checking",
            foundation_model="anthropic.claude-3-5-sonnet-20241022-v2:0",
            instruction="""You are an expert MSA (Master Services Agreement) invoice auditing agent. 
            Your role is to analyze invoices against MSA standards, identify discrepancies, 
            calculate overcharges, and suggest cost savings opportunities. 
            Use the knowledge base for MSA rates and compliance requirements.""",
            role_arn=bedrock_agent_role.role_arn,
            knowledge_bases=[
                bedrock.CfnAgent.AgentKnowledgeBaseProperty(
                    knowledge_base_id=self.knowledge_base.attr_knowledge_base_id,
                    description="MSA rates and compliance standards",
                    knowledge_base_state="ENABLED"
                )
            ],
            action_groups=[
                bedrock.CfnAgent.AgentActionGroupProperty(
                    action_group_name="msa-rate-lookup",
                    description="Look up MSA rates and perform calculations",
                    action_group_executor=bedrock.CfnAgent.ActionGroupExecutorProperty(
                        lambda_=self.agent_lambda.function_arn
                    ),
                    function_schema=bedrock.CfnAgent.FunctionSchemaProperty(
                        functions=[
                            bedrock.CfnAgent.FunctionProperty(
                                name="lookup_msa_rate",
                                description="Look up MSA rate for a specific category and subcategory"
                            ),
                            bedrock.CfnAgent.FunctionProperty(
                                name="calculate_overcharge",
                                description="Calculate overcharge amount based on MSA rates"
                            )
                        ]
                    )
                )
            ]
        )
        
        # Bedrock Agent Alias
        self.bedrock_agent_alias = bedrock.CfnAgentAlias(
            self,
            "MSABedrockAgentAlias",
            agent_alias_name="PROD",
            agent_id=self.bedrock_agent.attr_agent_id,
            description="Production alias for MSA invoice audit agent"
        )
    
    def _create_step_functions_workflow(self) -> None:
        """Create Step Functions workflow."""
        # Step Functions execution role
        sfn_role = iam.Role(
            self,
            "MSAStepFunctionsRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com")
        )

        sfn_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lambda:InvokeFunction"],
                resources=[
                    self.ingestion_lambda.function_arn,
                    self.agent_lambda.function_arn,
                    self.comparison_lambda.function_arn,
                    self.report_lambda.function_arn
                ]
            )
        )

        extract_task = sfn_tasks.LambdaInvoke(
            self,
            "ExtractData",
            lambda_function=self.ingestion_lambda,
            payload=sfn.TaskInput.from_object({
                "bucket.$": "$.bucket",
                "key.$": "$.key"
            }),
            result_path="$.extraction"
        )

        agent_task = sfn_tasks.LambdaInvoke(
            self,
            "InvokeAgent",
            lambda_function=self.agent_lambda,
            payload=sfn.TaskInput.from_object({
                "extracted_data.$": "$.extraction.Payload",
                "bucket.$": "$.bucket",
                "key.$": "$.key"
            }),
            result_path="$.agent"
        )

        comparison_task = sfn_tasks.LambdaInvoke(
            self,
            "CompareRates",
            lambda_function=self.comparison_lambda,
            payload=sfn.TaskInput.from_object({
                "extracted_data.$": "$.extraction.Payload",
                "agent_analysis.$": "$.agent.Payload",
                "bucket.$": "$.bucket",
                "key.$": "$.key"
            }),
            result_path="$.comparison"
        )

        human_approval = sfn_tasks.LambdaInvoke(
            self,
            "AwaitHumanApproval",
            lambda_function=self.agent_lambda,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            payload=sfn.TaskInput.from_object({
                "action": "hitl_approval",
                "flags.$": "$.comparison.Payload.discrepancy_analysis",
                "taskToken": sfn.JsonPath.task_token
            }),
            result_path="$.approval"
        )

        report_task = sfn_tasks.LambdaInvoke(
            self,
            "GenerateReport",
            lambda_function=self.report_lambda,
            payload=sfn.TaskInput.from_object({
                "extracted_data.$": "$.extraction.Payload",
                "agent_analysis.$": "$.agent.Payload",
                "comparison_results.$": "$.comparison.Payload",
                "bucket.$": "$.bucket",
                "key.$": "$.key"
            }),
            result_path="$.report"
        )

        agent_wait = sfn.Wait(
            self,
            "AgentStatusWait",
            time=sfn.WaitTime.duration(Duration.seconds(30))
        )

        check_agent_status = sfn.Choice(self, "CheckAgentStatus")
        check_agent_status.when(
            sfn.Condition.string_equals("$.agent.Payload.status", "pending_approval"),
            agent_wait.next(agent_task)
        )
        check_agent_status.otherwise(comparison_task)

        check_discrepancies = sfn.Choice(self, "DiscrepanciesPresent")
        check_discrepancies.when(
            sfn.Condition.number_greater_than("$.comparison.Payload.discrepancy_analysis.summary.total_discrepancies", 0),
            human_approval
        )
        check_discrepancies.otherwise(report_task)

        human_approval.next(report_task)

        definition = extract_task.next(agent_task).next(check_agent_status)
        comparison_task.next(check_discrepancies)

        self.step_function = sfn.StateMachine(
            self,
            "MSAInvoiceAuditWorkflow",
            state_machine_name="msa-invoice-audit-workflow",
            definition=definition,
            role=sfn_role,
            timeout=Duration.hours(1)
        )
    
    def _create_ui_components(self) -> None:
        """Create UI components (ECR repository and App Runner service)."""
        # ECR repository for Streamlit app
        self.ecr_repository = ecr.Repository(
            self,
            "MSAUIRepository",
            repository_name="msa-invoice-audit-ui",
            image_scan_on_push=True,
            lifecycle_rules=[
                ecr.LifecycleRule(
                    description="Keep only 10 most recent images",
                    max_image_count=10,
                    rule_priority=1
                )
            ],
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # App Runner IAM role
        app_runner_role = iam.Role(
            self,
            "MSAUIAppRunnerRole",
            assumed_by=iam.ServicePrincipal("tasks.apprunner.amazonaws.com"),
            description="IAM role for MSA Invoice Audit UI App Runner service"
        )
        
        # Add S3 permissions
        app_runner_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                resources=[
                    self.ingestion_bucket.bucket_arn,
                    f"{self.ingestion_bucket.bucket_arn}/*",
                    self.reports_bucket.bucket_arn,
                    f"{self.reports_bucket.bucket_arn}/*"
                ]
            )
        )
        
        # Add Step Functions permissions
        app_runner_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "states:StartExecution",
                    "states:DescribeExecution",
                    "states:ListExecutions"
                ],
                resources=[
                    self.step_function.state_machine_arn,
                    f"{self.step_function.state_machine_arn}:*"
                ]
            )
        )
        
        # Add Bedrock permissions
        app_runner_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "bedrock:InvokeAgent",
                    "bedrock:GetAgent",
                    "bedrock:ListAgents"
                ],
                resources=[
                    f"arn:aws:bedrock:{self.region}:{self.account}:agent/{self.bedrock_agent.attr_agent_id}",
                    f"arn:aws:bedrock:{self.region}:{self.account}:agent-alias/{self.bedrock_agent.attr_agent_id}/{self.bedrock_agent_alias.attr_agent_alias_id}"
                ]
            )
        )
        
        # App Runner access role for ECR
        access_role = iam.Role(
            self,
            "MSAUIAppRunnerAccessRole",
            assumed_by=iam.ServicePrincipal("build.apprunner.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSAppRunnerServicePolicyForECRAccess")
            ]
        )
        
        # App Runner service
        self.app_runner_service = apprunner.CfnService(
            self,
            "MSAUIAppRunnerService",
            service_name="msa-invoice-audit-ui",
            source_configuration=apprunner.CfnService.SourceConfigurationProperty(
                auto_deployments_enabled=True,
                image_repository=apprunner.CfnService.ImageRepositoryProperty(
                    image_identifier=f"{self.ecr_repository.repository_uri}:latest",
                    image_configuration=apprunner.CfnService.ImageConfigurationProperty(
                        port="8501",
                        runtime_environment_variables=[
                            apprunner.CfnService.KeyValuePairProperty(
                                name="INGESTION_BUCKET",
                                value=self.ingestion_bucket.bucket_name
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name="REPORTS_BUCKET",
                                value=self.reports_bucket.bucket_name
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name="STEP_FUNCTION_ARN",
                                value=self.step_function.state_machine_arn
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name="BEDROCK_AGENT_ID",
                                value=self.bedrock_agent.attr_agent_id
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name="BEDROCK_AGENT_ALIAS_ID",
                                value=self.bedrock_agent_alias.attr_agent_alias_id
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name="AWS_DEFAULT_REGION",
                                value=self.region
                            )
                        ]
                    ),
                    image_repository_type="ECR"
                ),
                access_role_arn=access_role.role_arn
            ),
            instance_configuration=apprunner.CfnService.InstanceConfigurationProperty(
                cpu="1 vCPU",
                memory="2 GB",
                instance_role_arn=app_runner_role.role_arn
            ),
            health_check_configuration=apprunner.CfnService.HealthCheckConfigurationProperty(
                protocol="HTTP",
                path="/",
                interval=30,
                timeout=10,
                healthy_threshold=2,
                unhealthy_threshold=3
            )
        )
        
        # CloudWatch log group
        self.log_group = logs.LogGroup(
            self,
            "MSAUILogGroup",
            log_group_name="/aws/apprunner/msa-invoice-audit-ui",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY
        )
    
    def _create_outputs(self) -> None:
        """Create CloudFormation outputs."""
        CfnOutput(
            self,
            "IngestionBucketName",
            value=self.ingestion_bucket.bucket_name,
            description="S3 bucket for invoice ingestion"
        )
        
        CfnOutput(
            self,
            "ReportsBucketName",
            value=self.reports_bucket.bucket_name,
            description="S3 bucket for generated reports"
        )
        
        CfnOutput(
            self,
            "TemplatesBucketName",
            value=self.templates_bucket.bucket_name,
            description="S3 bucket for Excel templates"
        )
        
        CfnOutput(
            self,
            "KnowledgeBaseBucketName",
            value=self.knowledge_base_bucket.bucket_name,
            description="S3 bucket for knowledge base documents"
        )
        
        CfnOutput(
            self,
            "MSARatesTableName",
            value=self.msa_rates_table.table_name,
            description="DynamoDB table for MSA rates"
        )
        
        CfnOutput(
            self,
            "StepFunctionArn",
            value=self.step_function.state_machine_arn,
            description="Step Functions state machine ARN"
        )
        
        CfnOutput(
            self,
            "BedrockAgentId",
            value=self.bedrock_agent.attr_agent_id,
            description="Bedrock Agent ID"
        )
        
        CfnOutput(
            self,
            "BedrockAgentAliasId",
            value=self.bedrock_agent_alias.attr_agent_alias_id,
            description="Bedrock Agent Alias ID"
        )
        
        CfnOutput(
            self,
            "SageMakerEndpointName",
            value=self.sagemaker_endpoint.endpoint_name,
            description="SageMaker endpoint for anomaly detection"
        )
        
        CfnOutput(
            self,
            "ECRRepositoryURI",
            value=self.ecr_repository.repository_uri,
            description="ECR Repository URI for Streamlit application"
        )
        
        CfnOutput(
            self,
            "AppRunnerServiceURL",
            value=f"https://{self.app_runner_service.attr_service_url}",
            description="URL of the deployed Streamlit application"
        )
        
        CfnOutput(
            self,
            "OpenSearchDomainEndpoint",
            value=self.opensearch_domain.domain_endpoint,
            description="OpenSearch domain endpoint for Knowledge Base"
        )
