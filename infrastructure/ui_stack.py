"""
MSA Invoice Auditing System - UI Stack

This module defines the CDK stack for deploying the Streamlit UI using AWS App Runner.
"""

from aws_cdk import (
    Stack,
    aws_apprunner as apprunner,
    aws_iam as iam,
    aws_ecr as ecr,
    aws_logs as logs,
    CfnOutput,
    Duration,
    RemovalPolicy
)
from constructs import Construct
from typing import Optional


class MSAInvoiceAuditUIStack(Stack):
    """CDK Stack for MSA Invoice Auditing UI using AWS App Runner."""
    
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        ingestion_bucket_name: str,
        reports_bucket_name: str,
        step_function_arn: str,
        bedrock_agent_id: str,
        bedrock_agent_alias_id: str = "TSTALIASID",
        **kwargs
    ) -> None:
        """
        Initialize the UI stack.
        
        Args:
            scope: The scope in which to define this construct
            construct_id: The scoped construct ID
            ingestion_bucket_name: Name of the S3 ingestion bucket
            reports_bucket_name: Name of the S3 reports bucket
            step_function_arn: ARN of the Step Functions state machine
            bedrock_agent_id: ID of the Bedrock Agent
            bedrock_agent_alias_id: Alias ID of the Bedrock Agent
            **kwargs: Additional keyword arguments
        """
        super().__init__(scope, construct_id, **kwargs)
        
        # Store configuration for outputs
        self.ingestion_bucket_name = ingestion_bucket_name
        self.reports_bucket_name = reports_bucket_name
        self.step_function_arn = step_function_arn
        self.bedrock_agent_id = bedrock_agent_id
        self.bedrock_agent_alias_id = bedrock_agent_alias_id
        
        # Create ECR repository for the Streamlit app
        self.ecr_repository = self._create_ecr_repository()
        
        # Create IAM role for App Runner
        self.app_runner_role = self._create_app_runner_role()
        
        # Create App Runner service
        self.app_runner_service = self._create_app_runner_service()
        
        # Create CloudWatch log group
        self.log_group = self._create_log_group()
        
        # Create outputs
        self._create_outputs()
    
    def _create_ecr_repository(self) -> ecr.Repository:
        """Create ECR repository for the Streamlit application."""
        repository = ecr.Repository(
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
        
        return repository
    
    def _create_app_runner_role(self) -> iam.Role:
        """Create IAM role for App Runner with necessary permissions."""
        role = iam.Role(
            self,
            "MSAUIAppRunnerRole",
            assumed_by=iam.ServicePrincipal("tasks.apprunner.amazonaws.com"),
            description="IAM role for MSA Invoice Audit UI App Runner service"
        )
        
        # Add permissions for S3 access
        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                resources=[
                    f"arn:aws:s3:::{self.ingestion_bucket_name}",
                    f"arn:aws:s3:::{self.ingestion_bucket_name}/*",
                    f"arn:aws:s3:::{self.reports_bucket_name}",
                    f"arn:aws:s3:::{self.reports_bucket_name}/*"
                ]
            )
        )
        
        # Add permissions for Step Functions
        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "states:StartExecution",
                    "states:DescribeExecution",
                    "states:ListExecutions"
                ],
                resources=[
                    self.step_function_arn,
                    f"{self.step_function_arn}:*"
                ]
            )
        )
        
        # Add permissions for Bedrock Agent
        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "bedrock:InvokeAgent",
                    "bedrock:GetAgent",
                    "bedrock:ListAgents"
                ],
                resources=[
                    f"arn:aws:bedrock:{self.region}:{self.account}:agent/{self.bedrock_agent_id}",
                    f"arn:aws:bedrock:{self.region}:{self.account}:agent-alias/{self.bedrock_agent_id}/{self.bedrock_agent_alias_id}"
                ]
            )
        )
        
        # Add CloudWatch Logs permissions
        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:DescribeLogGroups",
                    "logs:DescribeLogStreams"
                ],
                resources=["*"]
            )
        )
        
        return role
    
    def _create_app_runner_service(self) -> apprunner.CfnService:
        """Create App Runner service for the Streamlit application."""
        
        # Create App Runner access role for ECR
        access_role = iam.Role(
            self,
            "MSAUIAppRunnerAccessRole",
            assumed_by=iam.ServicePrincipal("build.apprunner.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSAppRunnerServicePolicyForECRAccess")
            ]
        )
        
        # App Runner service configuration
        service = apprunner.CfnService(
            self,
            "MSAUIAppRunnerService",
            service_name="msa-invoice-audit-ui",
            source_configuration=apprunner.CfnService.SourceConfigurationProperty(
                auto_deployments_enabled=True,
                image_repository=apprunner.CfnService.ImageRepositoryProperty(
                    image_identifier=f"{self.ecr_repository.repository_uri}:latest",
                    image_configuration=apprunner.CfnService.ImageConfigurationProperty(
                        port="8501",  # Streamlit default port
                        runtime_environment_variables=[
                            apprunner.CfnService.KeyValuePairProperty(
                                name="INGESTION_BUCKET",
                                value=self.ingestion_bucket_name
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name="REPORTS_BUCKET",
                                value=self.reports_bucket_name
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name="STEP_FUNCTION_ARN",
                                value=self.step_function_arn
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name="BEDROCK_AGENT_ID",
                                value=self.bedrock_agent_id
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name="BEDROCK_AGENT_ALIAS_ID",
                                value=self.bedrock_agent_alias_id
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
                instance_role_arn=self.app_runner_role.role_arn
            ),
            health_check_configuration=apprunner.CfnService.HealthCheckConfigurationProperty(
                protocol="HTTP",
                path="/",
                interval=30,
                timeout=10,
                healthy_threshold=2,
                unhealthy_threshold=3
            ),
            auto_scaling_configuration_arn=None  # Use default auto-scaling
        )
        
        return service
    
    def _create_log_group(self) -> logs.LogGroup:
        """Create CloudWatch log group for the application."""
        log_group = logs.LogGroup(
            self,
            "MSAUILogGroup",
            log_group_name="/aws/apprunner/msa-invoice-audit-ui",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY
        )
        
        return log_group
    
    def _create_outputs(self) -> None:
        """Create CloudFormation outputs."""
        CfnOutput(
            self,
            "ECRRepositoryURI",
            value=self.ecr_repository.repository_uri,
            description="ECR Repository URI for the Streamlit application"
        )
        
        CfnOutput(
            self,
            "AppRunnerServiceURL",
            value=f"https://{self.app_runner_service.attr_service_url}",
            description="URL of the deployed Streamlit application"
        )
        
        CfnOutput(
            self,
            "AppRunnerServiceArn",
            value=self.app_runner_service.attr_service_arn,
            description="ARN of the App Runner service"
        )
        
        CfnOutput(
            self,
            "LogGroupName",
            value=self.log_group.log_group_name,
            description="CloudWatch Log Group for the application"
        )
