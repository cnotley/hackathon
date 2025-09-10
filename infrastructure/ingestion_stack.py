from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_iam as iam,
    aws_s3_notifications as s3n,
    aws_logs as logs,
)
from constructs import Construct
import json


class InvoiceIngestionStack(Stack):
    """CDK Stack for Invoice Auditing File Ingestion Module."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 bucket for file storage
        self.bucket = self._create_s3_bucket()
        
        # Create Lambda layer for common utilities
        self.common_layer = self._create_lambda_layer()
        
        # Create Lambda function for file processing
        self.lambda_function = self._create_lambda_function()
        
        # Create extraction Lambda function for data extraction
        self.extraction_lambda = self._create_extraction_lambda_function()
        
        # Create Step Functions state machine
        self.state_machine = self._create_step_functions_workflow()

        # Placeholder for wiring to full pipeline; can be set by parent stack via parameter/prop
        self.full_pipeline_state_machine_arn = self.node.try_get_context("full_pipeline_state_machine_arn")
        
        # Configure S3 event notifications
        self._configure_s3_notifications()
        
        # Create CloudFormation outputs
        self._create_outputs()

    def _create_s3_bucket(self) -> s3.Bucket:
        """Create S3 bucket with security and lifecycle configurations."""
        bucket = s3.Bucket(
            self,
            "AuditFilesBucket",
            bucket_name="audit-files-bucket",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,  # For demo purposes
            auto_delete_objects=True,  # For demo purposes
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="TransitionToIA",
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
        
        # Add bucket policy to deny insecure connections
        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="DenyInsecureConnections",
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                actions=["s3:*"],
                resources=[
                    bucket.bucket_arn,
                    f"{bucket.bucket_arn}/*"
                ],
                conditions={
                    "Bool": {
                        "aws:SecureTransport": "false"
                    }
                }
            )
        )
        
        return bucket

    def _create_lambda_layer(self) -> _lambda.LayerVersion:
        """Create Lambda layer with common utilities."""
        return _lambda.LayerVersion(
            self,
            "CommonUtilsLayer",
            code=_lambda.Code.from_asset("lambda/layers/common"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
            description="Common utilities for invoice ingestion processing",
            removal_policy=RemovalPolicy.DESTROY
        )

    def _create_lambda_function(self) -> _lambda.Function:
        """Create Lambda function for file processing."""
        # Create IAM role for Lambda
        lambda_role = iam.Role(
            self,
            "IngestionLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )
        
        # Add S3 permissions
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:PutObject",
                    "s3:PutObjectTagging",
                    "s3:GetObjectTagging"
                ],
                resources=[f"{self.bucket.bucket_arn}/*"]
            )
        )
        
        # Add Step Functions permissions
        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "states:StartExecution",
                    "states:SendTaskSuccess",
                    "states:SendTaskFailure",
                    "states:SendTaskHeartbeat"
                ],
                resources=["*"]  # Will be restricted after state machine creation
            )
        )
        
        # Create Lambda function
        self.ingestion_lambda = _lambda.Function(
            self,
            "MSAIngestionLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(
                "lambda/ingestion",
                bundling=_lambda.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "set -euo pipefail; cd /asset-input && pip install --platform manylinux2014_x86_64 --only-binary=:all: -r ../../requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            ),
            role=lambda_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "STEP_FUNCTION_ARN": self.state_machine.state_machine_arn,
                "BUCKET_NAME": self.bucket.bucket_name
            },
            events=[]
        )
        
        return self.ingestion_lambda

    def _create_extraction_lambda_function(self) -> _lambda.Function:
        """Create Lambda function for data extraction using Textract and pandas."""
        # Create IAM role for extraction Lambda
        extraction_lambda_role = iam.Role(
            self,
            "ExtractionLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )
        
        # Add S3 permissions
        extraction_lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:PutObject",
                    "s3:PutObjectTagging",
                    "s3:GetObjectTagging"
                ],
                resources=[f"{self.bucket.bucket_arn}/*"]
            )
        )
        
        # Add Textract permissions
        extraction_lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "textract:AnalyzeDocument",
                    "textract:StartDocumentAnalysis",
                    "textract:GetDocumentAnalysis"
                ],
                resources=["*"]
            )
        )
        
        # Add Bedrock permissions for semantic mapping
        extraction_lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream"
                ],
                resources=["*"]
            )
        )
        
        # Add Comprehend permissions for entity recognition
        extraction_lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "comprehend:DetectEntities",
                    "comprehend:DetectKeyPhrases",
                    "comprehend:DetectSentiment",
                    "comprehend:DetectSyntax"
                ],
                resources=["*"]
            )
        )
        
        # Add Step Functions permissions
        extraction_lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "states:SendTaskSuccess",
                    "states:SendTaskFailure",
                    "states:SendTaskHeartbeat"
                ],
                resources=["*"]
            )
        )
        
        # Create extraction Lambda function
        extraction_lambda = _lambda.Function(
            self,
            "ExtractionLambda",
            function_name="extraction-lambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="extraction_lambda.lambda_handler",
            code=_lambda.Code.from_asset(
                "lambda",
                bundling=_lambda.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r extraction_lambda_requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            ),
            layers=[self.common_layer],
            role=extraction_lambda_role,
            timeout=Duration.minutes(15),  # Longer timeout for Textract processing
            memory_size=1024,  # More memory for pandas processing
            environment={
                "BUCKET_NAME": self.bucket.bucket_name,
                "LOG_LEVEL": "INFO"
            },
            log_retention=logs.RetentionDays.ONE_WEEK
        )
        
        return extraction_lambda

    def _create_step_functions_workflow(self) -> sfn.StateMachine:
        """Create Step Functions state machine for workflow orchestration."""
        # Create IAM role for Step Functions
        sfn_role = iam.Role(
            self,
            "StepFunctionsRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            inline_policies={
                "LambdaInvokePolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["lambda:InvokeFunction"],
                            resources=[
                                self.lambda_function.function_arn,
                                self.extraction_lambda.function_arn
                            ]
                        )
                    ]
                )
            }
        )
        
        # Define workflow tasks
        validate_task = sfn_tasks.LambdaInvoke(
            self,
            "ValidateFile",
            lambda_function=self.lambda_function,
            payload=sfn.TaskInput.from_object({
                "task": "validate",
                "input.$": "$"
            }),
            result_path="$.validation_result",
            retry_on_service_exceptions=True
        )
        
        extract_metadata_task = sfn_tasks.LambdaInvoke(
            self,
            "ExtractMetadata",
            lambda_function=self.lambda_function,
            payload=sfn.TaskInput.from_object({
                "task": "extract",
                "input.$": "$"
            }),
            result_path="$.metadata_result",
            retry_on_service_exceptions=True
        )
        
        extract_data_task = sfn_tasks.LambdaInvoke(
            self,
            "ExtractData",
            lambda_function=self.extraction_lambda,
            payload=sfn.TaskInput.from_object({
                "task": "extract",
                "input.$": "$"
            }),
            result_path="$.extraction_result",
            retry_on_service_exceptions=True
        )
        
        process_task = sfn_tasks.LambdaInvoke(
            self,
            "ProcessFile",
            lambda_function=self.lambda_function,
            payload=sfn.TaskInput.from_object({
                "task": "process",
                "input.$": "$"
            }),
            result_path="$.processing_result",
            retry_on_service_exceptions=True
        )

        # Optionally kick off the full AI pipeline after processing
        start_full_pipeline = None
        if self.full_pipeline_state_machine_arn:
            start_full_pipeline = sfn_tasks.StepFunctionsStartExecution(
                self,
                "StartFullAIPipeline",
                state_machine=sfn.StateMachine.from_state_machine_arn(
                    self, "ImportedFullPipelineSM", self.full_pipeline_state_machine_arn
                ),
                input=sfn.TaskInput.from_object({
                    "bucket.$": "$.bucket",
                    "key.$": "$.key"
                })
            )
        
        # Define success and failure states
        success_state = sfn.Succeed(
            self,
            "ProcessingComplete",
            comment="File processing completed successfully"
        )
        
        failure_state = sfn.Fail(
            self,
            "ProcessingFailed",
            comment="File processing failed",
            cause="Validation, extraction, or processing error"
        )
        
        # Define workflow with error handling
        definition = validate_task.add_catch(
            failure_state,
            errors=["States.ALL"],
            result_path="$.error"
        ).next(
            extract_metadata_task.add_catch(
                failure_state,
                errors=["States.ALL"],
                result_path="$.error"
            ).next(
                extract_data_task.add_catch(
                    failure_state,
                    errors=["States.ALL"],
                    result_path="$.error"
                ).next(
                    process_task.add_catch(
                        failure_state,
                        errors=["States.ALL"],
                        result_path="$.error"
                    ).next(start_full_pipeline.next(success_state) if start_full_pipeline else success_state)
                )
            )
        )
        
        # Create state machine
        state_machine = sfn.StateMachine(
            self,
            "InvoiceAuditWorkflow",
            state_machine_name="invoice-audit-workflow",
            definition=definition,
            role=sfn_role,
            timeout=Duration.minutes(15),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self,
                    "StepFunctionsLogGroup",
                    log_group_name="/aws/stepfunctions/invoice-audit-workflow",
                    retention=logs.RetentionDays.ONE_WEEK,
                    removal_policy=RemovalPolicy.DESTROY
                ),
                level=sfn.LogLevel.ALL
            )
        )
        
        # Update Lambda environment with state machine ARN
        self.lambda_function.add_environment(
            "STATE_MACHINE_ARN", state_machine.state_machine_arn
        )
        
        # Update Lambda role to allow starting this specific state machine
        self.lambda_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["states:StartExecution"],
                resources=[state_machine.state_machine_arn]
            )
        )
        
        return state_machine

    def _configure_s3_notifications(self) -> None:
        """Configure S3 event notifications to trigger Lambda function."""
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.lambda_function),
            s3.NotificationKeyFilter(
                suffix=".pdf"
            )
        )
        
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.lambda_function),
            s3.NotificationKeyFilter(
                suffix=".xlsx"
            )
        )
        
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.lambda_function),
            s3.NotificationKeyFilter(
                suffix=".xls"
            )
        )
        
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.lambda_function),
            s3.NotificationKeyFilter(
                suffix=".png"
            )
        )
        
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.lambda_function),
            s3.NotificationKeyFilter(
                suffix=".jpg"
            )
        )
        
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.lambda_function),
            s3.NotificationKeyFilter(
                suffix=".jpeg"
            )
        )

    def _create_outputs(self) -> None:
        """Create CloudFormation outputs for important resources."""
        CfnOutput(
            self,
            "BucketName",
            value=self.bucket.bucket_name,
            description="S3 bucket name for file uploads"
        )
        
        CfnOutput(
            self,
            "BucketArn",
            value=self.bucket.bucket_arn,
            description="S3 bucket ARN"
        )
        
        CfnOutput(
            self,
            "LambdaFunctionName",
            value=self.lambda_function.function_name,
            description="Lambda function name"
        )
        
        CfnOutput(
            self,
            "LambdaFunctionArn",
            value=self.lambda_function.function_arn,
            description="Lambda function ARN"
        )
        
        CfnOutput(
            self,
            "StateMachineArn",
            value=self.state_machine.state_machine_arn,
            description="Step Functions state machine ARN"
        )
        
        CfnOutput(
            self,
            "StateMachineName",
            value=self.state_machine.state_machine_name,
            description="Step Functions state machine name"
        )
        
        CfnOutput(
            self,
            "ExtractionLambdaFunctionName",
            value=self.extraction_lambda.function_name,
            description="Extraction Lambda function name"
        )
        
        CfnOutput(
            self,
            "ExtractionLambdaFunctionArn",
            value=self.extraction_lambda.function_arn,
            description="Extraction Lambda function ARN"
        )
