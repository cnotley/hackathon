"""
MSA Invoice Auditing System - Full Integrated Stack

This module defines a comprehensive CDK stack that deploys the complete
MSA Invoice Auditing System with all components integrated.
"""

from aws_cdk import (
    BundlingOptions,
    CfnOutput,
    CustomResource,
    Duration,
    RemovalPolicy,
    Stack,
    aws_apprunner as apprunner,
    aws_dynamodb as dynamodb,
    aws_ecr as ecr,
    aws_ecr_assets as ecr_assets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
from constructs import Construct
from aws_cdk.custom_resources import Provider


class MSAInvoiceAuditFullStack(Stack):
    """Minimal infrastructure for the invoice auditing prototype."""
    
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.ingestion_bucket = self._create_bucket("IngestionBucket", "ingestion")
        self.reports_bucket = self._create_bucket("ReportsBucket", "reports")

        self.msa_rates_table = self._create_msa_rates_table()

        self.ingestion_lambda = self._create_ingestion_lambda()
        self.extraction_lambda = self._create_extraction_lambda()
        self.reconciliation_lambda = self._create_reconciliation_lambda()
        self.report_lambda = self._create_report_lambda()

        self.state_machine = self._create_state_machine()
        self.ingestion_lambda.add_environment("STATE_MACHINE_ARN", self.state_machine.state_machine_arn)
        self.ingestion_lambda.role.add_to_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[self.state_machine.state_machine_arn],
            )
        )

        self.ui_repository = self._create_ui_repository()

        self.ui_service = self._create_app_runner_service()

        self._configure_ingestion_notifications()
        self._create_outputs()
    
    def _create_bucket(self, logical_id: str, bucket_type: str) -> s3.Bucket:
        return s3.Bucket(
            self,
            logical_id,
            bucket_name=self._bucket_name(bucket_type),
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(90),
                    abort_incomplete_multipart_upload_after=Duration.days(7),
                )
            ],
        )

    def _bucket_name(self, bucket_type: str) -> str:
        return f"invoice-{bucket_type}-{self.account}-{self.region}"

    def _create_lambda_function(
        self,
        logical_id: str,
        *,
        handler: str,
        code_path: str,
        environment: dict | None = None,
        timeout: Duration | None = None,
        additional_policy_statements: list[iam.PolicyStatement] | None = None,
    ) -> _lambda.Function:
        bundling = BundlingOptions(
            image=_lambda.Runtime.PYTHON_3_11.bundling_image,
            command=[
                "bash",
                "-c",
                "set -euo pipefail; if [ -f requirements.txt ]; then pip install -r requirements.txt -t /asset-output; elif [ -f ../requirements.txt ]; then pip install -r ../requirements.txt -t /asset-output; else echo 'requirements.txt not found, skipping install'; fi; cp -au . /asset-output",
            ],
        )

        role = iam.Role(
            self,
            f"{logical_id}ServiceRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
        )

        function = _lambda.Function(
            self,
            logical_id,
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler=handler,
            code=_lambda.Code.from_asset(code_path, bundling=bundling),
            role=role,
            memory_size=512,
            timeout=timeout or Duration.minutes(5),
            environment=environment or {},
        )

        for statement in additional_policy_statements or []:
            role.add_to_policy(statement)

        return function

    def _create_ingestion_lambda(self) -> _lambda.Function:
        bucket_access = iam.PolicyStatement(
            actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
            resources=[
                self.ingestion_bucket.bucket_arn,
                f"{self.ingestion_bucket.bucket_arn}/*",
            ],
        )

        ingestion_lambda = self._create_lambda_function(
            "IngestionLambda",
            handler="ingestion_lambda.lambda_handler",
            code_path="lambda",
            environment={"BUCKET_NAME": self.ingestion_bucket.bucket_name},
            additional_policy_statements=[bucket_access],
        )

        self.ingestion_bucket.grant_read_write(ingestion_lambda)
        return ingestion_lambda

    def _create_extraction_lambda(self) -> _lambda.Function:
        s3_read_access = iam.PolicyStatement(
            actions=["s3:GetObject", "s3:ListBucket"],
            resources=[
                self.ingestion_bucket.bucket_arn,
                f"{self.ingestion_bucket.bucket_arn}/*",
            ],
        )

        textract_access = iam.PolicyStatement(
            actions=[
                "textract:StartDocumentTextDetection",
                "textract:GetDocumentTextDetection",
            ],
            resources=["*"],
        )

        extraction_lambda = self._create_lambda_function(
            "ExtractionLambda",
            handler="extraction_lambda.lambda_handler",
            code_path="lambda",
            environment={"BUCKET_NAME": self.ingestion_bucket.bucket_name},
            additional_policy_statements=[s3_read_access, textract_access],
            timeout=Duration.minutes(10),
        )

        self.ingestion_bucket.grant_read(extraction_lambda)
        return extraction_lambda

    def _create_reconciliation_lambda(self) -> _lambda.Function:
        dynamo_access = iam.PolicyStatement(
            actions=["dynamodb:GetItem", "dynamodb:Query"],
            resources=[self.msa_rates_table.table_arn],
        )

        reconciliation_lambda = self._create_lambda_function(
            "ReconciliationLambda",
            handler="comparison_lambda.lambda_handler",
            code_path="lambda",
            environment={"MSA_RATES_TABLE_NAME": self.msa_rates_table.table_name},
            additional_policy_statements=[dynamo_access],
        )

        self.msa_rates_table.grant_read_data(reconciliation_lambda)
        return reconciliation_lambda

    def _create_report_lambda(self) -> _lambda.Function:
        reports_access = iam.PolicyStatement(
            actions=["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
            resources=[
                self.reports_bucket.bucket_arn,
                f"{self.reports_bucket.bucket_arn}/*",
            ],
        )

        report_lambda = self._create_lambda_function(
            "ReportLambda",
            handler="report_lambda.lambda_handler",
            code_path="lambda",
            environment={"REPORTS_BUCKET_NAME": self.reports_bucket.bucket_name},
            additional_policy_statements=[reports_access],
            timeout=Duration.minutes(10),
        )

        self.reports_bucket.grant_read_write(report_lambda)
        return report_lambda

    def _create_msa_rates_table(self) -> dynamodb.Table:
        table = dynamodb.Table(
            self,
            "MsaRatesTable",
            table_name="msa-rates",
            partition_key=dynamodb.Attribute(name="rate_id", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="effective_date", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        seed_function = _lambda.Function(
            self,
            "SeedMsaRatesFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="seed_msa_rates.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(5),
        )

        table.grant_write_data(seed_function)
        seed_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:BatchWriteItem"],
                resources=[table.table_arn],
            )
        )

        provider = Provider(self, "SeedMsaRatesProvider", on_event_handler=seed_function)
        CustomResource(
            self,
            "SeedMsaRates",
            service_token=provider.service_token,
            properties={"TableName": table.table_name},
        )

        return table

    def _create_state_machine(self) -> sfn.StateMachine:
        state_machine_role = iam.Role(
            self,
            "InvoiceWorkflowRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )

        lambda_functions = [
            self.ingestion_lambda,
            self.extraction_lambda,
            self.reconciliation_lambda,
            self.report_lambda,
        ]

        for function in lambda_functions:
            state_machine_role.add_to_policy(
                iam.PolicyStatement(
                    actions=["lambda:InvokeFunction"],
                    resources=[
                        function.function_arn,
                        f"{function.function_arn}:*",
                    ],
                )
            )

        ingestion_task = sfn_tasks.LambdaInvoke(
            self,
            "IngestInvoice",
            lambda_function=self.ingestion_lambda,
            result_path="$.ingestion",
        )

        extraction_task = sfn_tasks.LambdaInvoke(
            self,
            "ExtractInvoiceData",
            lambda_function=self.extraction_lambda,
            payload=sfn.TaskInput.from_object({"ingestion.$": "$.ingestion"}),
            result_path="$.extraction",
        )

        reconciliation_task = sfn_tasks.LambdaInvoke(
            self,
            "ReconcileAgainstMsaRates",
            lambda_function=self.reconciliation_lambda,
            payload=sfn.TaskInput.from_object({
                "ingestion.$": "$.ingestion",
                "extraction.$": "$.extraction",
            }),
            result_path="$.reconciliation",
        )

        report_task = sfn_tasks.LambdaInvoke(
            self,
            "GenerateAuditReport",
            lambda_function=self.report_lambda,
            payload=sfn.TaskInput.from_object({
                "ingestion.$": "$.ingestion",
                "extraction.$": "$.extraction",
                "reconciliation.$": "$.reconciliation",
            }),
            result_path="$.report",
        )

        human_review_task = sfn_tasks.LambdaInvoke(
            self,
            "AwaitHumanReview",
            lambda_function=self.reconciliation_lambda,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            payload=sfn.TaskInput.from_object({
                "taskToken": sfn.JsonPath.task_token,
                "discrepancies.$": "$.reconciliation.Payload.discrepancies",
            }),
            result_path="$.humanReview",
        )

        discrepancy_check = sfn.Choice(self, "DiscrepanciesPresent")
        discrepancy_check.when(
            sfn.Condition.number_greater_than("$.reconciliation.Payload.discrepancies.total", 0),
            human_review_task.next(report_task),
        )
        discrepancy_check.otherwise(report_task)

        workflow_definition = ingestion_task.next(extraction_task).next(reconciliation_task).next(discrepancy_check)

        return sfn.StateMachine(
            self,
            "InvoiceAuditStateMachine",
            state_machine_name=f"InvoiceAuditWorkflow-{self.region}",
            definition=workflow_definition,
            role=state_machine_role,
            timeout=Duration.hours(1),
        )

    def _configure_ingestion_notifications(self) -> None:
        self.ingestion_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.ingestion_lambda),
        )
    
    def _create_outputs(self) -> None:
        CfnOutput(
            self,
            "IngestionBucketName",
            value=self.ingestion_bucket.bucket_name,
            description="S3 bucket for invoice ingestion",
        )
        
        CfnOutput(
            self,
            "ReportsBucketName",
            value=self.reports_bucket.bucket_name,
            description="S3 bucket for generated reports",
        )
        
        CfnOutput(
            self,
            "MsaRatesTableName",
            value=self.msa_rates_table.table_name,
            description="DynamoDB table for MSA rates",
        )
        
        CfnOutput(
            self,
            "StateMachineArn",
            value=self.state_machine.state_machine_arn,
            description="Invoice audit workflow state machine ARN",
        )

        CfnOutput(
            self,
            "AppRunnerUrl",
            value=f"https://{self.ui_service.attr_service_url}",
            description="URL of the Streamlit UI hosted on App Runner",
        )

    def _create_ui_repository(self) -> ecr.Repository:
        return ecr.Repository(
            self,
            "UiRepository",
            repository_name=f"invoice-ui-{self.account}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_images=True,
        )

    def _create_app_runner_service(self) -> apprunner.CfnService:
        image_asset = ecr_assets.DockerImageAsset(
            self,
            "UiImage",
            directory="ui",
            file="Dockerfile",
        )

        app_runner_role = iam.Role(
            self,
            "UiAppRunnerServiceRole",
            assumed_by=iam.ServicePrincipal("tasks.apprunner.amazonaws.com"),
        )
        app_runner_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject"],
                resources=[
                    self.reports_bucket.bucket_arn,
                    f"{self.reports_bucket.bucket_arn}/*",
                ],
            )
        )
        app_runner_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[
                    self.ingestion_bucket.bucket_arn,
                    f"{self.ingestion_bucket.bucket_arn}/*",
                ],
            )
        )
        app_runner_role.add_to_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution", "states:DescribeExecution"],
                resources=[self.state_machine.state_machine_arn],
            )
        )
        app_runner_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ecr:GetAuthorizationToken"],
                resources=["*"],
            )
        )
        app_runner_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer",
                ],
                resources=[self.ui_repository.repository_arn],
            )
        )
        image_asset.repository.grant_pull(app_runner_role)

        image_repository = apprunner.CfnService.ImageRepositoryProperty(
            image_identifier=image_asset.image_uri,
            image_repository_type="ECR",
            image_configuration=apprunner.CfnService.ImageConfigurationProperty(
                port="8501",
                runtime_environment_variables=[
                    apprunner.CfnService.KeyValuePairProperty(name="INGESTION_BUCKET", value=self.ingestion_bucket.bucket_name),
                    apprunner.CfnService.KeyValuePairProperty(name="REPORTS_BUCKET", value=self.reports_bucket.bucket_name),
                    apprunner.CfnService.KeyValuePairProperty(name="STATE_MACHINE_ARN", value=self.state_machine.state_machine_arn),
                    apprunner.CfnService.KeyValuePairProperty(name="AWS_DEFAULT_REGION", value=self.region),
                ],
            ),
        )

        source_configuration = apprunner.CfnService.SourceConfigurationProperty(
            auto_deployments_enabled=True,
            authentication_configuration=apprunner.CfnService.AuthenticationConfigurationProperty(
                access_role_arn=app_runner_role.role_arn,
            ),
            image_repository=image_repository,
        )

        service = apprunner.CfnService(
            self,
            "UiAppRunnerService",
            service_name=f"invoice-ui-{self.region}",
            source_configuration=source_configuration,
            instance_configuration=apprunner.CfnService.InstanceConfigurationProperty(
                cpu="2 vCPU",
                memory="4 GB",
            ),
            health_check_configuration=apprunner.CfnService.HealthCheckConfigurationProperty(
                protocol="HTTP",
                path="/",
                interval=30,
                timeout=10,
                healthy_threshold=1,
                unhealthy_threshold=3,
            ),
        )

        return service
