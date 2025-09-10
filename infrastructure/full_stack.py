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
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
)
from constructs import Construct
from aws_cdk.custom_resources import Provider


class MSAInvoiceAuditFullStack(Stack):
    """Minimal infrastructure for the invoice auditing prototype."""
    
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.ingestion_bucket = self._create_bucket("IngestionBucket", "ingestion")
        self.reports_bucket = self._create_bucket("ReportsBucket", "reports")

        self.ingestion_lambda = self._create_ingestion_lambda()
        self.msa_rates_table = self._create_msa_rates_table()

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

    def _create_ingestion_lambda(self) -> _lambda.Function:
        ingestion_lambda = self._create_lambda_function(
            "IngestionLambda",
            handler="ingestion_lambda.lambda_handler",
            code_path="lambda",
            environment={"INGESTION_BUCKET_NAME": self.ingestion_bucket.bucket_name},
        )

        self.ingestion_bucket.grant_read_write(ingestion_lambda)
        return ingestion_lambda

    def _create_lambda_function(
        self,
        logical_id: str,
        *,
        handler: str,
        code_path: str,
        environment: dict | None = None,
        timeout: Duration | None = None,
    ) -> _lambda.Function:
        bundling = BundlingOptions(
            image=_lambda.Runtime.PYTHON_3_11.bundling_image,
            command=[
                "bash",
                "-c",
                "set -euo pipefail; cp -au . /asset-output",
            ],
        )

        return _lambda.Function(
            self,
            logical_id,
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler=handler,
            code=_lambda.Code.from_asset(code_path, bundling=bundling),
            memory_size=512,
            timeout=timeout or Duration.minutes(5),
            environment=environment or {},
        )

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
