import aws_cdk as cdk
from aws_cdk import (
    Stack, Duration,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_dynamodb as dynamodb,
    aws_s3_notifications as s3n,
    aws_logs as logs,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_cloudwatch as cloudwatch,
)
from constructs import Construct

class AuditFullStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = ec2.Vpc(self, "AuditVPC", max_azs=2)

        invoices_bucket = s3.Bucket(
            self,
            "AuditFilesBucket",
            bucket_name="audit-files-bucket",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )
        reports_bucket = s3.Bucket(
            self,
            "ReportsBucket",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        table = dynamodb.Table(self, "MwoRates",
                               partition_key=dynamodb.Attribute(name="code", type=dynamodb.AttributeType.STRING),
                               billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
                               point_in_time_recovery=True)

        common_layer = _lambda.LayerVersion(self, "CommonLayer",
                                            code=_lambda.Code.from_asset("layers/common"),
                                            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12])

        env = {
            "INVOICES_BUCKET": invoices_bucket.bucket_name,
            "REPORTS_BUCKET": reports_bucket.bucket_name,
            "MWO_TABLE_NAME": table.table_name,
            "OCR_MIN_CONF": "0.8",
            "MAX_UPLOAD_MB": "5",
        }

        ingestion_fn = _lambda.Function(
            self, "IngestionLambda",
            code=_lambda.Code.from_asset("lambda"),
            handler="ingestion_lambda.handle_event",
            runtime=_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(300),
            memory_size=512,
            environment=env,
            layers=[common_layer],
            log_retention=logs.RetentionDays.ONE_WEEK,
            vpc=vpc,
        )

        extraction_fn = _lambda.Function(
            self, "ExtractionLambda",
            code=_lambda.Code.from_asset("lambda"),
            handler="extraction_lambda.extract_handler",
            runtime=_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(300),
            memory_size=512,
            environment=env,
            layers=[common_layer],
            log_retention=logs.RetentionDays.ONE_WEEK,
            vpc=vpc,
        )

        agent_fn = _lambda.Function(
            self, "AgentLambda",
            code=_lambda.Code.from_asset("lambda"),
            handler="agent_lambda.invoke_agent",
            runtime=_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(300),
            memory_size=512,
            environment=env,
            layers=[common_layer],
            log_retention=logs.RetentionDays.ONE_WEEK,
            vpc=vpc,
        )

        comparison_fn = _lambda.Function(
            self, "ComparisonLambda",
            code=_lambda.Code.from_asset("lambda"),
            handler="comparison_lambda.compare_handler",
            runtime=_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(300),
            memory_size=512,
            environment=env,
            layers=[common_layer],
            log_retention=logs.RetentionDays.ONE_WEEK,
            vpc=vpc,
        )

        report_fn = _lambda.Function(
            self, "ReportLambda",
            code=_lambda.Code.from_asset("lambda"),
            handler="report_lambda.generate_handler",
            runtime=_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(300),
            memory_size=512,
            environment=env,
            layers=[common_layer],
            log_retention=logs.RetentionDays.ONE_WEEK,
            vpc=vpc,
        )

        # Seed DynamoDB rates on deploy
        seed_fn = _lambda.Function(
            self,
            "SeedRatesFn",
            code=_lambda.Code.from_asset("lambda"),
            handler="seeding.seed_rates",
            runtime=_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(60),
            environment={"MWO_TABLE_NAME": table.table_name},
            layers=[common_layer],
            vpc=vpc,
        )
        table.grant_read_write_data(seed_fn)
        cdk.CustomResource(self, "SeedRates", service_token=seed_fn.function_arn)

        # Provisioned concurrency for agent
        agent_fn.current_version.add_alias(
            "live", provisioned_concurrent_executions=1
        )

        # Step Functions: Extract -> Compare -> Report
        extract_task = tasks.LambdaInvoke(self, "Extract",
                                          lambda_function=extraction_fn,
                                          output_path="$.Payload",
                                          retry_on_service_exceptions=True)
        agent_task = tasks.LambdaInvoke(self, "Agent",
                                        lambda_function=agent_fn,
                                        output_path="$.Payload",
                                        retry_on_service_exceptions=True)
        compare_task = tasks.LambdaInvoke(self, "Compare",
                                          lambda_function=comparison_fn,
                                          output_path="$.Payload",
                                          retry_on_service_exceptions=True)
        report_task = tasks.LambdaInvoke(self, "Report",
                                         lambda_function=report_fn,
                                         output_path="$.Payload",
                                         retry_on_service_exceptions=True)
        for t in [extract_task, agent_task, compare_task, report_task]:
            t.add_retry(max_attempts=3, backoff_rate=1.5)

        definition = extract_task.next(agent_task).next(compare_task).next(report_task)
        sm = sfn.StateMachine(self, "AuditStateMachine",
                              definition_body=sfn.DefinitionBody.from_chainable(definition),
                              timeout=Duration.minutes(30))

        # permissions
        for fn in [ingestion_fn, extraction_fn, agent_fn, comparison_fn, report_fn]:
            invoices_bucket.grant_read(fn)
        reports_bucket.grant_read_write(report_fn)
        table.grant_read_data(comparison_fn)

        sm.grant_start_execution(ingestion_fn)
        invoices_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.LambdaDestination(ingestion_fn))

        # Bedrock, Comprehend, Textract, StepFunctions wide-open for prototype
        for fn in [extraction_fn, agent_fn, comparison_fn, report_fn, ingestion_fn]:
            fn.add_to_role_policy(iam.PolicyStatement(actions=["textract:*","bedrock:*","comprehend:*","states:*","sagemaker:InvokeEndpoint"], resources=["*"]))

        # CloudWatch alarm and dashboard
        alarm = cloudwatch.Alarm(
            self,
            "IngestionErrorsAlarm",
            metric=ingestion_fn.metric_errors(period=Duration.minutes(1)),
            threshold=1,
            evaluation_periods=1,
        )
        dashboard = cloudwatch.Dashboard(self, "AuditDashboard")
        dashboard.add_widgets(
            cloudwatch.GraphWidget(title="Ingestion Errors", left=[ingestion_fn.metric_errors()])
        )
