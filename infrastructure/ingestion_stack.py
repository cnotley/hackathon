from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
)
from constructs import Construct


class IngestionStack(Stack):
    """Stack creating S3 bucket and ingestion Lambda."""

    def __init__(self, scope: Construct, construct_id: str, *, state_machine_arn: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(
            self,
            'InvoiceBucket',
            versioned=True,
            cors=[s3.CorsRule(allowed_methods=[s3.HttpMethods.GET, s3.HttpMethods.PUT], allowed_origins=['*'], allowed_headers=['*'])],
        )

        layer = _lambda.LayerVersion(
            self,
            'CommonLayer',
            code=_lambda.Code.from_asset('layers/common'),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
        )

        fn = _lambda.Function(
            self,
            'IngestionLambda',
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler='ingestion_lambda.handle_event',
            code=_lambda.Code.from_asset('lambda'),
            memory_size=256,
            timeout=Duration.minutes(15),
            layers=[layer],
            environment={'STATE_MACHINE_ARN': state_machine_arn},
        )

        bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.LambdaDestination(fn))
        self.bucket = bucket
        self.ingestion_lambda = fn
