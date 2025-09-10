# Deployment Guide

1. Configure AWS CLI credentials with permissions for S3, Lambda, Step Functions, DynamoDB, SageMaker, and Bedrock.
2. Bootstrap CDK environment:
```bash
cd infrastructure
cdk bootstrap
```
3. Deploy the full stack:
```bash
cdk deploy
```
4. To seed DynamoDB rates, a custom resource triggers `seeding_lambda` automatically during deploy.
5. For local testing use LocalStack and run `pytest`.
