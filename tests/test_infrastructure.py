"""
Unit tests for CDK infrastructure
"""

import pytest
import aws_cdk as cdk
from aws_cdk import assertions
import sys
import os

# Add the infrastructure to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from infrastructure.ingestion_stack import InvoiceIngestionStack


class TestInvoiceIngestionStack:
    """Test cases for the CDK infrastructure stack"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.app = cdk.App()
        self.stack = InvoiceIngestionStack(
            self.app, 
            "TestInvoiceIngestionStack"
        )
        self.template = assertions.Template.from_stack(self.stack)
    
    def test_s3_bucket_created(self):
        """Test that S3 bucket is created with correct properties"""
        self.template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": "audit-files-bucket",
            "VersioningConfiguration": {
                "Status": "Enabled"
            },
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [
                    {
                        "ServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256"
                        }
                    }
                ]
            },
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True
            }
        })
    
    def test_s3_bucket_policy_created(self):
        """Test that S3 bucket policy denies insecure connections"""
        self.template.has_resource_properties("AWS::S3::BucketPolicy", {
            "PolicyDocument": {
                "Statement": assertions.Match.array_with([
                    {
                        "Sid": "DenyInsecureConnections",
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:*",
                        "Condition": {
                            "Bool": {
                                "aws:SecureTransport": "false"
                            }
                        }
                    }
                ])
            }
        })
    
    def test_lambda_function_created(self):
        """Test that Lambda function is created with correct properties"""
        self.template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": "ingestion-lambda",
            "Runtime": "python3.11",
            "Handler": "handler.lambda_handler",
            "Timeout": 300,
            "MemorySize": 512,
            "Environment": {
                "Variables": {
                    "LOG_LEVEL": "INFO",
                    "BUCKET_NAME": "audit-files-bucket"
                }
            }
        })
    
    def test_lambda_layer_created(self):
        """Test that Lambda layer is created"""
        self.template.has_resource("AWS::Lambda::LayerVersion", {
            "Properties": {
                "CompatibleRuntimes": ["python3.11"],
                "Description": "Common utilities for invoice processing"
            }
        })
    
    def test_lambda_iam_role_created(self):
        """Test that Lambda IAM role is created with correct permissions"""
        self.template.has_resource_properties("AWS::IAM::Role", {
            "AssumeRolePolicyDocument": {
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            },
            "ManagedPolicyArns": [
                {
                    "Fn::Join": [
                        "",
                        [
                            "arn:",
                            {"Ref": "AWS::Partition"},
                            ":iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                        ]
                    ]
                }
            ]
        })
    
    def test_lambda_s3_permissions(self):
        """Test that Lambda has correct S3 permissions"""
        self.template.has_resource_properties("AWS::IAM::Policy", {
            "PolicyDocument": {
                "Statement": assertions.Match.array_with([
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:GetObjectVersion",
                            "s3:PutObject",
                            "s3:PutObjectTagging"
                        ]
                    }
                ])
            }
        })
    
    def test_step_functions_state_machine_created(self):
        """Test that Step Functions state machine is created"""
        self.template.has_resource_properties("AWS::StepFunctions::StateMachine", {
            "StateMachineName": "invoice-audit-workflow",
            "StateMachineType": "STANDARD"
        })
    
    def test_step_functions_iam_role_created(self):
        """Test that Step Functions IAM role is created"""
        self.template.has_resource_properties("AWS::IAM::Role", {
            "AssumeRolePolicyDocument": {
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "states.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
        })
    
    def test_s3_event_notification_created(self):
        """Test that S3 event notification is configured"""
        # Check for Lambda permission to be invoked by S3
        self.template.has_resource_properties("AWS::Lambda::Permission", {
            "Action": "lambda:InvokeFunction",
            "Principal": "s3.amazonaws.com"
        })
    
    def test_cloudformation_outputs_created(self):
        """Test that CloudFormation outputs are created"""
        self.template.has_output("BucketName", {
            "Description": "S3 bucket name for audit files"
        })
        
        self.template.has_output("LambdaFunctionName", {
            "Description": "Lambda function name for file ingestion"
        })
        
        self.template.has_output("StateMachineArn", {
            "Description": "Step Functions state machine ARN"
        })
        
        self.template.has_output("BucketArn", {
            "Description": "S3 bucket ARN for audit files"
        })
    
    def test_log_groups_created(self):
        """Test that CloudWatch log groups are created"""
        # Lambda log group
        self.template.has_resource_properties("AWS::Logs::LogGroup", {
            "RetentionInDays": 7
        })
        
        # Step Functions log group
        self.template.has_resource_properties("AWS::Logs::LogGroup", {
            "LogGroupName": "/aws/stepfunctions/invoice-audit-workflow",
            "RetentionInDays": 7
        })
    
    def test_resource_count(self):
        """Test that expected number of resources are created"""
        # Count key resources
        self.template.resource_count_is("AWS::S3::Bucket", 1)
        self.template.resource_count_is("AWS::Lambda::Function", 1)
        self.template.resource_count_is("AWS::Lambda::LayerVersion", 1)
        self.template.resource_count_is("AWS::StepFunctions::StateMachine", 1)
        
        # Should have multiple IAM roles (Lambda, Step Functions)
        iam_roles = self.template.find_resources("AWS::IAM::Role")
        assert len(iam_roles) >= 2
    
    def test_s3_lifecycle_rules(self):
        """Test that S3 lifecycle rules are configured"""
        self.template.has_resource_properties("AWS::S3::Bucket", {
            "LifecycleConfiguration": {
                "Rules": assertions.Match.array_with([
                    {
                        "Id": "DeleteIncompleteMultipartUploads",
                        "Status": "Enabled",
                        "AbortIncompleteMultipartUpload": {
                            "DaysAfterInitiation": 1
                        }
                    },
                    {
                        "Id": "TransitionToIA",
                        "Status": "Enabled",
                        "Transitions": [
                            {
                                "StorageClass": "STANDARD_IA",
                                "TransitionInDays": 30
                            }
                        ]
                    }
                ])
            }
        })


class TestStackSynthesis:
    """Test CDK stack synthesis"""
    
    def test_stack_synthesizes_without_errors(self):
        """Test that the stack can be synthesized without errors"""
        app = cdk.App()
        stack = InvoiceIngestionStack(app, "TestStack")
        
        # This should not raise any exceptions
        template = app.synth().get_stack_by_name("TestStack").template
        
        # Basic validation that template was generated
        assert "Resources" in template
        assert len(template["Resources"]) > 0
    
    def test_stack_with_environment(self):
        """Test stack creation with specific environment"""
        app = cdk.App()
        stack = InvoiceIngestionStack(
            app, 
            "TestStackWithEnv",
            env=cdk.Environment(
                account="123456789012",
                region="us-west-2"
            )
        )
        
        template = assertions.Template.from_stack(stack)
        
        # Should still create all required resources
        template.resource_count_is("AWS::S3::Bucket", 1)
        template.resource_count_is("AWS::Lambda::Function", 1)
        template.resource_count_is("AWS::StepFunctions::StateMachine", 1)


if __name__ == '__main__':
    pytest.main([__file__])
