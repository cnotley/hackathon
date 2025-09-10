"""
Test suite for deployment and integration components.
Tests CDK synthesis, deployment simulation, and full stack integration.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import os
import sys
from pathlib import Path

# Add the project root to the path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import aws_cdk as cdk
from aws_cdk import assertions

# Import our stacks
from infrastructure.full_stack import MSAInvoiceAuditFullStack
from infrastructure.ingestion_stack import IngestionStack
from infrastructure.agent_stack import AgentStack
from infrastructure.ui_stack import UIStack
import app


class TestCDKSynthesis(unittest.TestCase):
    """Test CDK template synthesis for all stacks."""
    
    def setUp(self):
        """Set up test environment."""
        self.app = cdk.App()
        
    def test_full_stack_synthesis(self):
        """Test that the full stack synthesizes without errors."""
        # Create the full stack
        stack = MSAInvoiceAuditFullStack(
            self.app, 
            "TestMSAInvoiceAuditFullStack",
            env=cdk.Environment(account="123456789012", region="us-east-1")
        )
        
        # Generate template
        template = assertions.Template.from_stack(stack)
        
        # Verify key resources exist
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketName": assertions.Match.string_like_regexp("msa-invoice-ingestion-.*")
        })
        
        template.has_resource_properties("AWS::Lambda::Function", {
            "FunctionName": assertions.Match.string_like_regexp(".*-extraction-lambda")
        })
        
        template.has_resource_properties("AWS::StepFunctions::StateMachine", {
            "StateMachineName": assertions.Match.string_like_regexp(".*-audit-workflow")
        })
        
        template.has_resource_properties("AWS::DynamoDB::Table", {
            "TableName": assertions.Match.string_like_regexp(".*-msa-rates")
        })
        
        # Verify Bedrock Agent resources
        template.has_resource_properties("AWS::Bedrock::Agent", {
            "AgentName": assertions.Match.string_like_regexp(".*-msa-audit-agent")
        })
        
        # Verify App Runner service for UI
        template.has_resource_properties("AWS::AppRunner::Service", {
            "ServiceName": assertions.Match.string_like_regexp(".*-msa-audit-ui")
        })
        
    def test_modular_stacks_synthesis(self):
        """Test that individual stacks synthesize correctly."""
        # Test ingestion stack
        ingestion_stack = IngestionStack(
            self.app, 
            "TestIngestionStack",
            env=cdk.Environment(account="123456789012", region="us-east-1")
        )
        
        ingestion_template = assertions.Template.from_stack(ingestion_stack)
        ingestion_template.has_resource_properties("AWS::S3::Bucket", {})
        ingestion_template.has_resource_properties("AWS::Lambda::Function", {})
        
        # Test agent stack
        agent_stack = AgentStack(
            self.app, 
            "TestAgentStack",
            env=cdk.Environment(account="123456789012", region="us-east-1")
        )
        
        agent_template = assertions.Template.from_stack(agent_stack)
        agent_template.has_resource_properties("AWS::Bedrock::Agent", {})
        
        # Test UI stack
        ui_stack = UIStack(
            self.app, 
            "TestUIStack",
            env=cdk.Environment(account="123456789012", region="us-east-1")
        )
        
        ui_template = assertions.Template.from_stack(ui_stack)
        ui_template.has_resource_properties("AWS::AppRunner::Service", {})


class TestDeploymentSimulation(unittest.TestCase):
    """Test deployment simulation and validation."""
    
    def setUp(self):
        """Set up test environment."""
        self.app = cdk.App()
        
    @patch('boto3.client')
    def test_deployment_prerequisites(self, mock_boto3):
        """Test that deployment prerequisites are met."""
        # Mock AWS clients
        mock_sts = Mock()
        mock_sts.get_caller_identity.return_value = {
            'Account': '123456789012',
            'Arn': 'arn:aws:iam::123456789012:user/test-user'
        }
        
        mock_s3 = Mock()
        mock_s3.list_buckets.return_value = {'Buckets': []}
        
        mock_boto3.side_effect = lambda service: {
            'sts': mock_sts,
            's3': mock_s3
        }.get(service, Mock())
        
        # Simulate deployment validation
        result = self._validate_deployment_prerequisites()
        self.assertTrue(result['valid'])
        self.assertEqual(result['account'], '123456789012')
        
    def _validate_deployment_prerequisites(self):
        """Validate deployment prerequisites."""
        try:
            import boto3
            
            # Check AWS credentials
            sts = boto3.client('sts')
            identity = sts.get_caller_identity()
            
            # Check CDK bootstrap
            s3 = boto3.client('s3')
            buckets = s3.list_buckets()
            
            return {
                'valid': True,
                'account': identity['Account'],
                'buckets': len(buckets['Buckets'])
            }
        except Exception as e:
            return {
                'valid': False,
                'error': str(e)
            }
    
    def test_deployment_configuration(self):
        """Test deployment configuration validation."""
        # Test full deployment mode
        full_config = {
            'deployment_mode': 'full',
            'environment': {
                'account': '123456789012',
                'region': 'us-east-1'
            }
        }
        
        self.assertTrue(self._validate_config(full_config))
        
        # Test modular deployment mode
        modular_config = {
            'deployment_mode': 'modular',
            'environment': {
                'account': '123456789012',
                'region': 'us-east-1'
            }
        }
        
        self.assertTrue(self._validate_config(modular_config))
        
    def _validate_config(self, config):
        """Validate deployment configuration."""
        required_fields = ['deployment_mode', 'environment']
        for field in required_fields:
            if field not in config:
                return False
                
        if 'account' not in config['environment']:
            return False
            
        if 'region' not in config['environment']:
            return False
            
        return True


class TestIntegrationValidation(unittest.TestCase):
    """Test integration between components."""
    
    def setUp(self):
        """Set up test environment."""
        self.app = cdk.App()
        
    def test_lambda_integration(self):
        """Test Lambda function integration points."""
        stack = MSAInvoiceAuditFullStack(
            self.app, 
            "TestIntegrationStack",
            env=cdk.Environment(account="123456789012", region="us-east-1")
        )
        
        template = assertions.Template.from_stack(stack)
        
        # Verify Lambda functions have proper IAM roles
        template.has_resource_properties("AWS::IAM::Role", {
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
            }
        })
        
        # Verify Step Functions integration
        template.has_resource_properties("AWS::StepFunctions::StateMachine", {
            "RoleArn": assertions.Match.any_value()
        })
        
    def test_s3_integration(self):
        """Test S3 bucket integration and permissions."""
        stack = MSAInvoiceAuditFullStack(
            self.app, 
            "TestS3IntegrationStack",
            env=cdk.Environment(account="123456789012", region="us-east-1")
        )
        
        template = assertions.Template.from_stack(stack)
        
        # Verify S3 buckets exist
        template.resource_count_is("AWS::S3::Bucket", 4)  # ingestion, reports, templates, knowledge-base
        
        # Verify bucket policies for cross-service access
        template.has_resource_properties("AWS::S3::BucketPolicy", {
            "PolicyDocument": {
                "Statement": assertions.Match.any_value()
            }
        })
        
    def test_bedrock_integration(self):
        """Test Bedrock Agent and Knowledge Base integration."""
        stack = MSAInvoiceAuditFullStack(
            self.app, 
            "TestBedrockIntegrationStack",
            env=cdk.Environment(account="123456789012", region="us-east-1")
        )
        
        template = assertions.Template.from_stack(stack)
        
        # Verify Bedrock Agent
        template.has_resource_properties("AWS::Bedrock::Agent", {
            "AgentName": assertions.Match.string_like_regexp(".*-msa-audit-agent"),
            "FoundationModel": "anthropic.claude-3-5-sonnet-20241022-v2:0"
        })
        
        # Verify Knowledge Base
        template.has_resource_properties("AWS::Bedrock::KnowledgeBase", {
            "Name": assertions.Match.string_like_regexp(".*-msa-knowledge-base")
        })


class TestErrorHandling(unittest.TestCase):
    """Test error handling in deployment scenarios."""
    
    def test_invalid_configuration(self):
        """Test handling of invalid deployment configurations."""
        app_instance = cdk.App()
        
        # Test missing environment
        with self.assertRaises(Exception):
            MSAInvoiceAuditFullStack(app_instance, "InvalidStack")
            
    def test_resource_naming_conflicts(self):
        """Test handling of resource naming conflicts."""
        app_instance = cdk.App()
        
        # Create two stacks with same resources - should handle naming
        stack1 = MSAInvoiceAuditFullStack(
            app_instance, 
            "Stack1",
            env=cdk.Environment(account="123456789012", region="us-east-1")
        )
        
        stack2 = MSAInvoiceAuditFullStack(
            app_instance, 
            "Stack2", 
            env=cdk.Environment(account="123456789012", region="us-east-1")
        )
        
        # Both should synthesize without conflicts
        template1 = assertions.Template.from_stack(stack1)
        template2 = assertions.Template.from_stack(stack2)
        
        self.assertIsNotNone(template1)
        self.assertIsNotNone(template2)


class TestDeploymentModes(unittest.TestCase):
    """Test different deployment modes."""
    
    def test_full_deployment_mode(self):
        """Test full deployment mode creates all resources."""
        # Simulate app.py with full deployment mode
        test_app = cdk.App()
        test_app.node.set_context("deployment", "full")
        
        # This would create the full stack
        deployment_mode = test_app.node.try_get_context("deployment") or "full"
        self.assertEqual(deployment_mode, "full")
        
        # Verify full stack creation
        if deployment_mode == "full":
            stack = MSAInvoiceAuditFullStack(
                test_app,
                "MSAInvoiceAuditFullStack",
                env=cdk.Environment(account="123456789012", region="us-east-1")
            )
            
            template = assertions.Template.from_stack(stack)
            
            # Should have all major resource types
            template.resource_count_is("AWS::S3::Bucket", 4)
            template.resource_count_is("AWS::Lambda::Function", 4)  # extraction, agent, comparison, report
            template.resource_count_is("AWS::StepFunctions::StateMachine", 1)
            template.resource_count_is("AWS::DynamoDB::Table", 1)
            template.resource_count_is("AWS::Bedrock::Agent", 1)
            template.resource_count_is("AWS::AppRunner::Service", 1)
            
    def test_modular_deployment_mode(self):
        """Test modular deployment mode creates separate stacks."""
        test_app = cdk.App()
        test_app.node.set_context("deployment", "modular")
        
        deployment_mode = test_app.node.try_get_context("deployment") or "full"
        self.assertEqual(deployment_mode, "modular")
        
        # In modular mode, we'd create separate stacks
        if deployment_mode == "modular":
            # This simulates what app.py would do
            ingestion_stack = IngestionStack(
                test_app,
                "IngestionStack",
                env=cdk.Environment(account="123456789012", region="us-east-1")
            )
            
            agent_stack = AgentStack(
                test_app,
                "AgentStack", 
                env=cdk.Environment(account="123456789012", region="us-east-1")
            )
            
            # Verify separate stacks
            ingestion_template = assertions.Template.from_stack(ingestion_stack)
            agent_template = assertions.Template.from_stack(agent_stack)
            
            self.assertIsNotNone(ingestion_template)
            self.assertIsNotNone(agent_template)


class TestResourceValidation(unittest.TestCase):
    """Test validation of deployed resources."""
    
    def test_iam_permissions(self):
        """Test IAM permissions are correctly configured."""
        stack = MSAInvoiceAuditFullStack(
            cdk.App(), 
            "TestIAMStack",
            env=cdk.Environment(account="123456789012", region="us-east-1")
        )
        
        template = assertions.Template.from_stack(stack)
        
        # Verify Lambda execution roles have necessary permissions
        template.has_resource_properties("AWS::IAM::Policy", {
            "PolicyDocument": {
                "Statement": assertions.Match.array_with([
                    assertions.Match.object_like({
                        "Effect": "Allow",
                        "Action": assertions.Match.array_with(["s3:GetObject", "s3:PutObject"])
                    })
                ])
            }
        })
        
    def test_security_configurations(self):
        """Test security configurations are properly set."""
        stack = MSAInvoiceAuditFullStack(
            cdk.App(), 
            "TestSecurityStack",
            env=cdk.Environment(account="123456789012", region="us-east-1")
        )
        
        template = assertions.Template.from_stack(stack)
        
        # Verify S3 buckets have encryption
        template.has_resource_properties("AWS::S3::Bucket", {
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": assertions.Match.any_value()
            }
        })
        
        # Verify DynamoDB table has encryption
        template.has_resource_properties("AWS::DynamoDB::Table", {
            "SSESpecification": {
                "SSEEnabled": True
            }
        })


if __name__ == '__main__':
    # Run all tests
    unittest.main(verbosity=2)
