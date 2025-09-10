from aws_cdk import App
from aws_cdk.assertions import Template
from infrastructure.app import AuditFullStack


def test_cdk_synth():
    app = App(context={'state_machine_arn': 'arn:aws:states:us-east-1:123:sm'})
    stack = AuditFullStack(app, 'AuditFullStackTest')
    template = Template.from_stack(stack)
    template.resource_count_is('AWS::S3::Bucket', 1)
