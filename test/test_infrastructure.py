import aws_cdk as cdk
import sys
sys.path.append("infrastructure")
from ingestion_stack import AuditFullStack

def test_cdk_synth():
    app = cdk.App()
    AuditFullStack(app, "AuditFullStackTest")
    assembly = app.synth()
    names = [s.stack_name for s in assembly.stacks]
    assert "AuditFullStackTest" in names
