import sys
import pytest

try:
    import aws_cdk as cdk  # pragma: no cover - optional
except Exception:  # noqa: F401
    pytest.skip("aws_cdk not installed", allow_module_level=True)

sys.path.append("infrastructure")
from ingestion_stack import AuditFullStack

def test_cdk_synth():
    app = cdk.App()
    AuditFullStack(app, "AuditFullStackTest")
    assembly = app.synth()
    names = [s.stack_name for s in assembly.stacks]
    assert "AuditFullStackTest" in names
