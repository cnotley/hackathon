import aws_cdk as cdk
from ingestion_stack import AuditFullStack

app = cdk.App()
AuditFullStack(app, "AuditFullStack")
app.synth()
