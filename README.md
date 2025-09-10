# Disaster Recovery Invoice Auditing Prototype (AWS Serverless)

**Tech stack:** S3, Textract, Bedrock, Lambda, Step Functions, DynamoDB, SageMaker (anomaly detection), Streamlit UI, AWS CDK (Python).

**Local dev:** LocalStack + pdfplumber fallback for Textract, sklearn IsolationForest fallback for SageMaker, simple Bedrock/Comprehend fallbacks.


## Quick start (local, fully offline)
1. Python 3.10+ and pip
2. `pip install -r requirements.txt`
3. Optional: `localstack start -d` then `export LOCALSTACK_URL=http://localhost:4566`
4. Run tests: `pytest -q`
5. Start UI: `streamlit run ui/app.py`

## AWS deploy (synth)
```bash
cd infrastructure
cdk synth
```
See `docs/deployment.md` for full deployment notes.
