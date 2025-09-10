# Deployment & Local Testing

## Local
- Install deps: `pip install -r requirements.txt`
- Optional LocalStack: `localstack start -d` then `export LOCALSTACK_URL=http://localhost:4566`
- Tests: `pytest`
- UI: `streamlit run ui/app.py`

## AWS
- `cd infrastructure && cdk synth` (and then `cdk deploy` with proper AWS creds)
