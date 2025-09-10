# Disaster Recovery Invoice Auditing Prototype

This project implements a serverless Python application for auditing disaster recovery invoices using AWS services. It includes ingestion, extraction, AI agent reasoning, comparison, report generation, and a Streamlit UI. Infrastructure is managed via AWS CDK v2.

## Setup

1. Create virtual environment and install requirements:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Install and start LocalStack for local testing:
```bash
pip install localstack awscli-local
localstack start -d
```

3. Deploy infrastructure using CDK:
```bash
cd infrastructure
cdk deploy
```

4. Run tests:
```bash
pytest
```

5. Launch Streamlit UI:
```bash
streamlit run ui/app.py
```

Ensure AWS credentials are configured. See `docs/deployment.md` for detailed deployment instructions.
