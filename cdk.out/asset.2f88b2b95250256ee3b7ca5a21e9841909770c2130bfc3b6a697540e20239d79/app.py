"""Minimal Streamlit UI for the invoice auditing prototype."""

from __future__ import annotations

import json
import os
import time
from io import BytesIO
from typing import Any, Dict, List

import boto3
import pandas as pd
import streamlit as st
from botocore.exceptions import ClientError

INGESTION_BUCKET = os.environ.get("INGESTION_BUCKET_NAME")
REPORTS_BUCKET = os.environ.get("REPORTS_BUCKET_NAME")
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")

s3_client = boto3.client("s3")
stepfunctions_client = boto3.client("stepfunctions")

st.set_page_config(page_title="Invoice Auditor", page_icon="📄", layout="wide")
st.title("📄 Invoice Auditing Prototype")

if not INGESTION_BUCKET or not STATE_MACHINE_ARN or not REPORTS_BUCKET:
    st.error("Missing required environment variables for buckets or Step Functions.")
    st.stop()


def _upload_pdf(file) -> str | None:
    key = f"uploads/{int(time.time())}_{file.name}"
    try:
        s3_client.put_object(Bucket=INGESTION_BUCKET, Key=key, Body=file.getvalue(), ContentType="application/pdf")
        return key
    except ClientError as exc:  # pragma: no cover
        st.error(f"Upload failed: {exc}")
        return None


def _start_execution(key: str) -> str | None:
    try:
        response = stepfunctions_client.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=f"audit-{int(time.time())}",
            input=json.dumps({"bucket": INGESTION_BUCKET, "key": key})
        )
        return response["executionArn"]
    except ClientError as exc:  # pragma: no cover
        st.error(f"Unable to start Step Functions execution: {exc}")
        return None


def _poll_execution(execution_arn: str) -> Dict[str, Any]:
    response = stepfunctions_client.describe_execution(executionArn=execution_arn)
    status = response.get("status", "UNKNOWN")
    output = {}
    if status in {"SUCCEEDED", "FAILED", "TIMED_OUT"} and response.get("output"):
        try:
            output = json.loads(response["output"])
        except json.JSONDecodeError:
            output = {"raw": response["output"]}
    return {"status": status, "output": output, "executionArn": execution_arn}


def _display_discrepancies(output: Dict[str, Any]) -> None:
    discrepancies = output.get("discrepancies") or output.get("discrepancy_analysis", {}).get("rate_variances")
    if not discrepancies:
        st.info("No discrepancies reported.")
        return
    df = pd.DataFrame(discrepancies)
    st.dataframe(df, use_container_width=True)


def _send_decision(task_token: str, approved: bool) -> None:
    payload = json.dumps({"approved": approved})
    try:
        stepfunctions_client.send_task_success(taskToken=task_token, output=payload)
        st.success("Decision sent to Step Functions.")
    except ClientError as exc:  # pragma: no cover
        st.error(f"Failed to send decision: {exc}")


def _list_reports(prefix: str | None = None) -> List[Dict[str, Any]]:
    list_kwargs: Dict[str, Any] = {"Bucket": REPORTS_BUCKET, "Prefix": "reports/"}
    if prefix:
        list_kwargs["Prefix"] = prefix
    response = s3_client.list_objects_v2(**list_kwargs)
    contents = response.get("Contents", [])
    return sorted(contents, key=lambda obj: obj["LastModified"], reverse=True)


def _download_button(obj: Dict[str, Any]) -> None:
    key = obj["Key"]
    try:
        body = s3_client.get_object(Bucket=REPORTS_BUCKET, Key=key)["Body"].read()
    except ClientError as exc:  # pragma: no cover
        st.error(f"Download failed: {exc}")
        return
    filename = key.split("/")[-1]
    st.download_button("Download", data=body, file_name=filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


uploaded_file = st.file_uploader("Upload invoice PDF", type=["pdf"])
if uploaded_file is not None:
    filename = uploaded_file.name
    key = f"uploads/{int(time.time())}_{filename}"
    try:
        s3_client.upload_fileobj(uploaded_file, INGESTION_BUCKET, key, ExtraArgs={"ContentType": "application/pdf"})
        st.success(f"Uploaded to s3://{INGESTION_BUCKET}/{key}")
        execution_arn = _start_execution(key)
        if execution_arn:
            st.session_state.execution_arn = execution_arn
            st.info("Execution started. Use the status panel to monitor progress.")
    except ClientError as exc:  # pragma: no cover
        st.error(f"Upload failed: {exc}")

execution_arn = st.session_state.get("execution_arn")
if execution_arn:
    st.subheader("Execution status")
    result = _poll_execution(execution_arn)
    st.write(f"Status: {result['status']}")
    if result["status"] in {"RUNNING", "STARTED"}:
        st.button("Refresh", on_click=lambda: st.experimental_rerun())
    if result["status"] == "SUCCEEDED":
        _display_discrepancies(result.get("output", {}))
    if result["status"] == "FAILED":
        st.error(result.get("output"))

st.subheader("Reports")
reports = _list_reports()
if reports:
    latest = reports[0]
    st.write(f"Latest report: {latest['Key'].split('/')[-1]} ({latest['LastModified']})")
    _download_button(latest)
else:
    st.info("No reports available yet.")
