"""Minimal report Lambda that writes discrepancies to Excel in S3."""

from __future__ import annotations

import json
import logging
import os
from io import BytesIO
from typing import Any, Dict, List

import boto3
import openpyxl
from openpyxl.styles import Alignment, Font

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv("LOG_LEVEL", "INFO"))

REPORTS_BUCKET = os.getenv("REPORTS_BUCKET_NAME")
S3 = boto3.client("s3")


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    flags = event.get("discrepancies") or event.get("reconciliation", {}).get("discrepancies") or []
    report_id = event.get("report_id") or "audit-report"

    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Project Summary"

    headers = ["Name", "Type", "Hours", "Actual Rate", "MSA Rate", "Variance", "As Presented", "As Analyzed"]
    sheet.append(headers)
    _style_header(sheet[1])

    total_savings = float(event.get("total_savings", 0.0))

    for item in flags:
        if item.get("type") == "rate_variance":
            row = [
                item.get("worker"),
                item.get("labor_type"),
                item.get("hours"),
                item.get("actual_rate"),
                item.get("msa_rate"),
                item.get("variance_multiplier"),
                item.get("actual_rate"),
                item.get("msa_rate"),
            ]
        elif item.get("type") == "overtime":
            row = [
                item.get("worker"),
                item.get("labor_type"),
                item.get("hours"),
                None,
                None,
                "overtime",
                None,
                None,
            ]
        else:
            row = [
                item.get("worker"),
                item.get("labor_type"),
                item.get("hours"),
                item.get("rate"),
                item.get("msa_rate"),
                item.get("type"),
                None,
                None,
            ]
        sheet.append(row)

    summary = workbook.create_sheet("Summary")
    summary.append(["Total Savings", total_savings])

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)

    key = f"reports/{report_id}.xlsx"
    S3.put_object(Bucket=REPORTS_BUCKET, Key=key, Body=buffer.getvalue(), ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    return {
        "status": "ok",
        "bucket": REPORTS_BUCKET,
        "key": key,
        "total_savings": total_savings,
    }


def _style_header(row):
    for cell in row:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")


if __name__ == "__main__":
    sample_event = {
        "discrepancies": [
            {"type": "rate_variance", "worker": "Alice", "labor_type": "RS", "hours": 45, "actual_rate": 80, "msa_rate": 70, "variance_multiplier": 1.14},
            {"type": "overtime", "worker": "Bob", "labor_type": "US", "hours": 50},
        ],
        "total_savings": 350.0,
        "report_id": "demo",
    }
    print(json.dumps(lambda_handler(sample_event, None), indent=2))
