"""Minimal report Lambda that writes discrepancies to Excel in S3."""

from __future__ import annotations

import json
import logging
import os
from io import BytesIO
from typing import Any, Dict, List

import boto3
import openpyxl
from openpyxl.styles import Alignment, Font, NamedStyle
from openpyxl.utils import get_column_letter

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv("LOG_LEVEL", "INFO"))

REPORTS_BUCKET = os.getenv("REPORTS_BUCKET_NAME")
S3 = boto3.client("s3")

HEADER_STYLE = NamedStyle(name="header_style")
HEADER_STYLE.font = Font(bold=True)
HEADER_STYLE.alignment = Alignment(horizontal="center")

CURRENCY_STYLE = NamedStyle(name="currency_style")
CURRENCY_STYLE.number_format = "$#,##0.00"


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    flags = event.get("discrepancies") or event.get("reconciliation", {}).get("discrepancies") or []
    report_id = event.get("report_id") or "audit-report"
    vendor = (event.get("vendor")
              or event.get("metadata", {}).get("vendor")
              or event.get("extracted_data", {}).get("vendor")
              or "UNKNOWN").upper()

    total_savings = float(event.get("total_savings") or event.get("reconciliation", {}).get("total_savings") or 0.0)

    workbook = openpyxl.Workbook()
    _register_styles(workbook)

    project_sheet = workbook.active
    project_sheet.title = "Project Summary"
    headers = [
        "Worker",
        "Labor Type",
        "Issue Type",
        "Hours",
        "Actual Rate",
        "MSA Rate",
        "Variance",
        "Savings / Details",
    ]
    project_sheet.append(headers)
    _style_header_row(project_sheet[1])

    savings_total = 0.0
    for item in flags:
        row, row_savings = _format_discrepancy_row(item)
        project_sheet.append(row)
        savings_total += row_savings

    if project_sheet.max_row == 1:
        project_sheet.append(["No discrepancies detected", "", "", "", "", "", "", ""])

    totals_row_idx = project_sheet.max_row + 1
    project_sheet.append(["", "", "Totals", "", "", "", "", savings_total])
    totals_row = project_sheet[totals_row_idx]
    totals_row[2].font = Font(bold=True)
    totals_row[7].style = CURRENCY_STYLE

    _auto_size_columns(project_sheet)

    summary_sheet = workbook.create_sheet("Summary")
    summary_sheet.append(["Vendor", vendor])
    summary_sheet.append(["Total Savings", total_savings])
    summary_sheet["B2"].style = CURRENCY_STYLE
    _style_summary_labels(summary_sheet["A1"], summary_sheet["A2"])

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)

    key = f"reports/{report_id}.xlsx"
    S3.put_object(
        Bucket=REPORTS_BUCKET,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    LOGGER.info("Report written to s3://%s/%s", REPORTS_BUCKET, key)

    return {
        "status": "ok",
        "bucket": REPORTS_BUCKET,
        "key": key,
        "vendor": vendor,
        "total_savings": round(total_savings, 2),
    }


def _format_discrepancy_row(item: Dict[str, Any]) -> tuple[List[Any], float]:
    discrepancy_type = item.get("type", "unknown")
    savings = float(item.get("savings", 0.0) or 0.0)

    base_row: List[Any] = [
        item.get("worker"),
        item.get("labor_type"),
        discrepancy_type,
        item.get("hours"),
        item.get("actual_rate"),
        item.get("msa_rate"),
        item.get("variance_multiplier") or item.get("type"),
        savings,
    ]

    if discrepancy_type == "overtime":
        base_row[7] = item.get("overtime_hours")
    elif discrepancy_type == "cost_anomaly":
        base_row[6] = item.get("z_score")
        base_row[7] = item.get("total")
    elif discrepancy_type == "missing_rate":
        base_row[7] = item.get("rate_id")
    elif discrepancy_type == "duplicate":
        base_row[7] = "Duplicate entry"

    return base_row, savings


def _register_styles(workbook: openpyxl.Workbook) -> None:
    if HEADER_STYLE.name not in workbook.named_styles:
        workbook.add_named_style(HEADER_STYLE)
    if CURRENCY_STYLE.name not in workbook.named_styles:
        workbook.add_named_style(CURRENCY_STYLE)


def _style_header_row(row) -> None:
    for cell in row:
        cell.style = HEADER_STYLE


def _style_summary_labels(*cells) -> None:
    for cell in cells:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="left")


def _auto_size_columns(sheet: openpyxl.worksheet.worksheet.Worksheet) -> None:
    for column_idx, column in enumerate(sheet.columns, start=1):
        max_length = 0
        for cell in column:
            if cell.value is not None:
                max_length = max(max_length, len(str(cell.value)))
        adjusted_width = max(max_length + 2, 12)
        sheet.column_dimensions[get_column_letter(column_idx)].width = adjusted_width


if __name__ == "__main__":
    sample_event = {
        "discrepancies": [
            {
                "type": "rate_variance",
                "worker": "Robbins, Dorian",
                "labor_type": "RS",
                "hours": 40,
                "actual_rate": 85,
                "msa_rate": 77,
                "variance_multiplier": 1.10,
                "savings": 320,
            },
            {
                "type": "overtime",
                "worker": "Robbins, Dorian",
                "labor_type": "RS",
                "hours": 48,
                "overtime_hours": 8,
            },
        ],
        "total_savings": 320.0,
        "vendor": "SERVPRO",
        "report_id": "demo",
    }
    print(json.dumps(lambda_handler(sample_event, None), indent=2, default=str))
