import json
import logging
import os
import subprocess
from typing import Any, Dict, List

import boto3
from openpyxl import Workbook
from openpyxl.styles import PatternFill

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')


def generate_report(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Generate Excel and PDF report and upload to S3."""
    flags: List[Dict[str, Any]] = event.get('flags', [])
    wb = Workbook()
    ws = wb.active
    ws.title = 'Project Summary'
    ws.append(['Category', 'As Presented', 'Analyzed', 'Hold'])
    summary = event.get('summary', {'labor': 77000, 'analyzed': 70000, 'hold': 7000})
    ws.append(['Labor', summary.get('labor'), summary.get('analyzed'), summary.get('hold')])
    if summary.get('hold', 0) > 0:
        ws['D2'].fill = PatternFill(start_color='FF0000', end_color='FF0000', fill_type='solid')

    xlsx_path = '/tmp/report.xlsx'
    wb.save(xlsx_path)

    bucket = event.get('bucket', os.environ.get('REPORT_BUCKET', 'reports'))
    key = event.get('key', 'report.xlsx')
    try:
        s3.upload_file(xlsx_path, bucket, key)
    except Exception as exc:
        logger.error('Failed to upload report: %s', exc)

    md_path = '/tmp/report.md'
    with open(md_path, 'w') as fh:
        fh.write('# Invoice Audit Report\n')
        for flag in flags:
            fh.write(f"- {flag['details']}\n")
    pdf_path = '/tmp/report.pdf'
    try:
        subprocess.call(['wkhtmltopdf', md_path, pdf_path])
    except FileNotFoundError:
        logger.warning('wkhtmltopdf not installed, skipping PDF generation')
    if os.path.exists(pdf_path):
        try:
            s3.upload_file(pdf_path, bucket, key.replace('.xlsx', '.pdf'))
        except Exception as exc:
            logger.error('Failed to upload PDF: %s', exc)

    return {'bucket': bucket, 'key': key}
