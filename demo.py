from __future__ import annotations

import json
import os
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, List

import boto3
from botocore.exceptions import ClientError
from moto import mock_aws
import openpyxl
from unittest.mock import patch
import importlib

REGION = "us-east-1"
INGESTION_BUCKET = "invoice-ingestion-demo"
REPORTS_BUCKET = "reports-bucket-demo"
MSA_TABLE_NAME = "msa-rates"
PDF_KEY = "uploads/test-invoice.pdf"
PDF_PATH = Path(__file__).with_name("test-invoice.pdf")


def _fake_textract_blocks() -> List[Dict[str, object]]:
    return [
        {
            "BlockType": "TABLE",
            "Id": "table-1",
            "Relationships": [
                {
                    "Type": "CHILD",
                    "Ids": [
                        "cell-1",
                        "cell-2",
                        "cell-3",
                        "cell-4",
                        "cell-5",
                        "cell-6",
                        "cell-7",
                        "cell-8",
                        "cell-9",
                        "cell-10",
                        "cell-11",
                        "cell-12",
                    ],
                }
            ],
        },
        {"Id": "cell-1", "BlockType": "CELL", "RowIndex": 1, "ColumnIndex": 1, "Relationships": [{"Type": "CHILD", "Ids": ["word-worker"]}]},
        {"Id": "cell-2", "BlockType": "CELL", "RowIndex": 1, "ColumnIndex": 2, "Relationships": [{"Type": "CHILD", "Ids": ["word-type"]}]},
        {"Id": "cell-3", "BlockType": "CELL", "RowIndex": 1, "ColumnIndex": 3, "Relationships": [{"Type": "CHILD", "Ids": ["word-reg"]}]},
        {"Id": "cell-4", "BlockType": "CELL", "RowIndex": 1, "ColumnIndex": 4, "Relationships": [{"Type": "CHILD", "Ids": ["word-ot"]}]},
        {"Id": "cell-5", "BlockType": "CELL", "RowIndex": 1, "ColumnIndex": 5, "Relationships": [{"Type": "CHILD", "Ids": ["word-rate"]}]},
        {"Id": "cell-6", "BlockType": "CELL", "RowIndex": 1, "ColumnIndex": 6, "Relationships": [{"Type": "CHILD", "Ids": ["word-total"]}]},
        {"Id": "cell-7", "BlockType": "CELL", "RowIndex": 2, "ColumnIndex": 1, "Relationships": [{"Type": "CHILD", "Ids": ["word-name-1", "word-name-2"]}]},
        {"Id": "cell-8", "BlockType": "CELL", "RowIndex": 2, "ColumnIndex": 2, "Relationships": [{"Type": "CHILD", "Ids": ["word-type-value"]}]},
        {"Id": "cell-9", "BlockType": "CELL", "RowIndex": 2, "ColumnIndex": 3, "Relationships": [{"Type": "CHILD", "Ids": ["word-reg-value"]}]},
        {"Id": "cell-10", "BlockType": "CELL", "RowIndex": 2, "ColumnIndex": 4, "Relationships": [{"Type": "CHILD", "Ids": ["word-ot-value"]}]},
        {"Id": "cell-11", "BlockType": "CELL", "RowIndex": 2, "ColumnIndex": 5, "Relationships": [{"Type": "CHILD", "Ids": ["word-rate-value"]}]},
        {"Id": "cell-12", "BlockType": "CELL", "RowIndex": 2, "ColumnIndex": 6, "Relationships": [{"Type": "CHILD", "Ids": ["word-total-value"]}]},
        {"BlockType": "WORD", "Id": "word-worker", "Text": "Worker"},
        {"BlockType": "WORD", "Id": "word-type", "Text": "Type"},
        {"BlockType": "WORD", "Id": "word-reg", "Text": "Reg"},
        {"BlockType": "WORD", "Id": "word-ot", "Text": "OT"},
        {"BlockType": "WORD", "Id": "word-rate", "Text": "Rate"},
        {"BlockType": "WORD", "Id": "word-total", "Text": "Total"},
        {"BlockType": "WORD", "Id": "word-name-1", "Text": "Robbins,"},
        {"BlockType": "WORD", "Id": "word-name-2", "Text": "Dorian"},
        {"BlockType": "WORD", "Id": "word-type-value", "Text": "RS"},
        {"BlockType": "WORD", "Id": "word-reg-value", "Text": "40"},
        {"BlockType": "WORD", "Id": "word-ot-value", "Text": "15"},
        {"BlockType": "WORD", "Id": "word-rate-value", "Text": "77"},
        {"BlockType": "WORD", "Id": "word-total-value", "Text": "4812.5"},
        {"BlockType": "QUERY_RESULT", "Query": {"Alias": "VENDOR_NAME"}, "Text": "SERVPRO Commercial, LLC"},
    ]


def _bedrock_payload() -> Dict[str, object]:
    labour_rows: List[Dict[str, object]] = [
        {
            "name": "Robbins, Dorian",
            "type": "RS",
            "reg_hours": 40.0,
            "ot_hours": 15.0,
            "rate": 77.0,
            "total": 4812.5,
        }
    ]
    for idx in range(1, 44):
        labour_rows.append(
            {
                "name": f"Crew Member {idx}",
                "type": "GL",
                "reg_hours": 8.5,
                "ot_hours": 0.0,
                "rate": 42.0,
                "total": 357.0,
            }
        )
    return {
        "vendor": "SERVPRO",
        "labor": labour_rows,
        "summaries": {
            "total_regular_hours": 458.75,
            "total_ot_hours": 661.0,
            "total_labor_charges": 77150.25,
        },
    }


@contextmanager
def _patched_extraction(bedrock_response: Dict[str, object] | None, raise_client_error: bool = False):
    extraction_module = importlib.import_module("lambda.extraction_lambda")

    def fake_run_textract(_bucket: str, _key: str, _size: int) -> List[Dict[str, object]]:
        return _fake_textract_blocks()

    def fake_bedrock(raw_text: str) -> Dict[str, object]:
        if raise_client_error:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "denied"}}, "InvokeModel")
        return bedrock_response or {}

    with patch.object(extraction_module, "_run_textract", side_effect=fake_run_textract), patch.object(
        extraction_module, "_invoke_bedrock_for_extraction", side_effect=fake_bedrock
    ):
        yield extraction_module


def _invoke_lambda_chain(
    ingestion_module,
    extraction_module,
    reconciliation_module,
    report_module,
):
    def invoke(function_name: str, payload: Dict[str, object]) -> Dict[str, object]:
        if function_name == ingestion_module.EXTRACTION_LAMBDA_NAME:
            return extraction_module.lambda_handler(payload, None)
        if function_name == ingestion_module.RECONCILIATION_LAMBDA_NAME:
            return reconciliation_module.lambda_handler({"extraction": {"Payload": payload}, "vendor": payload.get("vendor")}, None)
        if function_name == ingestion_module.REPORT_LAMBDA_NAME:
            event = {
                "discrepancies": payload.get("reconciliation", {}).get("discrepancies", []),
                "total_savings": payload.get("reconciliation", {}).get("total_savings"),
                "vendor": payload.get("vendor"),
                "report_id": "test-invoice",
                "extracted_data": payload.get("extracted_data"),
            }
            if not event["discrepancies"]:
                reconciliation_payload = payload.get("reconciliation")
                if reconciliation_payload:
                    event["discrepancies"] = reconciliation_payload.get("discrepancies", [])
            if "extracted_data" not in payload:
                event["extracted_data"] = payload
            return report_module.lambda_handler(event, None)
        raise ValueError(f"Unexpected lambda invocation: {function_name}")

    return invoke


def _create_msa_table(dynamodb_resource):
    dynamodb_resource.create_table(
        TableName=MSA_TABLE_NAME,
        KeySchema=[{"AttributeName": "rate_id", "KeyType": "HASH"}, {"AttributeName": "effective_date", "KeyType": "RANGE"}],
        AttributeDefinitions=[
            {"AttributeName": "rate_id", "AttributeType": "S"},
            {"AttributeName": "effective_date", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )


def _seed_rates():
    seed_module = importlib.import_module("lambda.seed_msa_rates")
    seed_module.lambda_handler({"vendor": "SERVPRO"}, None)
    dynamodb_resource = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb_resource.Table(MSA_TABLE_NAME)
    table.update_item(
        Key={"rate_id": "SERVPRO#RS", "effective_date": seed_module.DEFAULT_EFFECTIVE_DATE},
        UpdateExpression="SET standard_rate = :rate",
        ExpressionAttributeValues={":rate": 70},
    )


def _upload_sample_pdf():
    if not PDF_PATH.exists():
        raise FileNotFoundError(f"Sample PDF not found at {PDF_PATH}")
    s3_client = boto3.client("s3", region_name=REGION)
    with PDF_PATH.open("rb") as pdf_file:
        s3_client.put_object(
            Bucket=INGESTION_BUCKET,
            Key=PDF_KEY,
            Body=pdf_file.read(),
            Metadata={"vendor": "Servpro Commercial, LLC"},
            ContentType="application/pdf",
        )


def _prepare_environment():
    os.environ.update(
        {
            "AWS_DEFAULT_REGION": REGION,
            "INGESTION_BUCKET": INGESTION_BUCKET,
            "STATE_MACHINE_ARN": "",
            "USE_SFN": "false",
            "DEFAULT_VENDOR_NAME": "SERVPRO",
            "REPORTS_BUCKET_NAME": REPORTS_BUCKET,
            "MSA_RATES_TABLE_NAME": MSA_TABLE_NAME,
            "MSA_VENDOR_NAME": "SERVPRO",
        }
    )


def _load_modules():
    ingestion_module = importlib.reload(importlib.import_module("lambda.ingestion_lambda"))
    reconciliation_module = importlib.reload(importlib.import_module("lambda.reconciliation_lambda"))
    report_module = importlib.reload(importlib.import_module("lambda.report_lambda"))
    return ingestion_module, reconciliation_module, report_module


def _run_ingestion_scenario(bedrock_response: Dict[str, object] | None, *, expect_bedrock: bool) -> Dict[str, object]:
    ingestion_module, reconciliation_module, report_module = _load_modules()
    with _patched_extraction(bedrock_response, raise_client_error=not expect_bedrock) as extraction_module:
        invoke_lambda = _invoke_lambda_chain(ingestion_module, extraction_module, reconciliation_module, report_module)
        with patch.object(ingestion_module, "_invoke_lambda", side_effect=invoke_lambda):
            s3_event = {
                "Records": [
                    {
                        "s3": {
                            "bucket": {"name": INGESTION_BUCKET},
                            "object": {"key": PDF_KEY},
                        }
                    }
                ]
            }
            response = ingestion_module.handle_s3_event(s3_event, None)
    result_body = response["body"]["results"][0]
    assert result_body["status"] == "fallback_completed"
    return result_body["details"]


def _assert_report_contents(vendor_expected: str, total_savings_expected: float):
    s3_client = boto3.client("s3", region_name=REGION)
    report_object = s3_client.get_object(Bucket=REPORTS_BUCKET, Key="reports/test-invoice.xlsx")
    workbook = openpyxl.load_workbook(BytesIO(report_object["Body"].read()))
    summary_sheet = workbook["Summary"]
    vendor_value = str(summary_sheet["B1"].value or "").strip().upper()
    assert vendor_value == vendor_expected
    summary_total = summary_sheet["B2"].value
    assert summary_total == pytest_approx(total_savings_expected)
    project_sheet = workbook["Project Summary"]
    rows = list(project_sheet.iter_rows(min_row=2, values_only=True))
    assert any(row[2] == "rate_variance" for row in rows)
    assert any(row[2] == "overtime" for row in rows)


def pytest_approx(value: float, tolerance: float = 1e-6) -> float:
    def _approx(other: float) -> bool:
        return abs(other - value) <= tolerance

    class Approx(float):
        def __eq__(self, other: object) -> bool:
            if isinstance(other, (int, float)):
                return _approx(float(other))
            return False

    return Approx(value)


def main() -> None:
    with mock_aws():
        _prepare_environment()
        s3_client = boto3.client("s3", region_name=REGION)
        s3_client.create_bucket(Bucket=INGESTION_BUCKET)
        s3_client.create_bucket(Bucket=REPORTS_BUCKET)
        dynamodb_resource = boto3.resource("dynamodb", region_name=REGION)
        _create_msa_table(dynamodb_resource)
        _seed_rates()
        _upload_sample_pdf()

        bedrock_response = _bedrock_payload()
        details = _run_ingestion_scenario(bedrock_response, expect_bedrock=True)
        savings_value = details["reconciliation"].get("total_savings", 0.0)
        assert details["extraction"]["vendor"] == "SERVPRO"
        assert savings_value >= 0.0
        _assert_report_contents("SERVPRO", savings_value)

        # Bedrock failure scenario falls back to Textract-only extraction
        details_fallback = _run_ingestion_scenario({}, expect_bedrock=False)
        assert details_fallback["extraction"]["vendor"] == "SERVPRO"
        assert details_fallback["summaries"] == {}
        print(json.dumps({
            "status": "ok",
            "report_key": details["report"]["key"],
            "total_savings": savings_value,
        }, indent=2))


if __name__ == "__main__":
    main()
