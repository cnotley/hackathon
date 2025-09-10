"""Tests for minimal extraction lambda."""

from __future__ import annotations

import json
from decimal import Decimal
from importlib import import_module
from io import BytesIO
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

BUCKET = "input-bucket"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def _load_extraction_module():
    return import_module("lambda.extraction_lambda")


@mock_aws
def test_extraction_returns_labour_rows(monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    s3.put_object(Bucket=BUCKET, Key="invoice.pdf", Body=b"pdf")

    fake_blocks = [
        {"BlockType": "TABLE", "Id": "1", "Relationships": [{"Type": "CHILD", "Ids": ["2", "3"]}]},
        {"Id": "2", "BlockType": "CELL", "RowIndex": 1, "ColumnIndex": 1, "Relationships": [{"Type": "CHILD", "Ids": ["4"]}]},
        {"Id": "3", "BlockType": "CELL", "RowIndex": 1, "ColumnIndex": 2, "Relationships": [{"Type": "CHILD", "Ids": ["5"]}]},
        {"Id": "4", "BlockType": "WORD", "Text": "Worker"},
        {"Id": "5", "BlockType": "WORD", "Text": "Hours"},
    ]

    with patch("lambda.extraction_lambda.textract_client", MagicMock(analyze_document=MagicMock(return_value={"Blocks": fake_blocks}))):
        extraction_lambda = _load_extraction_module()
        event = {"bucket": BUCKET, "key": "invoice.pdf"}
        result = extraction_lambda.lambda_handler(event, None)
        assert result["status"] == "ok"
        assert result["bucket"] == BUCKET


def test_extraction_missing_location():
    extraction_lambda = _load_extraction_module()

    with pytest.raises(ValueError):
        extraction_lambda.lambda_handler({}, None)


def test_parse_query_results_extracts_vendor():
    extraction_lambda = _load_extraction_module()
    blocks = [
        {
            "BlockType": "QUERY_RESULT",
            "Query": {"Alias": "VENDOR_NAME"},
            "Text": "Servpro Commercial, LLC",
        },
        {
            "BlockType": "QUERY_RESULT",
            "Query": {"Alias": "INVOICE_TOTAL"},
            "Text": "$4,321.45",
        },
    ]

    metadata = extraction_lambda._parse_query_results(blocks)
    assert metadata["vendor"] == "SERVPRO COMMERCIAL, LLC"
    assert metadata["invoice_total"] == pytest.approx(4321.45)


def test_to_float_handles_ocr_noise():
    extraction_lambda = _load_extraction_module()

    assert extraction_lambda._to_float("5C") == pytest.approx(50.0)
    assert extraction_lambda._to_float("ooc") == pytest.approx(0.0)
    assert extraction_lambda._to_float(Decimal("12.5")) == pytest.approx(12.5)


def test_labour_entries_normalizes_regular_ot():
    extraction_lambda = _load_extraction_module()
    table = [
        ["Worker", "Type", "Reg", "OT", "Rate", "Total"],
        ["Robbins, Dorian", "RS", "32", "8", "77", "3080"],
    ]

    labour_rows = extraction_lambda._labour_entries([table])
    assert len(labour_rows) == 1
    record = labour_rows[0]
    assert record["hours"] == pytest.approx(40.0)
    assert record["hours_regular"] == pytest.approx(32.0)
    assert record["hours_ot"] == pytest.approx(8.0)
    assert record["rate"] == pytest.approx(77.0)


def _fake_textract_blocks() -> list[dict]:
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
        {
            "Id": "cell-1",
            "BlockType": "CELL",
            "RowIndex": 1,
            "ColumnIndex": 1,
            "Relationships": [{"Type": "CHILD", "Ids": ["word-worker"]}],
        },
        {
            "Id": "cell-2",
            "BlockType": "CELL",
            "RowIndex": 1,
            "ColumnIndex": 2,
            "Relationships": [{"Type": "CHILD", "Ids": ["word-type"]}],
        },
        {
            "Id": "cell-3",
            "BlockType": "CELL",
            "RowIndex": 1,
            "ColumnIndex": 3,
            "Relationships": [{"Type": "CHILD", "Ids": ["word-reg"]}],
        },
        {
            "Id": "cell-4",
            "BlockType": "CELL",
            "RowIndex": 1,
            "ColumnIndex": 4,
            "Relationships": [{"Type": "CHILD", "Ids": ["word-ot"]}],
        },
        {
            "Id": "cell-5",
            "BlockType": "CELL",
            "RowIndex": 1,
            "ColumnIndex": 5,
            "Relationships": [{"Type": "CHILD", "Ids": ["word-rate"]}],
        },
        {
            "Id": "cell-6",
            "BlockType": "CELL",
            "RowIndex": 1,
            "ColumnIndex": 6,
            "Relationships": [{"Type": "CHILD", "Ids": ["word-total"]}],
        },
        {
            "Id": "cell-7",
            "BlockType": "CELL",
            "RowIndex": 2,
            "ColumnIndex": 1,
            "Relationships": [{"Type": "CHILD", "Ids": ["word-name-1", "word-name-2"]}],
        },
        {
            "Id": "cell-8",
            "BlockType": "CELL",
            "RowIndex": 2,
            "ColumnIndex": 2,
            "Relationships": [{"Type": "CHILD", "Ids": ["word-type-value"]}],
        },
        {
            "Id": "cell-9",
            "BlockType": "CELL",
            "RowIndex": 2,
            "ColumnIndex": 3,
            "Relationships": [{"Type": "CHILD", "Ids": ["word-reg-value"]}],
        },
        {
            "Id": "cell-10",
            "BlockType": "CELL",
            "RowIndex": 2,
            "ColumnIndex": 4,
            "Relationships": [{"Type": "CHILD", "Ids": ["word-ot-value"]}],
        },
        {
            "Id": "cell-11",
            "BlockType": "CELL",
            "RowIndex": 2,
            "ColumnIndex": 5,
            "Relationships": [{"Type": "CHILD", "Ids": ["word-rate-value"]}],
        },
        {
            "Id": "cell-12",
            "BlockType": "CELL",
            "RowIndex": 2,
            "ColumnIndex": 6,
            "Relationships": [{"Type": "CHILD", "Ids": ["word-total-value"]}],
        },
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
        {
            "BlockType": "QUERY_RESULT",
            "Query": {"Alias": "VENDOR_NAME"},
            "Text": "Fallback Vendor",
        },
    ]


@mock_aws
def test_bedrock_chain_prefers_bedrock(monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    s3.put_object(Bucket=BUCKET, Key="invoice.pdf", Body=b"pdf")

    bedrock_payload = {
        "vendor": "SERVPRO",
        "labor": [
            {
                "name": "Robbins, Dorian",
                "type": "RS",
                "reg_hours": 40.0,
                "ot_hours": 15.0,
                "rate": 77.0,
                "total": 4812.5,
            }
        ],
        "summaries": {
            "total_regular_hours": 458.75,
            "total_ot_hours": 661.0,
            "total_labor_charges": 77150.25,
            "by_type": {"RS": 7828.5},
        },
    }

    fake_response_body = json.dumps({"completion": json.dumps(bedrock_payload)}).encode()

    with patch("lambda.extraction_lambda._run_textract", return_value=_fake_textract_blocks()), patch(
        "lambda.extraction_lambda.bedrock_client.invoke_model",
        return_value={"body": BytesIO(fake_response_body)},
    ):
        extraction_lambda = _load_extraction_module()
        event = {"bucket": BUCKET, "key": "invoice.pdf"}
        result = extraction_lambda.lambda_handler(event, None)

    assert result["vendor"] == "SERVPRO"
    assert len(result["labor"]) >= 1
    assert result["labor"][0]["total"] == pytest.approx(4812.5)
    assert result["summaries"]["total_labor_charges"] == pytest.approx(77150.25)


@mock_aws
def test_bedrock_failure_falls_back_to_textract(monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    s3.put_object(Bucket=BUCKET, Key="invoice.pdf", Body=b"pdf")

    error = ClientError({"Error": {"Code": "AccessDenied", "Message": "denied"}}, "InvokeModel")

    with patch("lambda.extraction_lambda._run_textract", return_value=_fake_textract_blocks()), patch(
        "lambda.extraction_lambda.bedrock_client.invoke_model",
        side_effect=error,
    ):
        extraction_lambda = _load_extraction_module()
        event = {"bucket": BUCKET, "key": "invoice.pdf"}
        result = extraction_lambda.lambda_handler(event, None)

    assert result["vendor"] == "FALLBACK VENDOR"
    assert len(result["labor"]) == 1
    record = result["labor"][0]
    assert record["hours"] == pytest.approx(55.0)
    assert result["summaries"] == {}


def test_serialize_blocks_collects_lines():
    extraction_lambda = _load_extraction_module()
    blocks = [
        {"BlockType": "PAGE", "Id": "page-1"},
        {"BlockType": "LINE", "Text": "SERVPRO Commercial, LLC", "Page": 1},
        {"BlockType": "WORD", "Text": "Invoice", "Page": 1},
        {"BlockType": "LINE", "Text": "Total $160,356.28", "Page": 1},
        {"BlockType": "LINE", "Text": "Robbins, Dorian RS 40 15 77 4812.5", "Page": 2},
    ]

    serialized = extraction_lambda._serialize_blocks(blocks)
    assert "Page 1:" in serialized
    assert "SERVPRO Commercial, LLC" in serialized
    assert "Total $160,356.28" in serialized
    assert "Page 2:" in serialized


def test_invoke_bedrock_for_extraction_parses_completion(monkeypatch):
    extraction_lambda = _load_extraction_module()

    payload = {
        "completion": json.dumps(
            {
                "vendor": "SERVPRO",
                "labor": [{"name": "Robbins, Dorian", "type": "RS", "reg_hours": 40, "ot_hours": 15, "rate": 77, "total": 4812.5}],
                "summaries": {
                    "total_regular_hours": 458.75,
                    "total_ot_hours": 661.0,
                    "total_labor_charges": 77150.25,
                    "by_type": {"RS": 7828.5},
                },
            }
        )
    }

    with patch("lambda.extraction_lambda.bedrock_client.invoke_model", return_value={"body": BytesIO(json.dumps(payload).encode("utf-8"))}):
        result = extraction_lambda._invoke_bedrock_for_extraction("sample text")

    assert result["vendor"] == "SERVPRO"
    assert result["labor"][0]["total"] == pytest.approx(4812.5)
    assert result["summaries"]["total_regular_hours"] == pytest.approx(458.75)


def test_merge_extractions_prefers_bedrock():
    extraction_lambda = _load_extraction_module()

    textract_labor = [{"name": "Fallback Worker", "type": "RS", "hours": 10, "rate": 20, "total": 200}]
    bedrock_labor = [
        {
            "name": f"Worker {idx}",
            "type": "RS",
            "reg_hours": 40,
            "ot_hours": 15,
            "rate": 77,
            "total": 4812.5,
        }
        for idx in range(44)
    ]
    bedrock_result = {
        "vendor": "SERVPRO",
        "labor": bedrock_labor,
        "summaries": {
            "total_regular_hours": 458.75,
            "total_ot_hours": 661.0,
            "total_labor_charges": 77150.25,
            "by_type": {"RS": 7828.5},
        },
    }

    merged = extraction_lambda._merge_extractions(textract_labor, bedrock_result)
    assert merged["vendor"] == "SERVPRO"
    assert len(merged["labor"]) == 44
    assert merged["summaries"]["total_ot_hours"] == pytest.approx(661.0)


def test_merge_extractions_fallback_to_textract_when_bedrock_empty():
    extraction_lambda = _load_extraction_module()

    textract_labor = [{"name": "Fallback Worker", "type": "RS", "hours": 10, "rate": 20, "total": 200}]
    merged = extraction_lambda._merge_extractions(textract_labor, {})
    assert merged["labor"] == textract_labor
    assert merged["vendor"] is None
    assert merged["summaries"] == {}


@mock_aws
def test_lambda_handler_skips_bedrock_when_text_too_large(monkeypatch):
    extraction_lambda = _load_extraction_module()

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    s3.put_object(Bucket=BUCKET, Key="invoice.pdf", Body=b"pdf")

    with patch("lambda.extraction_lambda._run_textract", return_value=_fake_textract_blocks()), patch(
        "lambda.extraction_lambda._serialize_blocks",
        return_value="A" * (extraction_lambda.SERIALIZED_BLOCKS_MAX_CHARS + 1),
    ), patch("lambda.extraction_lambda._invoke_bedrock_for_extraction") as invoke_mock:
        event = {"bucket": BUCKET, "key": "invoice.pdf"}
        result = extraction_lambda.lambda_handler(event, None)

    invoke_mock.assert_not_called()
    assert result["vendor"] == "FALLBACK VENDOR"
    assert result["summaries"] == {}
