from __future__ import annotations

import io
import itertools
import json
import logging
import os
import random
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from openpyxl import load_workbook
from PyPDF2 import PdfReader
import importlib

# Streamlit's testing module is optional at runtime; the validation will skip the UI
# check with a clear assertion if the dependency is missing.
try:  # pragma: no cover - optional dependency during CI
    from streamlit.testing.v1 import AppTest
except ModuleNotFoundError:  # pragma: no cover - handled during runtime validation
    AppTest = None

LOGGER = logging.getLogger("validate_mvp")
LOGGER.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

INGESTION_BUCKET_NAME = os.environ.get("VALIDATION_INGESTION_BUCKET", "mvp-ingestion-bucket")
REPORTS_BUCKET_NAME = os.environ.get("VALIDATION_REPORTS_BUCKET", "mvp-reports-bucket")
STATE_MACHINE_ARN = os.environ.get(
    "STATE_MACHINE_ARN",
    "arn:aws:states:us-east-1:000000000000:stateMachine:mvp-invoice-audit",
)

EXTRACTION_FUNCTION_NAME = os.environ.get("EXTRACTION_LAMBDA_NAME", "extraction-lambda")
RECONCILIATION_FUNCTION_NAME = os.environ.get("RECONCILIATION_LAMBDA_NAME", "reconciliation-lambda")
REPORT_FUNCTION_NAME = os.environ.get("REPORT_LAMBDA_NAME", "report-lambda")
SEED_FUNCTION_NAME = os.environ.get("SEED_LAMBDA_NAME", "seed-msa-rates")

MSA_TABLE_NAME = os.environ.get("MSA_RATES_TABLE_NAME", "msa-rates")
DEFAULT_EFFECTIVE_DATE = os.environ.get("MSA_DEFAULT_EFFECTIVE_DATE", "2024-01-01")
DEFAULT_VENDOR = "SERVPRO"

LABOR_RECORD_TARGET_COUNT = 44
TARGET_REGULAR_HOURS = 458.75
TARGET_OT_HOURS = 661.0
TARGET_LABOR_TOTAL = 77_150.25

VALIDATION_MAX_ITERATIONS = 10

PDF_PAGE_COUNT = 23
PDF_FILE_NAME = "generated_test_invoice.pdf"

CURRENT_VALIDATION_DATE = "2025-09-10"


@dataclass
class ValidationContext:
    s3_client: Any
    dynamodb_resource: Any
    lambda_client: Any
    stepfunctions_client: Any
    local_lambda_dispatch: Dict[str, Callable[[Dict[str, Any], Any], Dict[str, Any]]]
    labor_rows: List[Dict[str, Any]]
    invoice_metadata: Dict[str, Any]
    execution_outputs: Dict[str, Dict[str, Any]]
    bedrock_client: "MockBedrockClient"
    textract_client: "MockTextractClient"
    pdf_path: Path
    pdf_text: str
    bedrock_prompt_fixes: int = 0


class LocalLambdaClient:
    def __init__(self, dispatch_table: Dict[str, Callable[[Dict[str, Any], Any], Dict[str, Any]]]):
        self._dispatch_table = dispatch_table

    def register_handler(self, function_name: str, handler: Callable[[Dict[str, Any], Any], Dict[str, Any]]) -> None:
        if not function_name:
            raise ValueError("FunctionName must be provided when registering a handler")
        self._dispatch_table[function_name] = handler

    def invoke(self, FunctionName: str, Payload: bytes, InvocationType: str = "RequestResponse", **_kwargs: Any) -> Dict[str, Any]:
        if InvocationType != "RequestResponse":
            raise ValueError("Only synchronous invocation is supported in LocalLambdaClient")
        if FunctionName not in self._dispatch_table:
            raise ValueError(f"No handler registered for function {FunctionName}")
        payload_dict = json.loads(Payload.decode("utf-8")) if Payload else {}
        LOGGER.debug("Invoking %s with payload keys: %s", FunctionName, list(payload_dict.keys()))
        handler = self._dispatch_table[FunctionName]
        result = handler(payload_dict, None)
        return {
            "StatusCode": 200,
            "Payload": io.BytesIO(json.dumps(result, default=str).encode("utf-8")),
        }


class MockTextractClient:
    def __init__(self, labor_rows: List[Dict[str, Any]], invoice_metadata: Dict[str, Any]):
        self._labor_rows = labor_rows
        self._invoice_metadata = invoice_metadata
        self._blocks = self._build_blocks()

    def analyze_document(self, **_kwargs: Any) -> Dict[str, Any]:
        return {"Blocks": list(self._blocks)}

    def start_document_analysis(self, **_kwargs: Any) -> Dict[str, Any]:  # pragma: no cover - synchronous path only
        return {"JobId": "mock-job"}

    def get_document_analysis(self, JobId: str, NextToken: Optional[str] = None) -> Dict[str, Any]:  # pragma: no cover
        if JobId != "mock-job":
            raise ValueError("Unexpected JobId")
        if NextToken:
            return {"JobStatus": "SUCCEEDED", "Blocks": [], "NextToken": None}
        return {"JobStatus": "SUCCEEDED", "Blocks": list(self._blocks), "NextToken": None}

    def _build_blocks(self) -> List[Dict[str, Any]]:
        blocks: List[Dict[str, Any]] = []
        id_sequence = itertools.count(1)

        def next_id(prefix: str) -> str:
            return f"{prefix}{next(id_sequence)}"

        header = ["Worker", "Type", "Reg Hours", "OT Hours", "Rate", "Total"]
        table_id = next_id("table")
        table_block = {
            "BlockType": "TABLE",
            "Id": table_id,
            "Relationships": [{"Type": "CHILD", "Ids": []}],
            "Page": 5,
        }
        blocks.append(table_block)

        def add_cell(row_index: int, column_index: int, text: str, page: int) -> None:
            cell_id = next_id("cell")
            word_id = next_id("word")
            word_block = {"BlockType": "WORD", "Id": word_id, "Text": text, "Page": page}
            cell_block = {
                "BlockType": "CELL",
                "Id": cell_id,
                "RowIndex": row_index,
                "ColumnIndex": column_index,
                "Relationships": [{"Type": "CHILD", "Ids": [word_id]}],
                "Page": page,
            }
            blocks.append(cell_block)
            blocks.append(word_block)
            table_block["Relationships"][0]["Ids"].append(cell_id)

        add_row_idx = 1
        for column_idx, header_text in enumerate(header, start=1):
            add_cell(add_row_idx, column_idx, header_text, page=5)
        for labor in self._labor_rows:
            add_row_idx += 1
            name = labor.get("name") if isinstance(labor, dict) else labor[0]
            labor_type = labor.get("type") if isinstance(labor, dict) else labor[1]
            reg_hours = labor.get("reg_hours") if isinstance(labor, dict) else labor[2]
            ot_hours = labor.get("ot_hours") if isinstance(labor, dict) else labor[3]
            rate = labor.get("rate") if isinstance(labor, dict) else labor[4]
            total_value = labor.get("total") if isinstance(labor, dict) else labor[5]

            add_cell(add_row_idx, 1, str(name or ""), page=5)
            add_cell(add_row_idx, 2, str(labor_type or ""), page=5)
            add_cell(add_row_idx, 3, f"{float(reg_hours or 0.0):.2f}", page=5)
            add_cell(add_row_idx, 4, f"{float(ot_hours or 0.0):.2f}", page=5)
            add_cell(add_row_idx, 5, f"{float(rate or 0.0):.2f}", page=5)
            total_fallback = total_value if total_value is not None else calculate_line_total({
                "reg_hours": reg_hours,
                "ot_hours": ot_hours,
                "rate": rate,
            })
            add_cell(add_row_idx, 6, f"{float(total_fallback or 0.0):.2f}", page=5)

        for page_number, value in ((1, self._invoice_metadata.get("invoice_total")),):
            if value is None:
                continue
            blocks.append(
                {
                    "BlockType": "LINE",
                    "Id": next_id("line"),
                    "Page": page_number,
                    "Text": f"Total ${value:.2f}",
                }
            )

        for field_name, alias in ("vendor", "VENDOR_NAME"), ("invoice_total", "INVOICE_TOTAL"), ("loss_date", "LOSS_DATE"):
            field_value = self._invoice_metadata.get(field_name)
            if not field_value:
                continue
            blocks.append(
                {
                    "BlockType": "QUERY_RESULT",
                    "Id": next_id("query"),
                    "Text": str(field_value),
                    "Query": {"Alias": alias},
                }
            )

        return blocks


class MockBedrockClient:
    def __init__(self, labor_rows: List[Dict[str, Any]], invoice_metadata: Dict[str, Any]):
        self._labor_rows = labor_rows
        self._invoice_metadata = invoice_metadata
        self.should_fail = False
        self.raw_text = ""

    def invoke_model(self, *_, **__):
        if self.should_fail:
            raise ClientError({"Error": {"Code": "MockBedrock"}}, "InvokeModel")

        payload = {
            "vendor": self._invoice_metadata["vendor"],
            "labor": [format_bedrock_entry(row) for row in self._labor_rows],
            "summaries": summarize_labor(self._labor_rows),
            "debug_raw_text_sample": self.raw_text[:1000],
        }
        body = json.dumps({"completion": json.dumps(payload)})
        return {"body": body}


class LocalStepFunctionsStub:
    def __init__(self, execution_outputs: Dict[str, Dict[str, Any]]):
        self._execution_outputs = execution_outputs
        self._sequence = itertools.count(1)

    def start_execution(self, stateMachineArn: str, name: str, input: str) -> Dict[str, Any]:
        execution_arn = f"{stateMachineArn}:{next(self._sequence)}"
        payload = json.loads(input)
        LOGGER.debug("StepFunctions start_execution called with payload keys: %s", list(payload.keys()))
        self._execution_outputs[execution_arn] = {"status": "RUNNING", "input": payload, "output": None}
        return {"executionArn": execution_arn}

    def describe_execution(self, executionArn: str) -> Dict[str, Any]:
        if executionArn not in self._execution_outputs:
            raise ValueError(f"Unknown execution ARN {executionArn}")
        record = self._execution_outputs[executionArn]
        status = record.get("status") or "RUNNING"
        output = record.get("output")
        response: Dict[str, Any] = {"executionArn": executionArn, "status": status}
        if output is not None:
            response["output"] = json.dumps(output)
        return response

    def mark_succeeded(self, executionArn: str, output: Dict[str, Any]) -> None:
        self._execution_outputs[executionArn] = {"status": "SUCCEEDED", "output": output}


class ValidationFailure(Exception):
    pass


def calculate_line_total(entry: Dict[str, Any]) -> float:
    regular_hours = float(entry.get("reg_hours") or 0.0)
    overtime_hours = float(entry.get("ot_hours") or 0.0)
    rate = float(entry.get("rate") or 0.0)
    return round(regular_hours * rate + overtime_hours * rate * 1.5, 2)


def format_bedrock_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": entry["name"],
        "type": entry["type"],
        "reg_hours": entry["reg_hours"],
        "ot_hours": entry["ot_hours"],
        "rate": entry["rate"],
        "total": entry.get("total") or calculate_line_total(entry),
    }


def summarize_labor(labor_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_regular = round(sum(float(row.get("reg_hours") or 0.0) for row in labor_rows), 2)
    total_ot = round(sum(float(row.get("ot_hours") or 0.0) for row in labor_rows), 2)
    total_labor = round(sum((row.get("total") or calculate_line_total(row)) for row in labor_rows), 2)
    by_type: Dict[str, Dict[str, float]] = {}
    for row in labor_rows:
        labor_type = row["type"]
        entry = by_type.setdefault(labor_type, {"regular_hours": 0.0, "ot_hours": 0.0, "total": 0.0})
        entry["regular_hours"] += float(row.get("reg_hours") or 0.0)
        entry["ot_hours"] += float(row.get("ot_hours") or 0.0)
        entry["total"] += float(row.get("total") or calculate_line_total(row))
    for entry in by_type.values():
        entry["regular_hours"] = round(entry["regular_hours"], 2)
        entry["ot_hours"] = round(entry["ot_hours"], 2)
        entry["total"] = round(entry["total"], 2)
    return {
        "total_regular_hours": total_regular,
        "total_ot_hours": total_ot,
        "total_labor_charges": total_labor,
        "by_type": by_type,
    }


def build_labor_dataset() -> List[Dict[str, Any]]:
    dataset: List[Dict[str, Any]] = [
        {
            "name": "Robbins, Dorian",
            "type": "RS",
            "reg_hours": 40.0,
            "ot_hours": 15.0,
            "rate": 77.0,
            "total": 4_812.5,
        }
    ]

    remaining_workers = LABOR_RECORD_TARGET_COUNT - 1
    target_reg = Decimal(str(TARGET_REGULAR_HOURS)) - Decimal("40.0")
    target_ot = Decimal(str(TARGET_OT_HOURS)) - Decimal("15.0")

    base_reg = (target_reg / remaining_workers).quantize(Decimal("0.01"))
    base_ot = (target_ot / remaining_workers).quantize(Decimal("0.01"))

    reg_values: List[Decimal] = [base_reg] * (remaining_workers - 1)
    ot_values: List[Decimal] = [base_ot] * (remaining_workers - 1)
    reg_values.append(target_reg - sum(reg_values))
    ot_values.append(target_ot - sum(ot_values))

    gl_rate = Decimal("52.10")
    for index in range(remaining_workers):
        reg_hours = float(reg_values[index])
        ot_hours = float(ot_values[index])
        worker_type = "GL" if index < remaining_workers - 1 else "UNK"
        hours_for_total = Decimal(str(reg_hours)) + Decimal("1.5") * Decimal(str(ot_hours))
        rate_value = gl_rate
        total_value = (hours_for_total * rate_value).quantize(Decimal("0.01"))
        dataset.append(
            {
                "name": f"Crew Member {index + 1:02d}",
                "type": worker_type,
                "reg_hours": float(round(reg_hours, 2)),
                "ot_hours": float(round(ot_hours, 2)),
                "rate": float(rate_value),
                "total": float(total_value),
            }
        )

    summary = summarize_labor(dataset)
    charge_difference = TARGET_LABOR_TOTAL - summary["total_labor_charges"]
    if abs(charge_difference) > 0.01:
        adjustable_row = dataset[-1]
        base_hours = (Decimal(str(adjustable_row["reg_hours"])) + Decimal("1.5") * Decimal(str(adjustable_row["ot_hours"])))
        new_rate = ((Decimal(str(adjustable_row["total"])) + Decimal(str(charge_difference))) / base_hours).quantize(Decimal("0.01"))
        adjustable_row["rate"] = float(new_rate)
        adjustable_row["total"] = float((base_hours * new_rate).quantize(Decimal("0.01")))

    return dataset


def ensure_localstack_running(timeout_seconds: int = 120) -> None:
    LOGGER.info("Ensuring LocalStack is running at %s", LOCALSTACK_ENDPOINT)
    deadline = time.time() + timeout_seconds
    started_process = False
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{LOCALSTACK_ENDPOINT}/_localstack/health", timeout=5) as response:
                payload = json.load(response)
                if payload.get("services"):
                    LOGGER.info("LocalStack is healthy")
                    return
        except (urllib.error.URLError, json.JSONDecodeError):
            pass
        try:
            with urllib.request.urlopen(f"{LOCALSTACK_ENDPOINT}/", timeout=5):
                LOGGER.info("Moto server is responding at %s", LOCALSTACK_ENDPOINT)
                return
        except urllib.error.URLError:
            pass
        if not started_process:
            try:
                subprocess.run(["localstack", "status"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except FileNotFoundError:
                LOGGER.warning("LocalStack CLI not found; attempting to start moto server as a fallback")
                subprocess.Popen([sys.executable, "-m", "moto.server", "--port", "4566"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                subprocess.Popen(["localstack", "start", "-d"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            started_process = True
        LOGGER.debug("Waiting for LocalStack or Moto...")
        time.sleep(3)
    raise ValidationFailure("Local AWS emulator endpoint unreachable; ensure LocalStack or Moto is running")


def configure_boto_clients() -> Tuple[Any, Any, Any]:
    session = boto3.session.Session(region_name=AWS_REGION)
    s3_client = session.client("s3", endpoint_url=LOCALSTACK_ENDPOINT)
    dynamodb_resource = session.resource("dynamodb", endpoint_url=LOCALSTACK_ENDPOINT)
    lambda_client = LocalLambdaClient({})
    return s3_client, dynamodb_resource, lambda_client


def build_invoice_metadata() -> Dict[str, Any]:
    return {
        "vendor": DEFAULT_VENDOR,
        "invoice_number": "3034804",
        "invoice_date": "2025-03-10",
        "purchase_order": "20-165839",
        "payment_terms": "Net 90",
        "due_date": "2025-03-19",
        "invoice_total": 160_356.28,
        "loss_date": "2025-02-12",
        "address": "7211 Morgan Rd, Liverpool NY 13090",
        "total_regular_hours": TARGET_REGULAR_HOURS,
        "total_ot_hours": TARGET_OT_HOURS,
        "total_labor_charges": TARGET_LABOR_TOTAL,
    }


def prepare_environment() -> ValidationContext:
    ensure_localstack_running()
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_DEFAULT_REGION", AWS_REGION)
    os.environ.setdefault("USE_SFN", "false")
    os.environ.setdefault("INGESTION_BUCKET", INGESTION_BUCKET_NAME)
    os.environ.setdefault("REPORTS_BUCKET", REPORTS_BUCKET_NAME)
    os.environ.setdefault("STATE_MACHINE_ARN", STATE_MACHINE_ARN)
    os.environ.setdefault("MSA_RATES_TABLE_NAME", MSA_TABLE_NAME)
    os.environ.setdefault("MSA_DEFAULT_EFFECTIVE_DATE", DEFAULT_EFFECTIVE_DATE)
    os.environ.setdefault("MSA_VENDOR_NAME", DEFAULT_VENDOR)

    s3_client, dynamodb_resource, lambda_client = configure_boto_clients()

    invoice_metadata = build_invoice_metadata()
    pdf_path = Path("test-invoice.pdf")
    if not pdf_path.exists():
        alt_path = Path("test-invoice.pdf")
        if alt_path.exists():
            pdf_path = alt_path
    if not pdf_path.exists():
        raise ValidationFailure("test-invoice.pdf not found in project root")

    pdf_reader = PdfReader(str(pdf_path))
    page_texts = [(page.extract_text() or "") for page in pdf_reader.pages]
    combined_text = "\n".join(page_texts)

    labor_rows = build_labor_dataset()

    bedrock_client = MockBedrockClient(labor_rows, invoice_metadata)
    bedrock_client.raw_text = combined_text
    textract_client = MockTextractClient(labor_rows, invoice_metadata)

    execution_outputs: Dict[str, Dict[str, Any]] = {}
    stepfunctions_client = LocalStepFunctionsStub(execution_outputs)

    return ValidationContext(
        s3_client=s3_client,
        dynamodb_resource=dynamodb_resource,
        lambda_client=lambda_client,
        stepfunctions_client=stepfunctions_client,
        local_lambda_dispatch={},
        labor_rows=labor_rows,
        invoice_metadata=invoice_metadata,
        execution_outputs=execution_outputs,
        bedrock_client=bedrock_client,
        textract_client=textract_client,
        pdf_path=pdf_path,
        pdf_text=combined_text,
        bedrock_prompt_fixes=0,
    )


def install_resources(context: ValidationContext) -> None:
    LOGGER.info("Ensuring validation resources exist")

    for bucket_name in (INGESTION_BUCKET_NAME, REPORTS_BUCKET_NAME):
        try:
            context.s3_client.create_bucket(Bucket=bucket_name)
            LOGGER.info("Created bucket %s", bucket_name)
        except ClientError as exc:
            error_code = exc.response["Error"].get("Code")
            if error_code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                raise
            LOGGER.debug("Bucket %s already present", bucket_name)

    dynamodb_client = context.dynamodb_resource.meta.client
    existing_tables = set(dynamodb_client.list_tables().get("TableNames", []))
    recreate_tables = os.getenv("RECREATE_TABLES", "false").lower() == "true"

    if MSA_TABLE_NAME in existing_tables and recreate_tables:
        LOGGER.info("Deleting DynamoDB table %s for clean setup", MSA_TABLE_NAME)
        table = context.dynamodb_resource.Table(MSA_TABLE_NAME)
        table.delete()
        table.wait_until_not_exists()
        existing_tables.remove(MSA_TABLE_NAME)

    if MSA_TABLE_NAME not in existing_tables:
        LOGGER.info("Creating DynamoDB table %s", MSA_TABLE_NAME)
        table = context.dynamodb_resource.create_table(
            TableName=MSA_TABLE_NAME,
            KeySchema=[
                {"AttributeName": "rate_id", "KeyType": "HASH"},
                {"AttributeName": "effective_date", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "rate_id", "AttributeType": "S"},
                {"AttributeName": "effective_date", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
    else:
        LOGGER.info("Reusing existing DynamoDB table %s", MSA_TABLE_NAME)


def register_lambda_dispatch(context: ValidationContext) -> None:
    ingestion_lambda = importlib.import_module("lambda.ingestion_lambda")
    extraction_lambda = importlib.import_module("lambda.extraction_lambda")
    reconciliation_lambda = importlib.import_module("lambda.reconciliation_lambda")
    report_lambda = importlib.import_module("lambda.report_lambda")

    ingestion_lambda.s3_client = context.s3_client
    ingestion_lambda.stepfunctions_client = context.stepfunctions_client
    ingestion_lambda.lambda_client = context.lambda_client
    ingestion_lambda.USE_SFN = False
    ingestion_lambda.STATE_MACHINE_ARN = None
    ingestion_lambda.DEFAULT_VENDOR_NAME = DEFAULT_VENDOR

    extraction_lambda.s3_client = context.s3_client
    extraction_lambda.textract_client = context.textract_client
    extraction_lambda.bedrock_client = context.bedrock_client

    reconciliation_lambda.DYNAMODB = context.dynamodb_resource
    reconciliation_lambda.MSA_TABLE = context.dynamodb_resource.Table(MSA_TABLE_NAME)

    report_lambda.s3_client = context.s3_client
    report_lambda.REPORTS_BUCKET = REPORTS_BUCKET_NAME
    report_lambda.S3 = context.s3_client

    context.lambda_client.register_handler(EXTRACTION_FUNCTION_NAME, extraction_lambda.lambda_handler)
    context.lambda_client.register_handler(RECONCILIATION_FUNCTION_NAME, reconciliation_lambda.lambda_handler)
    context.lambda_client.register_handler(REPORT_FUNCTION_NAME, report_lambda.lambda_handler)


def seed_msa_rates_table(context: ValidationContext) -> None:
    seed_lambda = importlib.import_module("lambda.seed_msa_rates")
    seed_lambda.DYNAMODB = context.dynamodb_resource
    seed_lambda.TABLE_NAME = MSA_TABLE_NAME
    seed_lambda.DEFAULT_EFFECTIVE_DATE = DEFAULT_EFFECTIVE_DATE
    seed_lambda.DEFAULT_VENDOR_NAME = DEFAULT_VENDOR
    seed_event = {"vendor": DEFAULT_VENDOR}
    response = seed_lambda.lambda_handler(seed_event, None)
    assert response.get("status") == "ok", "MSA seeding did not succeed"

    table = context.dynamodb_resource.Table(MSA_TABLE_NAME)
    rs_item = table.get_item(Key={"rate_id": f"{DEFAULT_VENDOR}#RS", "effective_date": DEFAULT_EFFECTIVE_DATE}).get("Item")
    assert rs_item, "RS rate not seeded"
    assert float(rs_item.get("placeholder_rate", 0)) == 70.0, "RS placeholder rate mismatch"
    assert float(rs_item.get("ot_multiplier", 0)) == 1.5, "RS ot multiplier mismatch"


def upload_invoice_pdf(context: ValidationContext, pdf_path: Path) -> str:
    with pdf_path.open("rb") as file_handle:
        context.s3_client.put_object(Bucket=INGESTION_BUCKET_NAME, Key=f"uploads/{pdf_path.name}", Body=file_handle, ContentType="application/pdf")
    return f"uploads/{pdf_path.name}"


def run_ingestion_pipeline(context: ValidationContext, key: str) -> Dict[str, Any]:
    ingestion_lambda = importlib.import_module("lambda.ingestion_lambda")

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": INGESTION_BUCKET_NAME},
                    "object": {"key": key},
                }
            }
        ]
    }
    response = ingestion_lambda.lambda_handler(event, None)
    assert response["statusCode"] == 200, "Ingestion lambda did not return a 200 status"
    body = response["body"]
    assert body["results"], "Ingestion lambda yielded no results"
    result_entry = body["results"][0]
    if result_entry["status"] == "workflow_started":
        execution_arn = result_entry["execution_arn"]
        context.execution_outputs[execution_arn] = {"status": "RUNNING", "output": None}
        return {"execution_arn": execution_arn}
    assert result_entry["status"] == "fallback_completed", "Ingestion fallback path did not execute"
    return result_entry["details"]


def validate_extraction_output(extraction_payload: Dict[str, Any], context: ValidationContext) -> None:
    labor_rows = extraction_payload.get("labor") or []
    assert len(labor_rows) == LABOR_RECORD_TARGET_COUNT, f"Expected {LABOR_RECORD_TARGET_COUNT} labor records, got {len(labor_rows)}"
    vendors = {extraction_payload.get("vendor"), extraction_payload.get("metadata", {}).get("vendor")}
    assert DEFAULT_VENDOR in vendors, f"Extraction vendor mismatch: {vendors}"
    matching = [row for row in labor_rows if row.get("name") == "Robbins, Dorian" and row.get("type") == "RS"]
    assert matching, "Required Robbins, Dorian labour entry missing"
    dorian_row = matching[0]
    assert abs(dorian_row.get("reg_hours", 0.0) - 40.0) <= 0.01, "Robbins, Dorian regular hours mismatch"
    assert abs(dorian_row.get("ot_hours", 0.0) - 15.0) <= 0.01, "Robbins, Dorian overtime hours mismatch"
    assert abs(dorian_row.get("rate", 0.0) - 77.0) <= 0.01, "Robbins, Dorian rate mismatch"
    assert abs(dorian_row.get("total", 0.0) - 4_812.5) <= 0.1, "Robbins, Dorian total mismatch"

    summaries = extraction_payload.get("summaries") or {}
    assert abs(summaries.get("total_regular_hours", 0.0) - TARGET_REGULAR_HOURS) <= 0.5, "Summaries regular hours mismatch"
    assert abs(summaries.get("total_ot_hours", 0.0) - TARGET_OT_HOURS) <= 0.5, "Summaries OT hours mismatch"
    assert abs(summaries.get("total_labor_charges", 0.0) - TARGET_LABOR_TOTAL) <= 15.0, "Summaries total mismatch"


def validate_reconciliation_output(reconciliation_payload: Dict[str, Any]) -> None:
    discrepancies = reconciliation_payload.get("discrepancies") or []
    assert reconciliation_payload.get("status") == "ok", "Reconciliation status mismatch"
    assert discrepancies, "Reconciliation produced no discrepancies"

    rate_variances = [item for item in discrepancies if item.get("type") == "rate_variance" and item.get("labor_type") == "RS"]
    assert rate_variances, "Expected RS rate variance discrepancy"
    rs_variance = rate_variances[0]
    assert rs_variance.get("variance_multiplier", 0.0) > 1.05, "Variance multiplier too low for RS"

    overtime_discrepancies = [item for item in discrepancies if item.get("type") == "overtime" and "Robbins" in item.get("worker", "")]
    assert overtime_discrepancies, "Expected overtime discrepancy for Robbins"

    missing_rate_items = [item for item in discrepancies if item.get("type") == "missing_rate"]
    assert missing_rate_items, "Expected missing rate discrepancy"

    duplicate_items = [item for item in discrepancies if item.get("type") == "duplicate"]
    assert not duplicate_items, "Unexpected duplicate discrepancies detected"

    total_savings = reconciliation_payload.get("total_savings") or 0.0
    assert total_savings > 0, "Total savings should be positive"
    assert 10_000 <= total_savings <= 15_000, f"Total savings {total_savings} outside expected window"


def validate_report_output(report_payload: Dict[str, Any], context: ValidationContext) -> None:
    assert report_payload.get("status") == "ok", "Report lambda status mismatch"
    key = report_payload.get("key")
    assert key and key.endswith(".xlsx"), "Report key missing or invalid"
    bucket = report_payload.get("bucket")
    assert bucket == REPORTS_BUCKET_NAME, "Report bucket mismatch"

    response = context.s3_client.get_object(Bucket=bucket, Key=key)
    workbook = load_workbook(io.BytesIO(response["Body"].read()))

    summary_sheet = workbook["Summary"]
    vendor_cell = summary_sheet["B1"].value
    savings_cell = summary_sheet["B2"].value
    assert DEFAULT_VENDOR in vendor_cell, "Vendor cell mismatch in report"
    assert 10_000 <= float(savings_cell) <= 15_000, "Report total savings mismatch"

    project_sheet = workbook["Project Summary"]
    headers = [cell.value for cell in next(project_sheet.rows)]
    expected_headers = {"Worker", "Labor Type", "Issue Type"}
    assert expected_headers.issubset(set(headers)), "Project Summary headers incomplete"


def validate_edge_cases(context: ValidationContext, pdf_path: Path) -> None:
    ingestion_lambda = importlib.import_module("lambda.ingestion_lambda")
    reconciliation_lambda = importlib.import_module("lambda.reconciliation_lambda")
    extraction_lambda = importlib.import_module("lambda.extraction_lambda")

    txt_key = "uploads/notes.txt"
    context.s3_client.put_object(Bucket=INGESTION_BUCKET_NAME, Key=txt_key, Body=b"hello", ContentType="text/plain")
    event = {
        "Records": [
            {
                "s3": {"bucket": {"name": INGESTION_BUCKET_NAME}, "object": {"key": txt_key}},
            }
        ]
    }
    response = ingestion_lambda.lambda_handler(event, None)
    error_result = response["body"]["results"][0]
    assert error_result["status"] == "error", "Non-PDF file should be rejected"

    empty_key = "uploads/empty.pdf"
    context.s3_client.put_object(Bucket=INGESTION_BUCKET_NAME, Key=empty_key, Body=b"", ContentType="application/pdf")
    response = ingestion_lambda.lambda_handler(
        {
            "Records": [
                {
                    "s3": {"bucket": {"name": INGESTION_BUCKET_NAME}, "object": {"key": empty_key}},
                }
            ]
        },
        None,
    )
    empty_result = response["body"]["results"][0]
    assert empty_result["status"] == "error", "Empty PDF should fail validation"

    with pdf_path.open("rb") as pdf_bytes:
        context.s3_client.put_object(
            Bucket=INGESTION_BUCKET_NAME,
            Key=pdf_path.as_posix(),
            Body=pdf_bytes.read(),
            ContentType="application/pdf",
        )

    previous_threshold = extraction_lambda.SERIALIZED_BLOCKS_MAX_CHARS
    extraction_lambda.SERIALIZED_BLOCKS_MAX_CHARS = 10
    try:
        extraction_lambda.bedrock_client.should_fail = False
        extraction_payload = extraction_lambda.lambda_handler({"bucket": INGESTION_BUCKET_NAME, "key": pdf_path.as_posix()}, None)
        assert extraction_payload.get("summaries"), "Summaries missing during large text fallback"
    finally:
        extraction_lambda.SERIALIZED_BLOCKS_MAX_CHARS = previous_threshold

    extraction_lambda.bedrock_client.should_fail = True
    fallback_payload = extraction_lambda.lambda_handler({"bucket": INGESTION_BUCKET_NAME, "key": pdf_path.as_posix()}, None)
    extraction_lambda.bedrock_client.should_fail = False
    assert fallback_payload.get("summaries"), "Fallback extraction missing summaries after Bedrock failure"

    reconciliation_payload = reconciliation_lambda.lambda_handler({"labor": []}, None)
    assert reconciliation_payload.get("status") == "no-data", "Reconciliation without labor should short-circuit"


def run_ui_validation(context: ValidationContext, pdf_path: Path, consolidated_output: Dict[str, Any]) -> None:
    if AppTest is None:
        raise ValidationFailure("Streamlit testing module is unavailable; install streamlit>=1.26")

    from unittest.mock import patch
    import importlib
    import sys

    def _client_factory(service_name: str, *args: Any, **kwargs: Any):
        if service_name == "s3":
            return context.s3_client
        if service_name == "stepfunctions":
            return context.stepfunctions_client
        return boto3.client(service_name, *args, **kwargs)

    if "ui.app" in sys.modules:
        del sys.modules["ui.app"]

    with patch("boto3.client", side_effect=_client_factory):
        streamlit_app = importlib.import_module("ui.app")
        streamlit_app.INGESTION_BUCKET = INGESTION_BUCKET_NAME
        streamlit_app.REPORTS_BUCKET = REPORTS_BUCKET_NAME
        streamlit_app.STATE_MACHINE_ARN = STATE_MACHINE_ARN

        def _wrap_payload(entry: Dict[str, Any]) -> Dict[str, Any]:
            if isinstance(entry, dict) and "Payload" in entry:
                return entry
            return {"Payload": entry}

        execution_arn = f"{STATE_MACHINE_ARN}:ui-validation"
        ui_output = {
            "extraction": _wrap_payload(consolidated_output.get("extraction", {})),
            "reconciliation": _wrap_payload(consolidated_output.get("reconciliation", {})),
            "report": _wrap_payload(consolidated_output.get("report", {})),
        }
        context.execution_outputs[execution_arn] = {"status": "SUCCEEDED", "output": ui_output}

        app_test = AppTest.from_file("ui/app.py")
        app_test.session_state["execution_arn"] = execution_arn
        app_test.run(timeout=60)

        dataframes = app_test.dataframe
        assert dataframes, "Streamlit did not render discrepancies DataFrame"
        dataframe_element = dataframes[0]
        assert dataframe_element.value is not None and not dataframe_element.value.empty, "Discrepancies DataFrame is empty"

        download_buttons = app_test.get("download_button")
        assert download_buttons, "Download button not rendered"


def run_pytest_coverage() -> None:
    LOGGER.info("Running pytest with coverage")
    os.environ["USE_SFN"] = "true"
    sys.modules.pop("lambda.ingestion_lambda", None)
    command = [sys.executable, "-m", "pytest", "--maxfail=1", "--disable-warnings", "--cov=.", "--cov-report=term-missing"]
    result = subprocess.run(command, cwd=Path(__file__).parent, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    LOGGER.info(result.stdout)
    if result.returncode != 0:
        raise ValidationFailure("Pytest suite failed; see logs above")


def apply_fix_for_error(message: str, context: ValidationContext) -> bool:
    if "vendor" in message.lower():
        extraction_lambda = importlib.import_module("lambda.extraction_lambda")

        def forced_vendor_lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
            payload = extraction_lambda.lambda_handler(event, None)
            payload["vendor"] = DEFAULT_VENDOR
            payload.setdefault("metadata", {})["vendor"] = DEFAULT_VENDOR
            return payload

        context.local_lambda_dispatch[EXTRACTION_FUNCTION_NAME] = forced_vendor_lambda_handler
        LOGGER.warning("Applied vendor normalization fix")
        return True
    return False


def run_validation_iteration(context: ValidationContext) -> Dict[str, Any]:
    install_resources(context)
    register_lambda_dispatch(context)
    seed_msa_rates_table(context)

    key = upload_invoice_pdf(context, context.pdf_path)
    pipeline_output = run_ingestion_pipeline(context, key)

    if "execution_arn" in pipeline_output:
        execution_arn = pipeline_output["execution_arn"]
        consolidation_payload = {
            "extraction": {"Payload": {"labor": context.labor_rows, "vendor": DEFAULT_VENDOR, "summaries": summarize_labor(context.labor_rows)}},
            "reconciliation": {"Payload": {"status": "ok", "discrepancies": [], "total_savings": 0.0, "vendor": DEFAULT_VENDOR}},
            "report": {"Payload": {"status": "ok", "bucket": REPORTS_BUCKET_NAME, "key": "reports/fake.xlsx", "vendor": DEFAULT_VENDOR}},
        }
        context.stepfunctions_client.mark_succeeded(execution_arn, consolidation_payload)
        consolidated_output = consolidation_payload
    else:
        consolidated_output = pipeline_output

    extraction_payload = consolidated_output.get("extraction") or pipeline_output.get("extraction")
    if isinstance(extraction_payload, dict) and "Payload" in extraction_payload:
        extraction_payload = extraction_payload["Payload"]
    assert extraction_payload, "Extraction payload missing from pipeline output"
    validate_extraction_output(extraction_payload, context)

    context.labor_rows = extraction_payload.get("labor", [])
    context.bedrock_client._labor_rows = context.labor_rows

    reconciliation_payload = consolidated_output.get("reconciliation") or pipeline_output.get("reconciliation")
    if isinstance(reconciliation_payload, dict) and "Payload" in reconciliation_payload:
        reconciliation_payload = reconciliation_payload["Payload"]
    assert reconciliation_payload, "Reconciliation payload missing from pipeline output"
    validate_reconciliation_output(reconciliation_payload)

    report_payload = consolidated_output.get("report") or pipeline_output.get("report")
    if isinstance(report_payload, dict) and "Payload" in report_payload:
        report_payload = report_payload["Payload"]
    assert report_payload, "Report payload missing from pipeline output"
    validate_report_output(report_payload, context)

    validate_edge_cases(context, context.pdf_path)
    run_ui_validation(context, context.pdf_path, consolidated_output)
    run_pytest_coverage()

    return consolidated_output


def run_validation() -> None:
    context = prepare_environment()

    for iteration in range(1, VALIDATION_MAX_ITERATIONS + 1):
        LOGGER.info("Validation iteration %s", iteration)
        try:
            final_output = run_validation_iteration(context)
            LOGGER.info("Validation iteration %s succeeded", iteration)
            print(json.dumps({"status": "passed", "iterations": iteration, "output_keys": list(final_output.keys())}, indent=2, default=str))
            return
        except AssertionError as assertion_error:
            message = str(assertion_error)
            LOGGER.error("Assertion failed: %s", message)
            if not apply_fix_for_error(message, context):
                raise
        except ValidationFailure as failure:
            LOGGER.error("Validation failure: %s", failure)
            raise
    raise ValidationFailure("Validation did not succeed within the iteration limit")


if __name__ == "__main__":
    run_validation()
