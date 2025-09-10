"""Minimal labour-focused Textract extractor."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Dict, Iterable, List, Tuple

import boto3
import pandas as pd
from botocore.exceptions import ClientError

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv("LOG_LEVEL", "INFO"))

BEDROCK_REGION = os.getenv("BEDROCK_REGION", "us-east-1")

s3_client = boto3.client("s3")
textract_client = boto3.client("textract")
bedrock_client = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

# NOTE: The extraction Lambda execution role must include permission to invoke the
# Bedrock model used for downstream requests. Update `infrastructure/full_stack.py`
# to attach a policy such as `iam.PolicyStatement(actions=["bedrock:InvokeModel"], resources=["*"])`
# to the extraction Lambda role before deploying.

ASYNC_THRESHOLD_BYTES = 500 * 1024
LABOUR_KEYWORDS = {"labour", "labor", "worker", "type", "hours", "rate", "reg", "ot"}
COLUMN_ALIASES: Dict[str, Iterable[str]] = {
    "name": ("name", "worker", "employee", "person"),
    "type": ("type", "classification", "class", "role"),
    "hours": ("hours", "hrs", "quantity", "qty"),
    "reg_hours": ("reg", "regular", "regular hours"),
    "ot_hours": ("ot", "overtime", "overtime hours"),
    "rate": ("rate", "hourly", "unit price", "cost"),
    "total": ("total", "amount", "line total"),
}

TEXTRACT_FEATURE_TYPES = ["TABLES", "FORMS", "QUERIES"]
TEXTRACT_QUERIES = [
    {"Text": "What is the vendor name?", "Alias": "VENDOR_NAME"},
    {"Text": "What is the invoice total?", "Alias": "INVOICE_TOTAL"},
    {"Text": "What is the date of loss?", "Alias": "LOSS_DATE"},
]
QUERY_ALIAS_MAPPING = {
    "VENDOR_NAME": "vendor",
    "INVOICE_TOTAL": "invoice_total",
    "LOSS_DATE": "loss_date",
}

SERIALIZED_BLOCKS_MAX_CHARS = 200_000


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    bucket, key = _object_location(event)
    metadata = _head_object(bucket, key)
    blocks = _run_textract(bucket, key, metadata["ContentLength"])
    tables = _tables_from_blocks(blocks)
    labour_rows = _labour_entries(tables)
    metadata_from_queries = _parse_query_results(blocks)
    raw_text = _serialize_blocks(blocks)

    bedrock_result: Dict[str, Any] = {}
    if raw_text and len(raw_text) > SERIALIZED_BLOCKS_MAX_CHARS:
        LOGGER.warning(
            "Raw text length %s exceeds Bedrock limit, falling back to Textract results only",
            len(raw_text),
        )
    elif raw_text:
        # Bedrock Claude invocation: ~$0.003 per 1K input tokens (at $3/million),
        # plus $0.015/1K output; estimate for 23-page PDF: <$0.01
        bedrock_result = _invoke_bedrock_for_extraction(raw_text)

    merged_extraction = _merge_extractions(labour_rows, bedrock_result)
    labour_rows = merged_extraction.get("labor", labour_rows)
    labour_rows = _normalize_labor_rows(labour_rows)
    vendor_name = merged_extraction.get("vendor") or metadata_from_queries.get("vendor")
    if vendor_name:
        metadata_from_queries["vendor"] = vendor_name
    summaries = merged_extraction.get("summaries") or {}
    if (not summaries or not summaries.get("total_regular_hours")) and labour_rows:
        summaries = _summaries_from_labor(labour_rows)

    response: Dict[str, Any] = {
        "status": "ok",
        "bucket": bucket,
        "key": key,
        "record_count": len(labour_rows),
        "labor": labour_rows,
        "metadata": metadata_from_queries,
    }
    response["vendor"] = vendor_name or "Unknown"
    response["summaries"] = summaries
    return response


def _run_textract(bucket: str, key: str, size_bytes: int) -> List[Dict[str, Any]]:
    if size_bytes > ASYNC_THRESHOLD_BYTES:
        job = textract_client.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
            FeatureTypes=TEXTRACT_FEATURE_TYPES,
            QueriesConfig={"Queries": TEXTRACT_QUERIES},
        )
        return _poll_textract(job["JobId"])

    response = textract_client.analyze_document(
        Document={"S3Object": {"Bucket": bucket, "Name": key}},
        FeatureTypes=TEXTRACT_FEATURE_TYPES,
        QueriesConfig={"Queries": TEXTRACT_QUERIES},
    )
    return response.get("Blocks", [])


def _serialize_blocks(blocks: List[Dict[str, Any]]) -> str:
    page_tables: Dict[int, List[List[str]]] = {}
    page_lines: Dict[int, List[str]] = {}

    id_to_block = {block.get("Id"): block for block in blocks if block.get("Id")}

    for block in blocks:
        block_type = block.get("BlockType")
        if block_type == "TABLE":
            page_number = int(block.get("Page", 1))
            rows: Dict[int, Dict[int, str]] = {}
            for relationship in block.get("Relationships", []):
                if relationship.get("Type") != "CHILD":
                    continue
                for cell_id in relationship.get("Ids", []):
                    cell = id_to_block.get(cell_id)
                    if not cell or cell.get("BlockType") != "CELL":
                        continue
                    row_idx = int(cell.get("RowIndex", 1))
                    col_idx = int(cell.get("ColumnIndex", 1))
                    text_parts: List[str] = []
                    for rel in cell.get("Relationships", []):
                        if rel.get("Type") != "CHILD":
                            continue
                        for word_id in rel.get("Ids", []):
                            word_block = id_to_block.get(word_id)
                            if word_block and word_block.get("BlockType") == "WORD":
                                text_parts.append(word_block.get("Text", ""))
                    rows.setdefault(row_idx, {})[col_idx] = " ".join(text_parts).strip()
            ordered_rows: List[List[str]] = []
            for row_idx in sorted(rows.keys()):
                row_data = rows[row_idx]
                row_values = [row_data.get(col_idx, "") for col_idx in sorted(row_data.keys())]
                ordered_rows.append(row_values)
            if ordered_rows:
                page_tables.setdefault(page_number, []).append(ordered_rows)
        elif block_type == "LINE":
            page_number = int(block.get("Page", 1))
            text_value = (block.get("Text") or "").strip()
            if text_value:
                page_lines.setdefault(page_number, []).append(text_value)

    serialized_pages: List[str] = []
    page_numbers = sorted({*page_lines.keys(), *page_tables.keys()})

    for page_number in page_numbers:
        sections: List[str] = [f"Page {page_number}:"]
        tables = page_tables.get(page_number, [])
        if tables:
            for table in tables:
                if table:
                    column_count = max(len(row) for row in table)
                    headers = table[0] if table else []
                    if not headers or any(header.strip() == "" for header in headers):
                        headers = [f"Column {idx}" for idx in range(1, column_count + 1)]
                        body_rows = table
                    else:
                        body_rows = table[1:]
                    header_line = " | ".join(header.strip() for header in headers)
                    separator_line = " | ".join("---" for _ in headers)
                    sections.append(f"| {header_line} |")
                    sections.append(f"| {separator_line} |")
                    for row in body_rows:
                        padded_row = list(row) + [""] * (len(headers) - len(row))
                        row_line = " | ".join(cell.strip() for cell in padded_row)
                        sections.append(f"| {row_line} |")
        if page_lines.get(page_number):
            sections.extend(page_lines[page_number])
        serialized_pages.append("\n".join(sections))

    return "\n\n".join(serialized_pages)


def _invoke_bedrock_for_extraction(raw_text: str) -> Dict[str, Any]:
    if not raw_text:
        LOGGER.warning("Bedrock invocation skipped because raw_text was empty")
        return {}

    model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"
    payload = {
        "prompt": (
            "You are an expert invoice parser. Extract structured JSON from the following OCR text."
            " The invoice includes multi-day labor tables where a worker row lists daily hours for a week"
            " (e.g., '2/13 Thu: 0.00', '2/14 Fri: 0.00', '2/15 Sat: 16.00'). Split hours into"
            " regular (first 40 hours) and overtime (hours beyond 40) totals per worker."
            " Normalize OCR artifacts ('ooc'→0.00, 'OOC'→0.00, '5C'→5.0, 'SC'→5.0)."
            " Vendor lines like 'Servpro Commercial, LLC' should yield vendor 'SERVPRO'."
            " Rates appear on later pages; use the numeric rate column in the labor table."
            " Output JSON with keys: vendor (str), labor (list of objects with name, type, reg_hours,"
            " ot_hours, rate, total), summaries (total_regular_hours, total_ot_hours, total_labor_charges)."
            f"\n\nOCR Source:\n{raw_text}\n"
        ),
        "max_tokens": 4096,
        "temperature": 0.0,
        "anthropic_version": "bedrock-2023-05-31",
    }

    try:
        LOGGER.info("Invoking Bedrock model %s for labor extraction", model_id)
        response = bedrock_client.invoke_model(modelId=model_id, body=json.dumps(payload))
    except ClientError as exc:
        LOGGER.error("Bedrock invocation failed: %s", exc, exc_info=True)
        return {}

    try:
        response_body = response.get("body")
        if hasattr(response_body, "read"):
            raw_response = response_body.read()
        else:
            raw_response = response_body
        if isinstance(raw_response, bytes):
            raw_response = raw_response.decode("utf-8")
        if isinstance(raw_response, str):
            parsed_body = json.loads(raw_response)
        else:
            parsed_body = raw_response or {}
    except json.JSONDecodeError as exc:
        LOGGER.error("Unable to decode Bedrock response body: %s", exc, exc_info=True)
        return {}

    completion_text = parsed_body.get("completion") or parsed_body.get("outputText")
    if not completion_text and isinstance(parsed_body.get("results"), list):
        first_result = parsed_body["results"][0]
        completion_text = first_result.get("outputText") or first_result.get("completion")

    if not completion_text or not isinstance(completion_text, str):
        LOGGER.warning("Bedrock response did not contain completion text")
        return {}

    completion_text = completion_text.strip()
    json_start = completion_text.find("{")
    json_end = completion_text.rfind("}")
    if json_start != -1 and json_end != -1 and json_end >= json_start:
        completion_text = completion_text[json_start : json_end + 1]

    try:
        return json.loads(completion_text)
    except json.JSONDecodeError as exc:
        LOGGER.error("Unable to parse completion JSON: %s", exc, exc_info=True)
        return {}


def _merge_extractions(textract_labor: List[Dict[str, Any]], bedrock_result: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(textract_labor, list):
        textract_labor = []

    bedrock_vendor: str | None = None
    merged_labor = textract_labor
    summaries: Dict[str, Any] | None = None

    if isinstance(bedrock_result, dict) and bedrock_result:
        vendor_candidate = bedrock_result.get("vendor")
        if isinstance(vendor_candidate, str) and vendor_candidate.strip():
            bedrock_vendor = vendor_candidate.strip()

        labor_candidate = bedrock_result.get("labor")
        normalized_bedrock_labor: List[Dict[str, Any]] = []
        if isinstance(labor_candidate, list):
            for entry in labor_candidate:
                if not isinstance(entry, dict):
                    continue
                normalized_entry: Dict[str, Any] = {}
                normalized_entry["name"] = str(entry.get("name", "")).strip()
                normalized_entry["type"] = str(entry.get("type", "RS") or "RS").upper()

                regular_hours = _to_float(entry.get("reg_hours"))
                overtime_hours = _to_float(entry.get("ot_hours"))
                total_hours = _to_float(entry.get("hours"))
                if total_hours is None:
                    hours_parts = [value for value in (regular_hours, overtime_hours) if value is not None]
                    if hours_parts:
                        total_hours = round(sum(hours_parts), 2)

                rate_value = _to_float(entry.get("rate"))
                total_value = _to_float(entry.get("total"))

                normalized_entry["hours"] = total_hours
                normalized_entry["rate"] = rate_value
                normalized_entry["total"] = total_value
                if regular_hours is not None:
                    normalized_entry["hours_regular"] = regular_hours
                if overtime_hours is not None:
                    normalized_entry["hours_ot"] = overtime_hours

                if not normalized_entry["name"] and normalized_entry["hours"] in (None, 0):
                    continue

                normalized_bedrock_labor.append(normalized_entry)

        if normalized_bedrock_labor:
            merged_labor = normalized_bedrock_labor

        summaries_candidate = bedrock_result.get("summaries")
        if isinstance(summaries_candidate, dict):
            summaries = summaries_candidate

    return {"labor": merged_labor, "vendor": bedrock_vendor, "summaries": summaries or {}}


def _normalize_labor_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        name_value = str(row.get("name", "")).strip()
        worker_type_value = str(row.get("type", "RS") or "RS").upper()

        reg_hours_value = _to_float(row.get("reg_hours"))
        if reg_hours_value is None:
            reg_hours_value = _to_float(row.get("hours_regular"))
        if reg_hours_value is None:
            reg_hours_value = _to_float(row.get("regular_hours"))

        ot_hours_value = _to_float(row.get("ot_hours"))
        if ot_hours_value is None:
            ot_hours_value = _to_float(row.get("hours_ot"))
        if ot_hours_value is None:
            ot_hours_value = _to_float(row.get("overtime_hours"))

        rate_value = _to_float(row.get("rate"))
        if rate_value is None:
            rate_value = _to_float(row.get("hourly_rate"))

        total_value = _to_float(row.get("total"))
        if total_value is None:
            total_value = _to_float(row.get("amount"))

        total_hours_value = 0.0
        if reg_hours_value is not None:
            total_hours_value += reg_hours_value
        if ot_hours_value is not None:
            total_hours_value += ot_hours_value

        if total_value is None and rate_value is not None:
            effective_hours = 0.0
            if reg_hours_value is not None:
                effective_hours += reg_hours_value
            if ot_hours_value is not None:
                effective_hours += 1.5 * ot_hours_value
            if effective_hours:
                total_value = round(rate_value * effective_hours, 2)

        normalized.append(
            {
                "name": name_value,
                "type": worker_type_value,
                "reg_hours": reg_hours_value if reg_hours_value is not None else 0.0,
                "ot_hours": ot_hours_value if ot_hours_value is not None else 0.0,
                "rate": rate_value if rate_value is not None else 0.0,
                "total": total_value if total_value is not None else 0.0,
                "hours": round(total_hours_value, 2) if total_hours_value else 0.0,
            }
        )

    return normalized


def _summaries_from_labor(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_regular = 0.0
    total_ot = 0.0
    total_labor = 0.0
    for row in rows:
        total_regular += float(row.get("reg_hours") or 0.0)
        total_ot += float(row.get("ot_hours") or 0.0)
        total_labor += float(row.get("total") or 0.0)
    return {
        "total_regular_hours": round(total_regular, 2),
        "total_ot_hours": round(total_ot, 2),
        "total_labor_charges": round(total_labor, 2),
    }


def _poll_textract(job_id: str, timeout_seconds: int = 300) -> List[Dict[str, Any]]:
    deadline = time.time() + timeout_seconds
    blocks: List[Dict[str, Any]] = []
    next_token: str | None = None

    while time.time() < deadline:
        response = textract_client.get_document_analysis(JobId=job_id, NextToken=next_token)
        status = response.get("JobStatus")
        if status == "SUCCEEDED":
            blocks.extend(response.get("Blocks", []))
            next_token = response.get("NextToken")
            if not next_token:
                return blocks
        elif status == "FAILED":
            raise RuntimeError(f"Textract job {job_id} failed: {response.get('StatusMessage')}")
        else:
            time.sleep(2)

    raise TimeoutError(f"Textract job {job_id} timed out after {timeout_seconds}s")


def _tables_from_blocks(blocks: List[Dict[str, Any]]) -> List[List[List[str]]]:
    id_map = {block["Id"]: block for block in blocks if "Id" in block}
    tables: List[List[List[str]]] = []

    for block in blocks:
        if block.get("BlockType") != "TABLE":
            continue
        rows: Dict[int, Dict[int, str]] = {}
        for relationship in block.get("Relationships", []):
            if relationship.get("Type") != "CHILD":
                continue
            for cell_id in relationship.get("Ids", []):
                cell = id_map.get(cell_id)
                if not cell or cell.get("BlockType") != "CELL":
                    continue
                row_idx = cell.get("RowIndex", 1)
                col_idx = cell.get("ColumnIndex", 1)
                rows.setdefault(row_idx, {})[col_idx] = _cell_text(cell, id_map)
        ordered_rows = []
        for row_idx in sorted(rows):
            columns = rows[row_idx]
            ordered_rows.append([columns.get(col, "") for col in sorted(columns)])
        if ordered_rows:
            tables.append(ordered_rows)

    return tables


def _labour_entries(tables: List[List[List[str]]]) -> List[Dict[str, Any]]:
    labour: List[Dict[str, Any]] = []
    for table in tables:
        df = _table_to_df(table)
        if df is None or df.empty:
            continue
        if not _looks_like_labour(df.columns):
            continue
        for _, row in df.iterrows():
            regular_hours = _to_float(row.get("reg_hours"))
            overtime_hours = _to_float(row.get("ot_hours"))
            record = {
                "name": str(row.get("name", "")).strip(),
                "type": str(row.get("type", "RS") or "RS").upper(),
                "hours": _to_float(row.get("hours")),
                "rate": _to_float(row.get("rate")),
                "total": _to_float(row.get("total")),
            }
            if record["hours"] is None:
                summed_hours = [value for value in (regular_hours, overtime_hours) if value is not None]
                if summed_hours:
                    record["hours"] = round(sum(summed_hours), 2)
            if regular_hours is not None:
                record["hours_regular"] = regular_hours
            if overtime_hours is not None:
                record["hours_ot"] = overtime_hours
            if record["total"] is None and record["hours"] is not None and record["rate"] is not None:
                record["total"] = round(record["hours"] * record["rate"], 2)
            if not record["name"] and record["hours"] in (None, 0):
                continue
            labour.append(record)
    return labour


def _table_to_df(rows: List[List[str]]) -> pd.DataFrame | None:
    if len(rows) < 2:
        return None
    header = [_clean_header(cell) for cell in rows[0]]
    if not any(header):
        return None
    df = pd.DataFrame(rows[1:], columns=header)
    df = _rename_columns(df)
    for column in {"hours", "rate", "total"}:
        if column in df:
            df[column] = pd.to_numeric(df[column].str.replace(r"[^0-9.\-]", "", regex=True), errors="coerce")
    return df


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename: Dict[str, str] = {}
    seen: set[str] = set()
    for column in df.columns:
        lowered = column.lower()
        for field, aliases in COLUMN_ALIASES.items():
            if any(alias in lowered for alias in aliases) and field not in seen:
                rename[column] = field
                seen.add(field)
                break
    return df.rename(columns=rename)


def _looks_like_labour(columns: Iterable[str]) -> bool:
    joined = " ".join(columns).lower()
    return any(keyword in joined for keyword in LABOUR_KEYWORDS)


def _cell_text(cell: Dict[str, Any], block_map: Dict[str, Dict[str, Any]]) -> str:
    text_parts: List[str] = []
    for relationship in cell.get("Relationships", []):
        if relationship.get("Type") != "CHILD":
            continue
        for child_id in relationship.get("Ids", []):
            child = block_map.get(child_id)
            if child and child.get("BlockType") == "WORD":
                text_parts.append(child.get("Text", ""))
    return " ".join(text_parts).strip()


def _object_location(event: Dict[str, Any]) -> Tuple[str, str]:
    if "bucket" in event and "key" in event:
        return str(event["bucket"]), str(event["key"])
    if "file_info" in event:
        info = event["file_info"]
        return str(info.get("bucket") or event.get("bucket")), str(info.get("key"))
    records = event.get("Records") or []
    if records:
        record = records[0]["s3"]
        return record["bucket"]["name"], record["object"]["key"]
    raise ValueError("Event did not contain bucket/key information")


def _head_object(bucket: str, key: str) -> Dict[str, Any]:
    try:
        return s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        raise RuntimeError(f"Unable to read s3://{bucket}/{key}: {exc}") from exc


def _clean_header(value: str) -> str:
    return (value or "").strip().lower()


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).strip()
    if not cleaned:
        return None
    replacements = {
        "o": "0",
        "O": "0",
        "l": "1",
        "I": "1",
        "S": "5",
        "s": "5",
        "B": "8",
        "C": "0",
    }
    for old, new in replacements.items():
        cleaned = cleaned.replace(old, new)
    cleaned = cleaned.replace(" ", "")
    numeric = re.sub(r"[^0-9.\-]", "", cleaned)
    if numeric in {"", "-", "."}:
        return None
    try:
        return float(numeric)
    except ValueError:
        return None


def _parse_query_results(blocks: List[Dict[str, Any]]) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {}
    for block in blocks:
        if block.get("BlockType") != "QUERY_RESULT":
            continue
        query_info = block.get("Query", {})
        alias = str(query_info.get("Alias") or "").upper()
        text = (block.get("Text") or "").strip()
        if not alias or not text:
            continue
        field_name = QUERY_ALIAS_MAPPING.get(alias)
        if not field_name:
            continue
        if field_name == "vendor":
            metadata[field_name] = text.upper()
        elif field_name == "invoice_total":
            numeric_value = _to_float(text)
            if numeric_value is not None:
                metadata[field_name] = numeric_value
        else:
            metadata[field_name] = text
    return metadata


if __name__ == "__main__":
    demo = [
        ["Worker", "Type", "Hours", "Rate", "Total"],
        ["Alice Smith", "RS", "40", "70", "2800"],
        ["Bob Jones", "US", "42", "50", "2100"],
    ]
    print(json.dumps(_labour_entries([demo]), indent=2))
