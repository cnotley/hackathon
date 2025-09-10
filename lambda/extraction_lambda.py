"""Minimal labour-focused Textract extractor."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Tuple

import boto3
import pandas as pd
from botocore.exceptions import ClientError

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv("LOG_LEVEL", "INFO"))

s3_client = boto3.client("s3")
textract_client = boto3.client("textract")

ASYNC_THRESHOLD_BYTES = 500 * 1024
LABOUR_KEYWORDS = {"labour", "labor", "worker", "type", "hours", "rate"}
COLUMN_ALIASES: Dict[str, Iterable[str]] = {
    "name": ("name", "worker", "employee", "person"),
    "type": ("type", "classification", "class", "role"),
    "hours": ("hours", "hrs", "quantity", "qty"),
    "rate": ("rate", "hourly", "unit price", "cost"),
    "total": ("total", "amount", "line total"),
}


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    bucket, key = _object_location(event)
    metadata = _head_object(bucket, key)
    blocks = _run_textract(bucket, key, metadata["ContentLength"])
    tables = _tables_from_blocks(blocks)
    labour_rows = _labour_entries(tables)

    return {
        "status": "ok",
        "bucket": bucket,
        "key": key,
        "record_count": len(labour_rows),
        "labor": labour_rows,
    }


def _run_textract(bucket: str, key: str, size_bytes: int) -> List[Dict[str, Any]]:
    if size_bytes > ASYNC_THRESHOLD_BYTES:
        job = textract_client.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
            FeatureTypes=["TABLES"],
        )
        return _poll_textract(job["JobId"])

    response = textract_client.analyze_document(
        Document={"S3Object": {"Bucket": bucket, "Name": key}},
        FeatureTypes=["TABLES"],
    )
    return response.get("Blocks", [])


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
            record = {
                "name": str(row.get("name", "")).strip(),
                "type": str(row.get("type", "RS") or "RS").upper(),
                "hours": _to_float(row.get("hours")),
                "rate": _to_float(row.get("rate")),
                "total": _to_float(row.get("total")),
            }
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
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    demo = [
        ["Worker", "Type", "Hours", "Rate", "Total"],
        ["Alice Smith", "RS", "40", "70", "2800"],
        ["Bob Jones", "US", "42", "50", "2100"],
    ]
    print(json.dumps(_labour_entries([demo]), indent=2))
