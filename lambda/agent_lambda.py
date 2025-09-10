"""Simplified invoice auditing Lambda focused on labour discrepancies."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from decimal import Decimal
from functools import lru_cache
from typing import Any, Dict, List, Tuple

import boto3
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")  # kept for future extensions that may need S3 metadata

MSA_RATES_TABLE = os.getenv("MSA_RATES_TABLE", "msa-rates")
MSA_TABLE = dynamodb.Table(MSA_RATES_TABLE)
DEFAULT_EFFECTIVE_DATE = os.getenv("MSA_DEFAULT_EFFECTIVE_DATE", "2024-01-01")
DEFAULT_VENDOR_NAME = os.getenv("MSA_VENDOR_NAME", "SERVPRO").upper()
VARIANCE_THRESHOLD = float(os.getenv("MSA_VARIANCE_THRESHOLD", "1.05"))
OVERTIME_THRESHOLD = float(os.getenv("MSA_OVERTIME_THRESHOLD", "40.0"))


def _rate_key(vendor: str, labor_type: str, location: str) -> Dict[str, str]:
    return {
        "rate_id": f"{vendor}#{labor_type}#{location}",
        "effective_date": DEFAULT_EFFECTIVE_DATE,
    }


class MSARatesManager:
    """Fetch vendor-aware MSA rates from DynamoDB."""

    def __init__(self) -> None:
        self.table = MSA_TABLE

    @lru_cache(maxsize=256)
    def get_rate_for_labor_type(self, vendor: str, labor_type: str, location: str = "default") -> float | None:
        normalized_vendor = (vendor or DEFAULT_VENDOR_NAME).strip().upper() or DEFAULT_VENDOR_NAME
        labor_code = str(labor_type or "RS").upper()
        locations: List[str] = [location] if location and location != "default" else []
        locations.append("default")

        rate_keys: List[Dict[str, str]] = [
            _rate_key(normalized_vendor, labor_code, loc) for loc in locations
        ]
        rate_keys.extend(
            [
                {
                    "rate_id": f"{normalized_vendor}#{labor_code}_default",
                    "effective_date": DEFAULT_EFFECTIVE_DATE,
                },
                {
                    "rate_id": f"{normalized_vendor}#{labor_code}",
                    "effective_date": DEFAULT_EFFECTIVE_DATE,
                },
            ]
        )

        for key in rate_keys:
            try:
                response = self.table.get_item(Key=key)
                item = response.get("Item")
                if item and "standard_rate" in item:
                    rate_value = _to_float(item["standard_rate"])
                    if rate_value is not None:
                        return rate_value
            except Exception as exc:  # pragma: no cover - defensive logging only
                logger.warning("Failed to fetch MSA rate %s: %s", key["rate_id"], exc)
        try:
            legacy_response = self.table.get_item(Key={"labor_type": labor_code, "location": "default"})
            legacy_item = legacy_response.get("Item")
            if legacy_item and "standard_rate" in legacy_item:
                return _to_float(legacy_item["standard_rate"])
        except Exception:
            pass
        return None


class InvoiceAuditor:
    """Performs labour discrepancy checks mirroring reconciliation logic."""

    def __init__(self) -> None:
        self.msa_manager = MSARatesManager()

    def audit_labour(self, labour_rows: List[Dict[str, Any]], vendor: str) -> Dict[str, Any]:
        discrepancies: List[Dict[str, Any]] = []
        savings_total = 0.0
        worker_hours: Dict[str, float] = {}
        costs: List[float] = []
        seen_keys: set[Tuple[str, str, float, float]] = set()

        for row in labour_rows:
            name = str(row.get("name") or "Unknown").strip()
            worker_key = name.lower()
            labor_type = str(row.get("type") or "RS").upper()
            hours = _to_float(row.get("hours")) or 0.0
            regular_hours = _to_float(row.get("hours_regular"))
            overtime_hours = _to_float(row.get("hours_ot"))
            if (hours is None or hours == 0.0) and regular_hours is not None:
                component_hours = [value for value in (regular_hours, overtime_hours) if value is not None]
                if component_hours:
                    hours = round(sum(component_hours), 2)
            rate = _to_float(row.get("rate")) or 0.0
            total = _to_float(row.get("total"))
            if total is None:
                total = hours * rate
            costs.append(total)

            worker_hours[worker_key] = worker_hours.get(worker_key, 0.0) + hours

            composite_key = (worker_key, labor_type, round(hours, 2), round(rate, 2))
            if composite_key in seen_keys:
                discrepancies.append(
                    {
                        "type": "duplicate",
                        "worker": name,
                        "labor_type": labor_type,
                        "hours": round(hours, 2),
                        "rate": round(rate, 2),
                    }
                )
            else:
                seen_keys.add(composite_key)

            msa_rate = self.msa_manager.get_rate_for_labor_type(vendor, labor_type)
            if msa_rate is None:
                discrepancies.append(
                    {
                        "type": "missing_rate",
                        "worker": name,
                        "labor_type": labor_type,
                        "rate_id": f"{vendor}#{labor_type}#default",
                        "vendor": vendor,
                    }
                )
                continue

            variance_multiplier = rate / msa_rate if msa_rate else 0.0
            if variance_multiplier > VARIANCE_THRESHOLD:
                variance_amount = (rate - msa_rate) * hours
                savings_total += max(0.0, variance_amount)
                discrepancies.append(
                    {
                        "type": "rate_variance",
                        "worker": name,
                        "labor_type": labor_type,
                        "hours": round(hours, 2),
                        "actual_rate": round(rate, 2),
                        "msa_rate": round(msa_rate, 2),
                        "variance_multiplier": round(variance_multiplier, 3),
                        "savings": round(max(0.0, variance_amount), 2),
                    }
                )

        for worker_key, total_hours in worker_hours.items():
            if total_hours > OVERTIME_THRESHOLD:
                discrepancies.append(
                    {
                        "type": "overtime",
                        "worker": worker_key.title() if worker_key else "Unknown",
                        "hours": round(total_hours, 2),
                        "overtime_hours": round(total_hours - OVERTIME_THRESHOLD, 2),
                    }
                )

        if costs:
            array_costs = np.array(costs, dtype=float)
            std_dev = np.std(array_costs) or 1.0
            z_scores = np.abs((array_costs - np.mean(array_costs)) / std_dev)
            for row, z_score_value, total_cost in zip(labour_rows, z_scores, costs):
                if z_score_value > 3.0:
                    discrepancies.append(
                        {
                            "type": "cost_anomaly",
                            "worker": str(row.get("name", "Unknown")),
                            "labor_type": str(row.get("type", "RS")),
                            "total": round(float(total_cost), 2),
                            "z_score": round(float(z_score_value), 2),
                        }
                    )

        return {
            "discrepancies": discrepancies,
            "total_savings": round(savings_total, 2),
        }


class BedrockAgentManager:
    """Handles Bedrock agent interaction with a deterministic fallback."""

    def __init__(
        self,
        agent_client: Any | None = None,
        runtime_client: Any | None = None,
    ) -> None:
        self.agent_client = agent_client or boto3.client("bedrock-agent-runtime")
        self.runtime_client = runtime_client or boto3.client("bedrock-runtime")
        self._session_cache: Dict[str, Dict[str, Any]] = {}
        self.msa_manager = MSARatesManager()

    def invoke_agent(self, query: str, session_id: str | None = None) -> Dict[str, Any]:
        session_key = session_id or f"session-{int(time.time() * 1000)}"
        try:
            prompt = query or "Summarize the labour audit findings."
            self.agent_client.invoke_agent(
                agentId=os.getenv("BEDROCK_AGENT_ID", ""),
                inputText=prompt,
                sessionState={"sessionId": session_key},
            )
            result = {
                "status": "success",
                "response": "Agent processing in progress.",
                "session_id": session_key,
            }
        except Exception:
            result = self._build_fallback_response(session_key)
        self._session_cache[session_key] = result
        return result

    def _build_fallback_response(self, session_key: str) -> Dict[str, Any]:
        rs_rate = self.msa_manager.get_rate_for_labor_type(DEFAULT_VENDOR_NAME, "RS") or 0.0
        message = (
            "Fallback analysis:\n"
            f"- RS (Regular Skilled) MSA rate: ${rs_rate:.2f}.\n"
            "Use the audit discrepancies to determine approvals."
        )
        return {"status": "fallback_success", "response": message, "session_id": session_key}


def call_extraction_lambda(bucket: str | None, key: str | None) -> Dict[str, Any]:
    if not bucket or not key:
        return {}
    payload = json.dumps({"bucket": bucket, "key": key}).encode("utf-8")
    function_name = os.getenv("EXTRACTION_LAMBDA_NAME", "extraction-lambda")
    lambda_client = boto3.client("lambda")
    try:
        response = lambda_client.invoke(FunctionName=function_name, Payload=payload)
        body = response.get("Payload")
        if body and hasattr(body, "read"):
            raw = body.read()
            if raw:
                return json.loads(raw)
    except Exception as exc:  # pragma: no cover - defensive logging only
        logger.warning("Extraction invocation failed for %s/%s: %s", bucket, key, exc)
    return {}


def _summarize_discrepancies(discrepancies: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {
        "total_discrepancies": len(discrepancies),
        "rate_variances": 0,
        "overtime": 0,
        "missing_rate": 0,
        "duplicates": 0,
        "cost_anomaly": 0,
    }
    key_mapping = {
        "rate_variance": "rate_variances",
        "overtime": "overtime",
        "missing_rate": "missing_rate",
        "duplicate": "duplicates",
        "cost_anomaly": "cost_anomaly",
    }
    for entry in discrepancies:
        entry_type = entry.get("type")
        mapped_key = key_mapping.get(entry_type)
        if mapped_key and mapped_key in summary:
            summary[mapped_key] += 1
    return summary


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    start_time = time.time()
    try:
        logger.info("Received event: %s", json.dumps(event, default=str))
        if not isinstance(event, dict):
            raise ValueError("Event must be a dictionary")

        action = event.get("action", "audit")
        if action != "audit":
            raise ValueError(f"Unsupported action for MVP: {action}")

        bucket = event.get("bucket") or event.get("context", {}).get("file_info", {}).get("bucket")
        key = event.get("key") or event.get("context", {}).get("file_info", {}).get("key")

        extracted_data = event.get("extracted_data")
        if not extracted_data:
            extracted_data = call_extraction_lambda(bucket, key)
        if not extracted_data:
            raise ValueError("No extracted data available for auditing")

        labour_rows = _extract_labour_rows(extracted_data)
        vendor = _extract_vendor(event, extracted_data)

        auditor = InvoiceAuditor()
        audit_payload = auditor.audit_labour(labour_rows, vendor)
        discrepancies = audit_payload.get("discrepancies", [])
        summary = _summarize_discrepancies(discrepancies)

        agent_manager = BedrockAgentManager()
        agent_result = agent_manager.invoke_agent(
            query="Summarize the latest invoice audit results.",
            session_id=event.get("session_id"),
        )

        return {
            "status": "pending_approval",
            "vendor": vendor,
            "timestamp": datetime.utcnow().isoformat(),
            "audit_results": {
                "summary": summary,
                "discrepancies": discrepancies,
                "total_savings": audit_payload.get("total_savings", 0.0),
            },
            "agent_response": agent_result,
        }
    except ValueError as exc:
        logger.warning("Validation error: %s", exc)
        return {
            "status": "error",
            "error_type": "validation_error",
            "message": str(exc),
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Unhandled error: %s", exc)
        return {
            "status": "error",
            "error_type": "internal_error",
            "message": str(exc),
            "timestamp": datetime.utcnow().isoformat(),
        }
    finally:
        duration = time.time() - start_time
        logger.info("Audit duration %.2fs", duration)


def _extract_labour_rows(extracted_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(extracted_data, dict):
        return []
    labour_rows = extracted_data.get("labor")
    if isinstance(labour_rows, list):
        return [_normalize_labour_row(row) for row in labour_rows]
    normalized = extracted_data.get("normalized_data")
    if isinstance(normalized, dict):
        labour_rows = normalized.get("labor")
        if isinstance(labour_rows, list):
            return [_normalize_labour_row(row) for row in labour_rows]
    return []


def _extract_vendor(event: Dict[str, Any], extracted_data: Dict[str, Any]) -> str:
    potential_values = [
        event.get("vendor"),
        event.get("metadata", {}).get("vendor") if isinstance(event.get("metadata"), dict) else None,
    ]
    if isinstance(extracted_data, dict):
        potential_values.extend(
            [
                extracted_data.get("vendor"),
                extracted_data.get("metadata", {}).get("vendor") if isinstance(extracted_data.get("metadata"), dict) else None,
            ]
        )
    for value in potential_values:
        if value:
            return str(value).strip().upper()
    return DEFAULT_VENDOR_NAME


def _normalize_labour_row(row: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    normalized_row = dict(row)
    if "hours" not in normalized_row and "total_hours" in normalized_row:
        normalized_row["hours"] = normalized_row["total_hours"]
    if "rate" not in normalized_row and "unit_price" in normalized_row:
        normalized_row["rate"] = normalized_row["unit_price"]
    if "type" in normalized_row and isinstance(normalized_row["type"], str):
        normalized_row["type"] = normalized_row["type"].upper()
    return normalized_row


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    demo_event = {
        "action": "audit",
        "extracted_data": {
            "labor": [
                {"name": "Alice", "type": "RS", "hours": 45, "rate": 85},
                {"name": "Bob", "type": "GL", "hours": 38, "rate": 50},
                {"name": "Alice", "type": "RS", "hours": 45, "rate": 85},
            ],
            "vendor": "SERVPRO",
        },
    }
    print(json.dumps(lambda_handler(demo_event, None), indent=2, default=str))
