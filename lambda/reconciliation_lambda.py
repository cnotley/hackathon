"""Minimal reconciliation Lambda comparing labour costs with MSA rates."""

from __future__ import annotations

import json
import logging
import os
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import boto3
import numpy as np

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv("LOG_LEVEL", "INFO"))

MSA_TABLE_NAME = os.getenv("MSA_RATES_TABLE_NAME", "msa-rates")
DYNAMODB = boto3.resource("dynamodb")
MSA_TABLE = DYNAMODB.Table(MSA_TABLE_NAME)
DEFAULT_VENDOR_NAME = os.getenv("MSA_VENDOR_NAME", "SERVPRO").upper()
VARIANCE_THRESHOLD = float(os.getenv("MSA_VARIANCE_THRESHOLD", "1.05"))
DEFAULT_EFFECTIVE_DATE = os.getenv("MSA_DEFAULT_EFFECTIVE_DATE", "2024-01-01")
OVERTIME_THRESHOLD = float(os.getenv("MSA_OVERTIME_THRESHOLD", "40.0"))


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    labour_rows = _extract_labour(event)
    vendor = _extract_vendor(event)
    if not labour_rows:
        return {"status": "no-data", "discrepancies": [], "total_savings": 0.0, "vendor": vendor}

    labour_types = {str(row.get("type", "RS")).upper() for row in labour_rows}
    msa_rates = _batch_lookup_rates(vendor, labour_types)

    discrepancies: List[Dict[str, Any]] = []
    savings_total = 0.0
    worker_hours: Dict[str, float] = {}
    costs: List[float] = []
    seen_keys: set[Tuple[str, str, float, float]] = set()

    worker_display_names: Dict[str, str] = {}

    for row in labour_rows:
        labor_type = str(row.get("type", "RS")).upper()
        name = str(row.get("name", "Unknown")).strip() or "Unknown"
        reg_hours = _to_float(row.get("reg_hours")) or _to_float(row.get("hours_regular")) or 0.0
        ot_hours = _to_float(row.get("ot_hours")) or _to_float(row.get("hours_ot")) or 0.0
        hours = _to_float(row.get("hours"))
        if hours is None:
            hours = reg_hours + ot_hours
        rate = _to_float(row.get("rate")) or 0.0
        total = _to_float(row.get("total"))
        if total is None:
            total = (reg_hours + ot_hours) * rate
        costs.append(total)

        worker_key = name.lower()
        worker_hours[worker_key] = worker_hours.get(worker_key, 0.0) + hours
        worker_display_names.setdefault(worker_key, name)

        composite_key: Tuple[str, str, float, float] = (worker_key, labor_type, round(hours, 2), round(rate, 2))
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

        msa_rate = msa_rates.get(labor_type)
        if msa_rate is None:
            discrepancies.append(
                {
                    "type": "missing_rate",
                    "worker": name,
                    "labor_type": labor_type,
                    "rate_id": f"{vendor}#{labor_type}_default",
                    "vendor": vendor,
                    "regular_hours": round(reg_hours, 2),
                    "overtime_hours": round(ot_hours, 2),
                }
            )
            continue

        variance_multiplier = rate / msa_rate if msa_rate else 0.0
        if variance_multiplier > VARIANCE_THRESHOLD:
            rate_difference = rate - msa_rate
            base_hours = max(hours, 0.0)
            overtime_hours = max(ot_hours, 0.0)
            base_savings = rate_difference * base_hours if rate_difference > 0 and base_hours else 0.0
            overtime_premium_savings = rate_difference * 0.5 * overtime_hours if rate_difference > 0 and overtime_hours else 0.0
            variance_amount = base_savings + overtime_premium_savings
            savings_total += max(0.0, variance_amount)
            discrepancies.append(
                {
                    "type": "rate_variance",
                    "worker": name,
                    "labor_type": labor_type,
                    "hours": round(hours, 2),
                    "regular_hours": round(reg_hours, 2),
                    "overtime_hours": round(ot_hours, 2),
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
                    "worker": worker_display_names.get(worker_key, worker_key.title() if worker_key else "Unknown"),
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
        "status": "ok",
        "discrepancies": discrepancies,
        "total_savings": round(savings_total, 2),
        "vendor": vendor,
    }


def _extract_labour(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    if "labor" in event:
        return event["labor"]
    if "reconciliation" in event and isinstance(event["reconciliation"], dict):
        return event["reconciliation"].get("labor", [])
    if "extraction" in event and isinstance(event["extraction"], dict):
        payload = event["extraction"].get("Payload", {})
        if isinstance(payload, dict):
            return payload.get("labor", [])
    return []


def _batch_lookup_rates(vendor: str, labor_types: set[str]) -> Dict[str, float]:
    results: Dict[str, float] = {}
    normalized_vendor = (vendor or DEFAULT_VENDOR_NAME).strip().upper() or DEFAULT_VENDOR_NAME
    for labor_type in labor_types:
        try:
            rate_candidates = [f"{normalized_vendor}#{labor_type}_default", f"{normalized_vendor}#{labor_type}"]
            for rate_id in rate_candidates:
                response = MSA_TABLE.get_item(
                    Key={"rate_id": rate_id, "effective_date": DEFAULT_EFFECTIVE_DATE}
                )
                item = response.get("Item")
                if item:
                    rate_value = _to_float(item.get("placeholder_rate"))
                    if rate_value is None:
                        rate_value = _to_float(item.get("standard_rate"))
                    if rate_value is not None:
                        results[labor_type] = rate_value
                        break
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning("Failed to fetch rate for %s (%s): %s", labor_type, normalized_vendor, exc)
    return results


def _extract_vendor(event: Dict[str, Any]) -> str:
    potential_values = [
        event.get("vendor"),
        event.get("metadata", {}).get("vendor"),
    ]
    extraction_payload = event.get("extraction")
    if isinstance(extraction_payload, dict):
        payload = extraction_payload.get("Payload") or {}
        if isinstance(payload, dict):
            potential_values.append(payload.get("vendor"))
            metadata = payload.get("metadata")
            if isinstance(metadata, dict):
                potential_values.append(metadata.get("vendor"))
    for value in potential_values:
        if value:
            return str(value).strip().upper()
    return DEFAULT_VENDOR_NAME


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
        "labor": [
            {"name": "Alice", "type": "RS", "hours": 45, "rate": 80},
            {"name": "Alice", "type": "RS", "hours": 45, "rate": 80},
            {"name": "Bob", "type": "US", "hours": 38, "rate": 48},
        ]
    }
    print(json.dumps(lambda_handler(demo_event, None), indent=2))
