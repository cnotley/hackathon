"""Minimal reconciliation Lambda comparing labour costs with MSA rates."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List

import boto3
import numpy as np
from boto3.dynamodb.conditions import Key

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv("LOG_LEVEL", "INFO"))

MSA_TABLE_NAME = os.getenv("MSA_RATES_TABLE_NAME", "msa-rates")
DYNAMODB = boto3.resource("dynamodb")
MSA_TABLE = DYNAMODB.Table(MSA_TABLE_NAME)
VARIANCE_THRESHOLD = 1.05  # 5% over standard rate
DEFAULT_EFFECTIVE_DATE = os.getenv("MSA_DEFAULT_EFFECTIVE_DATE", "2024-01-01")
OVERTIME_THRESHOLD = 40.0


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    labour_rows = _extract_labour(event)
    if not labour_rows:
        return {"status": "no-data", "discrepancies": [], "total_savings": 0.0}

    msa_rates = _batch_lookup_rates({row.get("type", "RS").upper() for row in labour_rows})
    discrepancies: List[Dict[str, Any]] = []
    savings_total = 0.0

    costs = []
    for row in labour_rows:
        labor_type = str(row.get("type", "RS")).upper()
        hours = float(row.get("hours") or 0.0)
        rate = float(row.get("rate") or 0.0)
        total = row.get("total")
        if total is None:
            total = hours * rate
        costs.append(total)

        msa_rate = msa_rates.get(labor_type)
        if msa_rate is None:
            continue

        variance = rate / msa_rate if msa_rate else 0
        if variance > VARIANCE_THRESHOLD:
            variance_amount = (rate - msa_rate) * hours
            savings_total += max(0.0, variance_amount)
            discrepancies.append(
                {
                    "type": "rate_variance",
                    "worker": row.get("name", "Unknown"),
                    "labor_type": labor_type,
                    "hours": hours,
                    "actual_rate": round(rate, 2),
                    "msa_rate": round(msa_rate, 2),
                    "variance_multiplier": round(variance, 3),
                    "savings": round(max(0.0, variance_amount), 2),
                }
            )

        if hours > OVERTIME_THRESHOLD:
            discrepancies.append(
                {
                    "type": "overtime",
                    "worker": row.get("name", "Unknown"),
                    "labor_type": labor_type,
                    "hours": hours,
                    "overtime_hours": round(hours - OVERTIME_THRESHOLD, 2),
                }
            )

    # Duplicate detection using tuple of key fields
    seen_keys = set()
    for row in labour_rows:
        key = (
            row.get("name", "").strip().lower(),
            str(row.get("type", "")).upper(),
            round(float(row.get("hours") or 0.0), 2),
            round(float(row.get("rate") or 0.0), 2),
        )
        if key in seen_keys:
            discrepancies.append(
                {
                    "type": "duplicate",
                    "worker": row.get("name", "Unknown"),
                    "labor_type": row.get("type", "RS"),
                    "hours": row.get("hours", 0.0),
                    "rate": row.get("rate", 0.0),
                }
            )
        else:
            seen_keys.add(key)

    # Simple z-score anomaly detection on totals
    if costs:
        z_scores = np.abs((np.array(costs) - np.mean(costs)) / (np.std(costs) or 1.0))
        for row, z in zip(labour_rows, z_scores):
            if z > 3.0:
                discrepancies.append(
                    {
                        "type": "cost_anomaly",
                        "worker": row.get("name", "Unknown"),
                        "labor_type": row.get("type", "RS"),
                        "total": row.get("total"),
                        "z_score": round(float(z), 2),
                    }
                )

    return {
        "status": "ok",
        "discrepancies": discrepancies,
        "total_savings": round(savings_total, 2),
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


def _batch_lookup_rates(labor_types: set[str]) -> Dict[str, float]:
    results: Dict[str, float] = {}
    for labor_type in labor_types:
        try:
            response = MSA_TABLE.get_item(
                Key={"rate_id": f"{labor_type}_default", "effective_date": DEFAULT_EFFECTIVE_DATE}
            )
            item = response.get("Item")
            if item and "standard_rate" in item:
                results[labor_type] = float(item["standard_rate"])
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning("Failed to fetch rate for %s: %s", labor_type, exc)
    return results


if __name__ == "__main__":
    demo_event = {
        "labor": [
            {"name": "Alice", "type": "RS", "hours": 45, "rate": 80},
            {"name": "Alice", "type": "RS", "hours": 45, "rate": 80},
            {"name": "Bob", "type": "US", "hours": 38, "rate": 48},
        ]
    }
    print(json.dumps(lambda_handler(demo_event, None), indent=2))
