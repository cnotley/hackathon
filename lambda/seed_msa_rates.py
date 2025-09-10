"""Minimal DynamoDB seeding for msa-rates table."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List

import boto3
from botocore.exceptions import ClientError

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv("LOG_LEVEL", "INFO"))

DYNAMODB = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("MSA_RATES_TABLE_NAME", "msa-rates")
DEFAULT_EFFECTIVE_DATE = os.environ.get("MSA_DEFAULT_EFFECTIVE_DATE", "default")
DEFAULT_VENDOR_NAME = os.environ.get("MSA_VENDOR_NAME", "SERVPRO")

SEED_ITEMS: List[Dict[str, Any]] = [
    {
        "rate_id": "RS",
        "description": "Restoration Supervisor",
        "standard_rate": Decimal("77.00"),
        "placeholder_rate": Decimal("70.00"),
        "ot_multiplier": Decimal("1.50"),
    },
    {
        "rate_id": "GL",
        "description": "General Labor",
        "standard_rate": Decimal("43.00"),
        "placeholder_rate": Decimal("43.00"),
        "ot_multiplier": Decimal("1.50"),
    },
    {
        "rate_id": "PM",
        "description": "Project Manager",
        "standard_rate": Decimal("115.00"),
        "placeholder_rate": Decimal("100.00"),
        "ot_multiplier": Decimal("1.50"),
    },
    {
        "rate_id": "SRPM",
        "description": "Senior Project Manager",
        "standard_rate": Decimal("135.00"),
        "placeholder_rate": Decimal("120.00"),
        "ot_multiplier": Decimal("1.50"),
    },
    {
        "rate_id": "CDL",
        "description": "CDL Driver",
        "standard_rate": Decimal("68.00"),
        "placeholder_rate": Decimal("60.00"),
        "ot_multiplier": Decimal("1.50"),
    },
    {
        "rate_id": "PC",
        "description": "Project Coordinator",
        "standard_rate": Decimal("145.00"),
        "placeholder_rate": Decimal("130.00"),
        "ot_multiplier": Decimal("1.50"),
    },
    {
        "rate_id": "DF",
        "description": "Drywaller / Finisher",
        "standard_rate": Decimal("88.00"),
        "placeholder_rate": Decimal("80.00"),
        "ot_multiplier": Decimal("1.50"),
    },
    {
        "rate_id": "PCA",
        "description": "Project Clerical Administrator",
        "standard_rate": Decimal("57.00"),
        "placeholder_rate": Decimal("50.00"),
        "ot_multiplier": Decimal("1.50"),
    },
]


def _determine_vendor_name(event: Dict[str, Any] | None) -> str:
    if event is None:
        return DEFAULT_VENDOR_NAME
    vendor_override = event.get("vendor") or event.get("Vendor") or event.get("VENDOR")
    if vendor_override:
        return str(vendor_override)
    return DEFAULT_VENDOR_NAME


def _items_with_metadata(vendor_name: str, seed_items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized_vendor = vendor_name.strip() or DEFAULT_VENDOR_NAME
    normalized_vendor = normalized_vendor.upper()
    items: List[Dict[str, Any]] = []
    for seed_item in seed_items:
        base_item = dict(seed_item)
        effective_date = base_item.pop("effective_date", DEFAULT_EFFECTIVE_DATE)
        vendor_payload = {
            **base_item,
            "rate_id": f"{normalized_vendor}#{seed_item['rate_id']}",
            "vendor": normalized_vendor,
            "effective_date": effective_date,
            "created_at": datetime.utcnow().isoformat(),
        }
        placeholder_value = base_item.get("placeholder_rate", base_item.get("standard_rate"))
        default_payload = {
            **base_item,
            "rate_id": f"{seed_item['rate_id']}_default",
            "vendor": normalized_vendor,
            "effective_date": effective_date,
            "created_at": datetime.utcnow().isoformat(),
            "standard_rate": placeholder_value,
        }
        items.append(vendor_payload)
        items.append(default_payload)
    items.append(
        {
            "rate_id": "ratio_rules_su_rs",
            "vendor": normalized_vendor,
            "effective_date": DEFAULT_EFFECTIVE_DATE,
            "created_at": datetime.utcnow().isoformat(),
            "max_ratio": Decimal("6.0"),
        }
    )
    return items


def lambda_handler(event, _context):
    vendor_name = _determine_vendor_name(event)
    items_to_seed = _items_with_metadata(vendor_name, SEED_ITEMS)
    LOGGER.info(
        "Seeding %s MSA rate items for vendor %s into table %s",
        len(items_to_seed),
        vendor_name,
        TABLE_NAME,
    )

    table = DYNAMODB.Table(TABLE_NAME)
    inserted = 0
    for item in items_to_seed:
        try:
            table.put_item(Item=item)
            inserted += 1
            LOGGER.info("Inserted rate %s", item["rate_id"])
        except ClientError as exc:  # pragma: no cover
            LOGGER.warning("Failed to insert %s: %s", item["rate_id"], exc)
    return {"status": "ok", "inserted": inserted, "table": TABLE_NAME, "vendor": vendor_name}


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
