"""Minimal DynamoDB seeding for msa-rates table."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(os.getenv("LOG_LEVEL", "INFO"))

DYNAMODB = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("MSA_RATES_TABLE_NAME", "msa-rates")
DEFAULT_EFFECTIVE_DATE = os.environ.get("MSA_DEFAULT_EFFECTIVE_DATE", "default")

SEED_DATA = [
    {"rate_id": "RS", "effective_date": DEFAULT_EFFECTIVE_DATE, "standard_rate": Decimal("70.00"), "description": "Regular Skilled"},
    {"rate_id": "US", "effective_date": DEFAULT_EFFECTIVE_DATE, "standard_rate": Decimal("45.00"), "description": "Unskilled"},
    {"rate_id": "SS", "effective_date": DEFAULT_EFFECTIVE_DATE, "standard_rate": Decimal("55.00"), "description": "Semi Skilled"},
    {"rate_id": "SU", "effective_date": DEFAULT_EFFECTIVE_DATE, "standard_rate": Decimal("85.00"), "description": "Supervisor"},
]


def lambda_handler(_event, _context):
    table = DYNAMODB.Table(TABLE_NAME)
    inserted = 0
    for item in SEED_DATA:
        try:
            payload = {**item, "created_at": datetime.utcnow().isoformat()}
            table.put_item(Item=payload)
            inserted += 1
        except ClientError as exc:  # pragma: no cover
            LOGGER.warning("Failed to insert %s: %s", item["rate_id"], exc)
    return {"status": "ok", "inserted": inserted, "table": TABLE_NAME}


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
