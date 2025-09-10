import os, logging
from decimal import Decimal
from layers.common.python.common import resource

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

def seed_rates(event=None, context=None):
    """Populate the DynamoDB table with standard labor rates.

    For local testing the function falls back to putting items individually, but
    in a real deployment it uses ``batch_write_item`` for efficiency.
    """
    table_name = os.environ.get("MWO_TABLE_NAME", "mwo-rates")
    ddb = resource("dynamodb")
    table = ddb.Table(table_name)

    rates = [
        {"code": "RS", "desc": "Restoration Specialist", "rate": Decimal("70.00")},
        {"code": "GL", "desc": "General Labor", "rate": Decimal("43.00")},
        {"code": "PM", "desc": "Project Manager", "rate": Decimal("115.00")},
        {"code": "SRPM", "desc": "Senior Project Manager", "rate": Decimal("135.00")},
        {"code": "PCA", "desc": "Project Cost Analyst", "rate": Decimal("57.00")},
    ]

    try:
        with table.batch_writer() as batch:
            for r in rates:
                batch.put_item(Item=r)
    except Exception:
        for r in rates:
            table.put_item(Item=r)

    return {"seeded": len(rates)}
