import os, logging
from decimal import Decimal
from layers.common.python.common import resource

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

def seed_rates(event=None, context=None):
    table_name = os.environ.get("MWO_TABLE_NAME","mwo-rates")
    ddb = resource("dynamodb")
    table = ddb.Table(table_name)
    rates = [
        {"code":"RS", "desc":"Restoration Specialist", "rate": Decimal("70.00")},
        {"code":"GL", "desc":"General Labor", "rate": Decimal("40.00")},
    ]
    for r in rates:
        table.put_item(Item=r)
    return {"seeded": len(rates)}
