import json, logging
from layers.common.python.common import resource

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

RATES = {
    "RS": 70,
    "GL": 43,
    "PM": 115,
    "SRPM": 135,
    "PCA": 57,
}


def handler(event=None, context=None):
    """Seed the knowledge base (DynamoDB table) with labor rates."""
    table_name = (event or {}).get("table_name") or "mwo-rates"
    ddb = resource("dynamodb")
    table = ddb.Table(table_name)
    for code, rate in RATES.items():
        table.put_item(Item={"code": code, "rate": rate})
    return {"seeded": len(RATES)}
