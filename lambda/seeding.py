import os, json, logging, time
from decimal import Decimal
from layers.common.python.common import resource, client

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def seed_rates(event=None, context=None):
    """Populate the DynamoDB table with labor rates and related metrics."""
    table_name = os.environ.get("MWO_TABLE_NAME", "mwo-rates")
    ddb_res = resource("dynamodb")
    ddb = client("dynamodb")
    try:
        ddb.describe_table(TableName=table_name)
    except ddb.exceptions.ResourceNotFoundException:
        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "code", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "code", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
        )
        time.sleep(10)
    table = ddb_res.Table(table_name)

    rates = [
        {"code": "RS", "desc": "Restoration Specialist", "rate_usd": Decimal("70"), "rate_eur": Decimal("65"), "currency": "USD"},
        {"code": "GL", "desc": "General Labor", "rate_usd": Decimal("43"), "rate_eur": Decimal("40"), "currency": "USD"},
        {"code": "PM", "desc": "Project Manager", "rate_usd": Decimal("115"), "rate_eur": Decimal("110"), "currency": "USD"},
        {"code": "SRPM", "desc": "Senior Project Manager", "rate_usd": Decimal("135"), "rate_eur": Decimal("130"), "currency": "USD"},
        {"code": "PCA", "desc": "Project Cost Analyst", "rate_usd": Decimal("57"), "rate_eur": Decimal("53"), "currency": "USD"},
        {"code": "PM:GL", "desc": "Management to Labor Ratio", "ratio_max": Decimal("0.2")},
    ]

    try:
        with table.batch_writer() as batch:
            for r in rates:
                batch.put_item(Item=r)
    except Exception:
        for r in rates:
            table.put_item(Item=r)

    cw = client("cloudwatch")
    try:
        cw.put_metric_data(
            Namespace="AuditSeeding",
            MetricData=[{"MetricName": "Seeded", "Value": len(rates), "Unit": "Count"}],
        )
    except Exception:
        pass

    if os.environ.get("SEED_KB"):
        with open("/tmp/rates.json", "w") as f:
            json.dump(
                [{"code": r["code"], "rate": float(r.get("rate_usd", r.get("rate", 0)))} for r in rates],
                f,
            )
        client("s3").upload_file("/tmp/rates.json", "kb-bucket", "rates.json")

    return {"seeded": len(rates)}
