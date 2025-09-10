import os, logging, json, statistics
from layers.common.python.common import client, resource

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

TABLE = os.environ.get("MWO_TABLE_NAME","mwo-rates")
VARIANCE_THRESH = 0.05

def _load_rates():
    try:
        ddb = resource("dynamodb"); table = ddb.Table(TABLE)
        resp = table.scan(); items = resp.get("Items", [])
        rates = {i["code"]: float(i["rate"]) for i in items if "code" in i and "rate" in i}
        return rates or {"RS":70.0, "GL":40.0}
    except Exception as e:
        logger.warning("Rate table scan failed: %s", e)
        return {"RS":70.0, "GL":40.0}

def _local_anomaly_scores(values):
    try:
        import numpy as np
        from sklearn.ensemble import IsolationForest
        X = np.array(values).reshape(-1,1)
        model = IsolationForest(contamination=0.1, random_state=42).fit(X)
        scores = -model.decision_function(X)
        return [float(s) for s in scores]
    except Exception as e:
        logger.warning("IsolationForest fallback: %s", e)
        if not values: return []
        m = statistics.mean(values); st = statistics.pstdev(values) or 1.0
        return [abs((v-m)/st) for v in values]

def _sagemaker_scores(values):
    ep = os.environ.get("SM_ENDPOINT")
    if not ep: return None
    try:
        sm = client("sagemaker-runtime")
        body = json.dumps({"instances":[{"features":[v]} for v in values]})
        resp = sm.invoke_endpoint(EndpointName=ep, Body=body, ContentType="application/json")
        out = resp["Body"].read().decode("utf-8"); parsed = json.loads(out)
        return parsed.get("scores")
    except Exception as e:
        logger.warning("SageMaker invoke failed: %s", e); return None

def compare_data(extracted):
    rates = _load_rates()
    flags = []; savings = 0.0
    labor = extracted.get("labor", [])
    amounts = [float(i.get("total", 0)) for i in labor if i.get("total") is not None]
    sm_scores = _sagemaker_scores(amounts) or _local_anomaly_scores(amounts)

    seen = set()
    for idx, item in enumerate(labor):
        name = item.get("name") or f"worker{idx}"
        code = (item.get("code") or "")[:2].upper()
        rate = float(item.get("rate") or 0)
        hours = float(item.get("total_hours") or 0)
        total = float(item.get("total") or 0)

        key = (name.lower(), code, rate, hours, total)
        if key in seen:
            flags.append({"type":"duplicate_line","item": item})
        else:
            seen.add(key)

        if rate < 0 or hours < 0 or total < 0:
            flags.append({"type":"negative_value","item": item})

        if hours > 40:
            flags.append({"type":"overtime_gt_40","hours": hours, "item": item})

        expected = rates.get(code)
        if expected:
            diff = (rate - expected) / expected
            if diff > VARIANCE_THRESH:
                flags.append({"type":"rate_high_vs_mwo","code": code, "expected": expected, "seen": rate})
                potential = (rate - expected) * hours
                savings += max(0.0, potential)

        score = sm_scores[idx] if idx < len(sm_scores) else 0.0
        if score > 0.6:
            flags.append({"type":"anomaly","score": score, "item": item})

    if extracted.get("total") is not None and sum(amounts) > extracted["total"] * 1.5:
        flags.append({"type":"distribution_mismatch","sum_labor": sum(amounts), "reported_total": extracted["total"]})

    if savings == 0 and flags:
        savings = sum(amounts) * 0.10

    return {"flags": flags, "estimated_savings": round(savings, 2), "rates_used": rates}

def compare_handler(event, context):
    return compare_data(event)
