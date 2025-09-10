import os, logging, json, statistics, time
from layers.common.python.common import client, resource
from functools import lru_cache
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

TABLE = os.environ.get("MWO_TABLE_NAME", "mwo-rates")
VARIANCE_THRESH = 0.05

try:
    from forex_python.converter import CurrencyRates
except Exception:  # pragma: no cover - library optional during tests
    CurrencyRates = None

try:
    from statsmodels.stats.diagnostic import normal_ad
except Exception:  # pragma: no cover
    normal_ad = None

@lru_cache(maxsize=128)
def _load_rates():
    try:
        ddb = resource("dynamodb"); table = ddb.Table(TABLE)
        resp = table.scan(); items = resp.get("Items", [])
        rates = {i["code"]: float(i["rate"]) for i in items if "code" in i and "rate" in i}
        return rates or {"RS":70.0, "GL":43.0, "PM":115.0, "SRPM":135.0, "PCA":57.0}
    except Exception as e:
        logger.warning("Rate table scan failed: %s", e)
        return {"RS":70.0, "GL":43.0, "PM":115.0, "SRPM":135.0, "PCA":57.0}

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
    if not ep:
        return None
    delay = 1.0
    for attempt in range(3):
        try:
            sm = client("sagemaker-runtime")
            body = json.dumps({"instances": [{"features": [v]} for v in values]})
            resp = sm.invoke_endpoint(
                EndpointName=ep, Body=body, ContentType="application/json"
            )
            out = resp["Body"].read().decode("utf-8")
            parsed = json.loads(out)
            return parsed.get("scores")
        except ClientError as e:
            logger.warning("SageMaker invoke failed (%s): %s", attempt + 1, e)
            time.sleep(delay)
            delay *= 1.5
        except Exception as e:  # pragma: no cover
            logger.warning("SageMaker invoke failed: %s", e)
            break
    return None

def compare_data(extracted):
    rates = _load_rates()
    flags = []
    savings = 0.0
    formatting = []

    currency = extracted.get("currency", "USD")
    rate_conv = 1.0
    if currency and currency.upper() != "USD" and CurrencyRates:
        try:
            rate_conv = CurrencyRates().get_rate(currency, "USD")
        except Exception as e:
            logger.warning("currency conversion failed: %s", e)
            flags.append({"type": "currency_conversion_failed", "currency": currency})

    labor = extracted.get("labor", [])
    amounts = [float(i.get("total", 0)) * rate_conv for i in labor if i.get("total") is not None]
    sm_scores = _sagemaker_scores(amounts) or _local_anomaly_scores(amounts)

    seen = set()
    for idx, item in enumerate(labor):
        name = item.get("name") or f"worker{idx}"
        code = (item.get("code") or "")[:2].upper()
        rate = float(item.get("rate") or 0)
        hours = float(item.get("total_hours") or 0)
        total = float(item.get("total") or 0) * rate_conv

        key = (name.lower(), code, rate, hours, total)
        if key in seen:
            flags.append({"type":"duplicate_line","item": item})
        else:
            seen.add(key)

        if rate < 0 or hours < 0 or total < 0:
            flags.append({"type": "negative_value", "item": item})
            raise ValueError("negative_value detected")

        if hours > 40:
            flags.append({"type":"overtime_gt_40","hours": hours, "item": item})

        expected = rates.get(code)
        if expected:
            diff = (rate - expected) / expected
            if diff > VARIANCE_THRESH:
                flags.append({"type": "rate_high_vs_mwo", "code": code, "expected": expected, "seen": rate})
                potential = (rate - expected) * hours
                savings += max(0.0, potential)
                formatting.append({"sheet": "Labor Export", "row": idx + 2, "severity": "high" if diff > 0.10 else "medium"})

        score = sm_scores[idx] if idx < len(sm_scores) else 0.0
        if score > 0.6:
            flags.append({"type":"anomaly","score": score, "item": item})

    reported_total = extracted.get("total")
    if reported_total is not None:
        reported_total = float(reported_total) * rate_conv
        if sum(amounts) > reported_total * 1.5:
            flags.append({"type": "distribution_mismatch", "sum_labor": sum(amounts), "reported_total": reported_total})

    if normal_ad and amounts:
        try:
            stat, p = normal_ad(amounts)
            if p < 0.05:
                flags.append({"type": "distribution_non_normal", "pvalue": p})
        except Exception as e:
            logger.warning("normality test failed: %s", e)

    if savings == 0 and flags:
        savings = sum(amounts) * 0.10

    return {"flags": flags, "estimated_savings": round(savings, 2), "rates_used": rates, "formatting": formatting}

def compare_handler(event, context):
    return compare_data(event)
