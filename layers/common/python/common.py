import os, json, logging, math, re
from collections import Counter
from decimal import Decimal
import boto3
try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency
    def load_dotenv(*args, **kwargs):
        return False

load_dotenv()

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

def is_localstack():
    return bool(os.environ.get("LOCALSTACK_URL"))

def client(service, region=None):
    kw = {}
    if region:
        kw["region_name"] = region
    if is_localstack():
        kw["endpoint_url"] = os.environ["LOCALSTACK_URL"]
    return boto3.client(service, **kw)

def resource(service, region=None):
    kw = {}
    if region:
        kw["region_name"] = region
    if is_localstack():
        kw["endpoint_url"] = os.environ["LOCALSTACK_URL"]
    return boto3.resource(service, **kw)


def with_error_handling(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error: {str(e)}", exc_info=True)
            try:
                cw = client("cloudwatch")
                cw.put_metric_data(
                    Namespace="AuditError",
                    MetricData=[{"MetricName": "Errors", "Value": 1, "Unit": "Count"}],
                )
            except Exception:
                pass
            raise
    return wrapper

def json_dumps(obj):
    def default(o):
        if isinstance(o, Decimal):
            return float(o)
    return json.dumps(obj, default=default)

def chunk_text(text, words_per_chunk=300, overlap_ratio=0.2):
    words = text.split()
    if not words:
        return []
    step = max(1, int(words_per_chunk * (1 - overlap_ratio)))
    chunks = []
    i = 0
    while i < len(words):
        chunk = " ".join(words[i:i+words_per_chunk])
        meta = {"start": i, "end": min(i+words_per_chunk, len(words))}
        chunks.append({"text": chunk, "meta": meta})
        i += step
    return chunks

def _bow_vector(text):
    tokens = re.findall(r"[a-zA-Z0-9$\.]+", text.lower())
    return Counter(tokens)

def _cosine(c1: Counter, c2: Counter):
    if not c1 or not c2:
        return 0.0
    inter = set(c1) & set(c2)
    num = sum(c1[t]*c2[t] for t in inter)
    den = (sum(v*v for v in c1.values()))**0.5 * (sum(v*v for v in c2.values()))**0.5
    return float(num/den) if den else 0.0

def embed_text(text):
    try:
        if os.environ.get("USE_BEDROCK","").lower() == "true":
            br = client("bedrock-runtime", region=os.environ.get("BEDROCK_REGION","us-east-1"))
            body = json.dumps({"inputText": text})
            out = br.invoke_model(modelId="amazon.titan-embed-text-v1", body=body)
            payload = json.loads(out["body"].read())
            return payload.get("embedding", [])
    except Exception as e:
        logger.warning("Bedrock embed fallback due to: %s", e)
    return _bow_vector(text)

def cosine_similarity(a,b):
    if isinstance(a, Counter) and isinstance(b, Counter):
        return _cosine(a,b)
    n = min(len(a or []), len(b or []))
    if n == 0:
        return 0.0
    num = sum((a[i]*b[i]) for i in range(n))
    da = sum(x*x for x in a)**0.5
    db = sum(x*x for x in b)**0.5
    return float(num/(da*db)) if da and db else 0.0

DEFAULT_SYNONYMS = {
    "labor": ["labour","workers","manpower","crew"],
    "consumables": ["consum","materials"],
    "equipment": ["equip","machines"],
    "subcontractors": ["sub","subs"],
    "rate": ["cost per hour","hourly","billing rate"],
    "general labor": ["gl","gen labor","helper"],
    "restoration specialist": ["rs","tech","specialist"],
}

def semantic_map(term, candidates, threshold=0.8):
    emb_t = embed_text(term)
    best = None
    best_sim = 0.0
    for c in candidates:
        emb_c = embed_text(c)
        sim = cosine_similarity(emb_t, emb_c)
        if sim > best_sim:
            best_sim, best = sim, c
    if best_sim >= threshold:
        return best, best_sim
    t = term.lower().strip()
    for k, vals in DEFAULT_SYNONYMS.items():
        if t == k or t in vals:
            for c in candidates:
                if c.lower().startswith(k):
                    return c, 0.81
    return None, best_sim
