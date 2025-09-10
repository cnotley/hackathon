import os, json, logging, re
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
