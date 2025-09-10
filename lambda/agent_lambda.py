import logging
from functools import lru_cache
from .extraction_lambda import extract_data
from .comparison_lambda import compare_data

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

@lru_cache(maxsize=128)
def _cached_extract(local_path):
    return extract_data(local_path=local_path)


def invoke_agent(event, context=None):
    action = event.get("action","audit")
    if action != "audit":
        return {"message":"Unsupported action","action":action}
    bucket = event.get("bucket"); key = event.get("key"); local = event.get("local_path")
    if local:
        extracted = _cached_extract(local)
    else:
        extracted = extract_data(bucket, key, None)
    comparison = compare_data(extracted)
    return {"session":{"id": event.get("session_id") or "local"}, "extracted": extracted, "comparison": comparison}
