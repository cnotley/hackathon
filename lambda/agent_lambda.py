import logging
from .extraction_lambda import extract_data
from .comparison_lambda import compare_data

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

def invoke_agent(event, context=None):
    action = event.get("action","audit")
    if action != "audit":
        return {"message":"Unsupported action","action":action}
    bucket = event.get("bucket"); key = event.get("key"); local = event.get("local_path")
    extracted = extract_data(bucket, key, local)
    comparison = compare_data(extracted)
    return {"session":{"id": event.get("session_id") or "local"}, "extracted": extracted, "comparison": comparison}
