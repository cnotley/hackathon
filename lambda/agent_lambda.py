import logging, os, re, json, uuid
from functools import lru_cache
from .extraction_lambda import extract_data
from .comparison_lambda import compare_data, _load_rates
from layers.common.python.common import client

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

@lru_cache(maxsize=128)
def _cached_extract(local_path):
    return extract_data(local_path=local_path)


@lru_cache(maxsize=128)
def get_rate_for_labor_type(labor_type: str):
    """Lookup a standard rate for the given labor code."""
    if not re.fullmatch(r"^[A-Z]{2,4}$", labor_type or ""):
        raise ValueError("invalid labor_type")
    rates = _load_rates()
    return rates.get(labor_type.upper())


def _invoke_lambda_async(function_name, payload):
    try:
        lam = client("lambda")
        lam.invoke(
            FunctionName=function_name,
            InvocationType="Event",
            Payload=json.dumps(payload).encode("utf-8"),
        )
    except Exception as e:  # pragma: no cover - best effort
        logger.warning("async invoke failed: %s", e)


SESSION_CACHE = {}


def invoke_agent(event, context=None):
    """Entry point for the Bedrock Agent. Falls back to local processing."""

    session_id = event.get("session_id") or str(uuid.uuid4())
    action = event.get("action", "audit")
    if action != "audit":
        return {"message": "Unsupported action", "action": action, "session": {"id": session_id}}

    bucket = event.get("bucket")
    key = event.get("key")
    local = event.get("local_path")

    # Attempt to use Bedrock Agent first
    if os.environ.get("USE_BEDROCK_AGENT", "").lower() == "true":
        try:
            agent = client("bedrock-agent-runtime")
            agent.invoke_agent(
                agentId=os.environ.get("AGENT_ID", "placeholder"),
                sessionId=session_id,
                inputText=json.dumps(event),
            )
        except Exception as e:  # pragma: no cover - falls back to local pipeline
            logger.warning("bedrock agent invocation failed: %s", e)

    if bucket and key and os.environ.get("ASYNC_PIPELINE", "").lower() == "true":
        _invoke_lambda_async(os.environ.get("AGENT_LAMBDA_NAME", "agent"), event)
        return {"session": {"id": session_id}, "status": "submitted"}

    if local:
        extracted = _cached_extract(local)
    else:
        extracted = extract_data(bucket, key, None)
    comparison = compare_data(extracted)

    SESSION_CACHE[session_id] = {"extracted": extracted, "comparison": comparison}
    return {"session": {"id": session_id}, "extracted": extracted, "comparison": comparison}
