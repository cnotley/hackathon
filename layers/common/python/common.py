import logging
import os

import boto3

try:  # pragma: no cover - optional dependency for local runs
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - fallback if python-dotenv is unavailable
    def load_dotenv(*_args, **_kwargs):
        return False

load_dotenv()

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


def is_localstack() -> bool:
    return bool(os.environ.get("LOCALSTACK_URL"))


def client(service: str, region: str | None = None):
    kwargs: dict[str, str] = {}
    if region:
        kwargs["region_name"] = region
    if is_localstack():
        kwargs["endpoint_url"] = os.environ["LOCALSTACK_URL"]
    return boto3.client(service, **kwargs)


def resource(service: str, region: str | None = None):
    kwargs: dict[str, str] = {}
    if region:
        kwargs["region_name"] = region
    if is_localstack():
        kwargs["endpoint_url"] = os.environ["LOCALSTACK_URL"]
    return boto3.resource(service, **kwargs)


# Placeholder for future text embedding helpers.
# For the MVP we rely on deterministic rule-based logic, but a simple
# bag-of-words fallback can be reintroduced here if semantic search returns.
