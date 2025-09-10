"""Helpers to normalize extracted JSON fields.

The function attempts to map various synonyms (e.g. "Rate" vs "Unit Price") to
canonical keys and extract additional entities using Amazon Comprehend when the
service is available.  It relies on the lightweight semantic helpers from the
shared layer but gracefully falls back when Bedrock/Comprehend are not
configured.
"""
from typing import Dict, Any
from layers.common.python.common import semantic_map, client

CANONICAL = ["Rate", "Unit Price", "Total", "Quantity"]


def normalize(data: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of ``data`` with field names normalized.

    Parameters
    ----------
    data: Dict[str, Any]
        Arbitrary JSON-like dictionary from extraction.
    """
    normalized = {}
    for key, value in data.items():
        mapped, score = semantic_map(key, CANONICAL)
        normalized[mapped or key] = value

    # attempt to use Comprehend to detect entities like people or quantities
    try:  # pragma: no cover - network call
        text = " ".join(f"{k}: {v}" for k, v in data.items())
        comp = client("comprehend")
        resp = comp.detect_entities(Text=text, LanguageCode="en")
        normalized["entities"] = [
            {"Text": e.get("Text"), "Type": e.get("Type")}
            for e in resp.get("Entities", [])
        ]
    except Exception:
        pass

    return normalized
