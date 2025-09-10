"""
Common utilities for Invoice Auditing File Ingestion Module

This module provides shared utilities and helper classes for file processing,
logging, S3 operations, and Step Functions integration.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any


def json_dumps(data: Dict[str, Any]) -> str:
    """Serialize data to JSON string."""
    return json.dumps(data, default=str)
