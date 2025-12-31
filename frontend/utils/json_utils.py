"""
JSON formatting utilities
"""
import json
from typing import Any


def format_json(obj: Any) -> str:
    """Format object as pretty JSON string"""
    try:
        return json.dumps(obj, indent=2, default=str)
    except Exception:
        return str(obj)
