"""
UI Helper functions
"""
import json
from typing import Any

def format_json(obj: Any) -> str:
	"""Formats a Python object into a pretty-printed JSON string."""
	try:
		return json.dumps(obj, indent=4, sort_keys=True)
	except Exception:
		return str(obj)
