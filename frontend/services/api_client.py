"""
API Client for DataForge Backend
"""
from typing import Any, Dict, List, Optional, Tuple
import requests

try:
	import pyarrow as pa
	import pyarrow.ipc as pa_ipc
except ImportError:
	pa = None
	pa_ipc = None

class ApiClient:
	def __init__(self, base_url: str):
		self.base_url = base_url.rstrip("/")

	# Commands
	def write_polars(self, schema_name: str, data: List[Dict[str, Any]], compression: str = "zstd") -> Dict[str, Any]:
		url = f"{self.base_url}/write/polars/{schema_name}"
		payload = {"data": data, "compression": compression}
		resp = requests.post(url, json=payload, timeout=600)
		resp.raise_for_status()
		return resp.json()

	# Queries
	def read_polars(self, schema_name: str) -> Tuple[Optional[Any], int]:
		url = f"{self.base_url}/read/polars/{schema_name}"
		resp = requests.get(url, timeout=600)
		resp.raise_for_status()
		if pa is None or pa_ipc is None:
			raise ImportError("PyArrow is not installed. Cannot read Arrow IPC stream.")
		# Parse Arrow IPC stream
		reader = pa_ipc.open_stream(pa.BufferReader(resp.content))
		table = reader.read_all()
		return table, len(table)

	# Schemas
	def list_schema_families(self) -> List[str]:
		url = f"{self.base_url}/schemas/"
		resp = requests.get(url, timeout=60)
		resp.raise_for_status()
		return resp.json()

	def get_latest_schema(self, schema_name: str) -> Dict[str, Any]:
		url = f"{self.base_url}/schemas/{schema_name}/latest"
		resp = requests.get(url, timeout=60)
		resp.raise_for_status()
		return resp.json()

	def get_schema_versions(self, schema_name: str) -> List[int]:
		url = f"{self.base_url}/schemas/{schema_name}"
		resp = requests.get(url, timeout=60)
		resp.raise_for_status()
		return resp.json()

	def register_schema(self, schema_name: str, schema_definition: Dict[str, Any]) -> Dict[str, Any]:
		url = f"{self.base_url}/schemas/{schema_name}"
		resp = requests.post(url, json=schema_definition, timeout=60)
		resp.raise_for_status()
		return resp.json() if resp.content else {"status": resp.status_code}
