"""
Data Generation Service
"""
from typing import Any, Dict, List
from datetime import datetime, timedelta

class DataGenerator:
	@staticmethod
	def generate_sample_data(num_records: int) -> List[Dict[str, Any]]:
		"""Generates realistic sample data for well production."""
		data = []
		start_date = datetime(2023, 1, 1)
		for i in range(num_records):
			data.append({
				"api": f"API_{i % 100}",
				"date": (start_date + timedelta(days=i)).strftime("%Y-%m-%d"),
				"oil": 100.0 + i * 0.1,
				"gas": 5000.0 + i * 0.5,
				"water": 200.0 - i * 0.05,
			})
		return data
