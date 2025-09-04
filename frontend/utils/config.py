"""
Application Configuration
"""
from pathlib import Path

class AppConfig:
	"""Fixed application configuration"""
	API_BASE_URL = "http://localhost:8080"
	FAVICON_PATH = Path(__file__).parent.parent.parent / "static" / "images" / "favicon.ico"
	DEFAULT_SCHEMA = "well_production"
	DEFAULT_RECORDS = "10000"
	DEFAULT_COMPRESSION = "zstd"
