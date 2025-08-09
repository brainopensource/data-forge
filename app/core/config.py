"""
Centralized high-performance configuration for Data Forge API.
This module now proxies configuration to app.config.settings.Settings,
reducing duplication and hardcoding while keeping backwards-compatible names.
"""
from typing import Optional
import os

from app.config.settings import settings

# ============================================================================
# PERFORMANCE CONFIGURATIONS (proxied to Settings)
# ============================================================================

# I/O Performance Settings
PARQUET_ROW_GROUP_SIZE = settings.parquet_row_group_size
POLARS_INFER_SCHEMA_LENGTH = settings.polars_infer_schema_length
DEFAULT_BATCH_SIZE = settings.default_batch_size
ULTRA_FAST_INFER_LENGTH = settings.ultra_fast_infer_length

# Compression Settings (kept as sane defaults; override via env if needed)
SKIP_STATISTICS = True
USE_ZSTD_COMPRESSION = True
DEFAULT_COMPRESSION = "zstd"

# Memory and Threading
DUCKDB_THREADS = settings.duckdb_threads
DUCKDB_MEMORY_LIMIT = settings.duckdb_memory_limit
ARROW_MEMORY_POOL_SIZE = settings.arrow_memory_pool_size

# Data Configuration
N_ROWS = settings.n_rows
DATA_DIR = settings.data_dir
TEMP_DIR = settings.temp_dir

# File Templates
PARQUET_FILE_TEMPLATE = "{schema_name}_data_{N_ROWS}K.parquet"
FEATHER_FILE_TEMPLATE = "{schema_name}_data_{N_ROWS}K.feather"

# Server Configuration
API_PORT = settings.api_port
API_HOST = settings.api_host

# ============================================================================
# WRITE OPTIMIZATION SETTINGS
# ============================================================================

ULTRA_FAST_WRITE_CONFIG = {
    "compression": "zstd",
    "row_group_size": 25000,
    "use_pyarrow": True,
    "statistics": False,
    "infer_schema_length": ULTRA_FAST_INFER_LENGTH,
}

STANDARD_WRITE_CONFIG = {
    "compression": "snappy",
    "row_group_size": PARQUET_ROW_GROUP_SIZE,
    "use_pyarrow": True,
    "statistics": True,
    "infer_schema_length": POLARS_INFER_SCHEMA_LENGTH,
}

# ============================================================================
# VALIDATION SETTINGS
# ============================================================================

MAX_VALIDATION_ERRORS = settings.max_validation_errors
VALIDATION_BATCH_SIZE = settings.validation_batch_size
SKIP_VALIDATION_THRESHOLD = settings.skip_validation_threshold

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_parquet_path(schema_name: str) -> str:
    """Generate standard parquet file path."""
    return os.path.join(DATA_DIR, PARQUET_FILE_TEMPLATE.format(
        schema_name=schema_name, N_ROWS=N_ROWS
    ))


def get_write_parquet_path(schema_name: str, suffix: str = "") -> str:
    """Generate path for write operations with optional suffix."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{schema_name}_write_{timestamp}{suffix}.parquet"
    schema_dir = os.path.join(DATA_DIR, schema_name)
    os.makedirs(schema_dir, exist_ok=True)
    return os.path.join(schema_dir, filename)


def get_write_feather_path(schema_name: str, suffix: str = "") -> str:
    """Generate path for feather write operations with optional suffix."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{schema_name}_write_{timestamp}{suffix}.feather"
    schema_dir = os.path.join(DATA_DIR, schema_name)
    os.makedirs(schema_dir, exist_ok=True)
    return os.path.join(schema_dir, filename)


def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB."""
    try:
        return os.path.getsize(file_path) / (1024 * 1024)
    except OSError:
        return 0.0


def ensure_directories():
    """Ensure all required directories exist."""
    directories = [DATA_DIR, settings.tables_dir, settings.schemas_dir, TEMP_DIR, settings.cache_dir]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)