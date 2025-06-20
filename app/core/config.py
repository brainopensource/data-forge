"""
Centralized high-performance configuration for Data Forge API.
All performance-critical settings are defined here.
"""
from typing import Optional
import os

# ============================================================================
# PERFORMANCE CONFIGURATIONS
# ============================================================================

# I/O Performance Settings
PARQUET_ROW_GROUP_SIZE = 1000000  # Optimized for your data sizes
POLARS_INFER_SCHEMA_LENGTH = 20   # Minimal schema inference for speed
DEFAULT_BATCH_SIZE = 900000       # Optimized batch size
ULTRA_FAST_INFER_LENGTH = 50      # Minimal inference for ultra-fast writes

# Compression Settings
SKIP_STATISTICS = True            # Skip Parquet statistics for speed
USE_ZSTD_COMPRESSION = True       # Fast compression with good ratio
DEFAULT_COMPRESSION = "zstd"      # Default compression type

# Memory and Threading
DUCKDB_THREADS = 8               # Multi-threading for DuckDB
DUCKDB_MEMORY_LIMIT = "8192MB"   # Memory allocation (DuckDB expects MB format)
ARROW_MEMORY_POOL_SIZE = "4096MB" # Arrow memory pool

# Data Configuration
N_ROWS = 1000                    # Default row count for file templates
DATA_DIR = "data"                # Data directory
TEMP_DIR = "temp"                # Temporary directory

# File Templates
PARQUET_FILE_TEMPLATE = "{schema_name}_data_{N_ROWS}K.parquet"
FEATHER_FILE_TEMPLATE = "{schema_name}_data_{N_ROWS}K.feather"

# Server Configuration
API_PORT = 8080                  # API port as requested by user
API_HOST = "0.0.0.0"            # API host

# ============================================================================
# WRITE OPTIMIZATION SETTINGS
# ============================================================================

# Ultra-fast write configurations
ULTRA_FAST_WRITE_CONFIG = {
    "compression": "zstd",
    "row_group_size": 25000,
    "use_pyarrow": True,
    "statistics": False,
    "infer_schema_length": ULTRA_FAST_INFER_LENGTH
}

# Standard write configurations
STANDARD_WRITE_CONFIG = {
    "compression": "snappy",
    "row_group_size": PARQUET_ROW_GROUP_SIZE,
    "use_pyarrow": True,
    "statistics": True,
    "infer_schema_length": POLARS_INFER_SCHEMA_LENGTH
}

# ============================================================================
# VALIDATION SETTINGS
# ============================================================================

# Validation performance settings
MAX_VALIDATION_ERRORS = 100      # Limit validation errors for performance
VALIDATION_BATCH_SIZE = 10000    # Batch size for validation
SKIP_VALIDATION_THRESHOLD = 1000000  # Skip validation for very large datasets

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
    # Create schema-specific directory if it doesn't exist
    schema_dir = os.path.join(DATA_DIR, schema_name)
    os.makedirs(schema_dir, exist_ok=True)
    return os.path.join(schema_dir, filename)

def get_write_feather_path(schema_name: str, suffix: str = "") -> str:
    """Generate path for feather write operations with optional suffix."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{schema_name}_write_{timestamp}{suffix}.feather"
    # Create schema-specific directory if it doesn't exist
    schema_dir = os.path.join(DATA_DIR, schema_name)
    os.makedirs(schema_dir, exist_ok=True)
    return os.path.join(schema_dir, filename)

def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB."""
    return os.path.getsize(file_path) / (1024 * 1024)

def ensure_directories():
    """Ensure all required directories exist."""
    directories = [DATA_DIR, TEMP_DIR]
    for directory in directories:
        os.makedirs(directory, exist_ok=True) 