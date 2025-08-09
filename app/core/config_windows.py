"""
Windows-Optimized Configuration for Data Forge API.
This module adapts to the host machine using centralized Settings and
Windows-specific sane defaults. Prefer environment variables over hardcoding.
"""
from typing import Optional
import os
import multiprocessing
from app.config.settings import settings
import re

# ============================================================================
# WINDOWS-SPECIFIC PERFORMANCE CONFIGURATIONS
# ============================================================================

def _parse_size_to_mb(value: str | int | float) -> int:
    """Parse size strings like '8GB'/'8192MB' or numeric to integer MB."""
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).strip().upper()
    m = re.match(r"^(\d+(?:\.\d+)?)\s*(GB|G|MB|M)?$", s)
    if not m:
        return 8192  # fallback 8GB
    num = float(m.group(1))
    unit = m.group(2) or "MB"
    if unit in ("GB", "G"):
        return int(num * 1024)
    return int(num)

# Windows System Information
WINDOWS_CPU_COUNT = multiprocessing.cpu_count()
# Use env override if provided; otherwise default to using all cores for parity
if os.environ.get("DUCKDB_THREADS"):
    try:
        val = int(settings.duckdb_threads)
        WINDOWS_OPTIMAL_THREADS = val if val > 0 else WINDOWS_CPU_COUNT
    except Exception:
        WINDOWS_OPTIMAL_THREADS = WINDOWS_CPU_COUNT
else:
    WINDOWS_OPTIMAL_THREADS = WINDOWS_CPU_COUNT

# I/O Performance Settings (Windows-optimized)
PARQUET_ROW_GROUP_SIZE = 1000000  # Optimized for Windows I/O patterns
POLARS_INFER_SCHEMA_LENGTH = 20   # Minimal schema inference for speed
DEFAULT_BATCH_SIZE = 900000       # Optimized batch size for Windows memory
ULTRA_FAST_INFER_LENGTH = 50      # Minimal inference for writes
# Align with high-performance defaults (1M rows per chunk)
WINDOWS_STREAMING_CHUNK_SIZE = 1_000_000

# Compression Settings (Windows-optimized)
SKIP_STATISTICS = True            # Skip Parquet statistics for speed
USE_ZSTD_COMPRESSION = True       # Fast compression with good ratio
DEFAULT_COMPRESSION = "zstd"      # Default compression type
WINDOWS_COMPRESSION_LEVEL = 3     # Balanced compression for Windows

# Memory and Threading (Windows-specific)
DUCKDB_THREADS = WINDOWS_OPTIMAL_THREADS
# Normalize memory limits from Settings (string like '8GB', etc.) to MB integer
DUCKDB_MEMORY_LIMIT_MB = _parse_size_to_mb(settings.duckdb_memory_limit)
# Backward-compatible export used by existing routes (represents MB integer)
DUCKDB_MEMORY_LIMIT = DUCKDB_MEMORY_LIMIT_MB
ARROW_MEMORY_POOL_SIZE = settings.arrow_memory_pool_size
WINDOWS_MEMORY_OPTIMIZATION = True

# Windows-specific I/O settings
WINDOWS_IO_COMPLETION_PORTS = True  # Use Windows I/O completion ports
WINDOWS_OVERLAPPED_IO = True        # Enable overlapped I/O
WINDOWS_FILE_BUFFERING = "optimal"  # Optimal file buffering for Windows

# Data Configuration (Windows paths) - Centralized in settings.py
DATA_DIR = settings.data_dir
TABLES_DIR = settings.tables_dir
TESTS_DIR = settings.tests_dir
TEMP_DIR = settings.temp_dir
CACHE_DIR = settings.cache_dir
SCHEMAS_DIR = settings.schemas_dir

# File Templates
PARQUET_FILE_TEMPLATE = "{schema_name}_data_{N_ROWS}K.parquet"
FEATHER_FILE_TEMPLATE = "{schema_name}_data_{N_ROWS}K.feather"

# Server/Data Configuration sourced from Settings
API_PORT = settings.api_port
API_HOST = settings.api_host
N_ROWS = settings.n_rows

# ============================================================================
# WINDOWS WRITE OPTIMIZATION SETTINGS
# ============================================================================

# Windows-optimized write configurations
WINDOWS_ULTRA_FAST_WRITE_CONFIG = {
    "compression": "zstd",
    "compression_level": WINDOWS_COMPRESSION_LEVEL,
    "row_group_size": 25000,
    "use_pyarrow": True,
    "statistics": False,
    "infer_schema_length": ULTRA_FAST_INFER_LENGTH,
    "use_compliant_nested_type": False,  # Faster nested type handling
    "use_legacy_dataset": False,         # Use new dataset API
    "memory_map": True,                  # Enable memory mapping on Windows
    "pre_buffer": True,                  # Pre-buffer for Windows I/O
}

# Windows-optimized standard write configurations
WINDOWS_STANDARD_WRITE_CONFIG = {
    "compression": "snappy",
    "row_group_size": PARQUET_ROW_GROUP_SIZE,
    "use_pyarrow": True,
    "statistics": True,
    "infer_schema_length": POLARS_INFER_SCHEMA_LENGTH,
    "memory_map": True,
    "pre_buffer": True,
}

# Windows-specific Polars configurations
# Align with high-performance initialization to avoid overriding optimal values
WINDOWS_POLARS_CONFIG = {
    "streaming_chunk_size": WINDOWS_STREAMING_CHUNK_SIZE,  # 1M rows per chunk
    "tbl_rows": -1,                   # No display limits for performance debugging
    "tbl_cols": -1,
    "tbl_width_chars": 1000,
    "verbose": False,                 # Disable verbose output for performance
}

# Windows-specific DuckDB configurations
WINDOWS_DUCKDB_CONFIG = {
    "threads": DUCKDB_THREADS,
    # Values are MB integers here; startup will append 'MB' and quote as required
    "memory_limit": DUCKDB_MEMORY_LIMIT_MB,
    "max_memory": DUCKDB_MEMORY_LIMIT_MB,
    "temp_directory": TEMP_DIR,
    "enable_progress_bar": False,
    "preserve_insertion_order": False,
}

# ============================================================================
# WINDOWS VALIDATION SETTINGS
# ============================================================================

# Validation performance settings (Windows-optimized)
MAX_VALIDATION_ERRORS = 100      # Limit validation errors for performance
VALIDATION_BATCH_SIZE = 10000    # Batch size for validation
SKIP_VALIDATION_THRESHOLD = 1000000  # Skip validation for very large datasets
WINDOWS_VALIDATION_THREADS = min(4, WINDOWS_OPTIMAL_THREADS)  # Limit validation threads

# ============================================================================
# WINDOWS-SPECIFIC HELPER FUNCTIONS
# ============================================================================

def get_parquet_path(schema_name: str) -> str:
    """Generate standard parquet file path with Windows path separators."""
    return os.path.join(TABLES_DIR, PARQUET_FILE_TEMPLATE.format(
        schema_name=schema_name, N_ROWS=N_ROWS
    ))

def get_write_parquet_path(schema_name: str, suffix: str = "") -> str:
    """Generate Windows-optimized path for write operations with optional suffix."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{schema_name}_write_{timestamp}{suffix}.parquet"
    # Create schema-specific directory if it doesn't exist
    schema_dir = os.path.join(TABLES_DIR, schema_name)
    os.makedirs(schema_dir, exist_ok=True)
    return os.path.join(schema_dir, filename)

def get_write_feather_path(schema_name: str, suffix: str = "") -> str:
    """Generate Windows-optimized path for feather write operations with optional suffix."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{schema_name}_write_{timestamp}{suffix}.feather"
    # Create schema-specific directory if it doesn't exist
    schema_dir = os.path.join(TABLES_DIR, schema_name)
    os.makedirs(schema_dir, exist_ok=True)
    return os.path.join(schema_dir, filename)

def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB."""
    try:
        return os.path.getsize(file_path) / (1024 * 1024)
    except OSError:
        return 0.0

def ensure_directories():
    """Ensure all required directories exist with Windows-optimized creation."""
    directories = [
        settings.data_dir, 
        settings.tables_dir, 
        settings.tests_dir, 
        settings.temp_dir, 
        settings.cache_dir, 
        settings.schemas_dir
    ]
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
        except OSError as e:
            print(f"Warning: Could not create directory {directory}: {e}")

def get_windows_system_info():
    """Get Windows-specific system information for optimization."""
    import platform
    import psutil
    
    try:
        return {
            "os": platform.system(),
            "os_version": platform.version(),
            "architecture": platform.architecture()[0],
            "processor": platform.processor(),
            "cpu_count": WINDOWS_CPU_COUNT,
            "optimal_threads": WINDOWS_OPTIMAL_THREADS,
            "total_memory_gb": round(psutil.virtual_memory().total / (1024**3), 2),
            "available_memory_gb": round(psutil.virtual_memory().available / (1024**3), 2),
            "disk_usage": psutil.disk_usage('.').percent,
        }
    except Exception as e:
        return {"error": f"Could not get system info: {e}"}

def apply_windows_optimizations():
    """Apply Windows-specific optimizations."""
    import polars as pl
    
    # Apply Polars Windows optimizations
    for key, value in WINDOWS_POLARS_CONFIG.items():
        try:
            getattr(pl.Config, f"set_{key}")(value)
        except AttributeError:
            pass  # Skip if setting doesn't exist
    
    # Set Windows-specific environment variables for performance
    os.environ['POLARS_MAX_THREADS'] = str(WINDOWS_OPTIMAL_THREADS)
    os.environ['RAYON_NUM_THREADS'] = str(WINDOWS_OPTIMAL_THREADS)
    
    return True 