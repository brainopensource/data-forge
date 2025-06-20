"""
Windows-Optimized Configuration for Data Forge API.
All performance-critical settings optimized for Windows systems.
"""
from typing import Optional
import os
import multiprocessing

# ============================================================================
# WINDOWS-SPECIFIC PERFORMANCE CONFIGURATIONS
# ============================================================================

# Windows System Information
WINDOWS_CPU_COUNT = multiprocessing.cpu_count()
WINDOWS_OPTIMAL_THREADS = max(1, WINDOWS_CPU_COUNT - 1)  # Reserve 1 core for OS

# I/O Performance Settings (Windows-optimized)
PARQUET_ROW_GROUP_SIZE = 1000000  # Optimized for Windows I/O patterns
POLARS_INFER_SCHEMA_LENGTH = 20   # Minimal schema inference for speed
DEFAULT_BATCH_SIZE = 900000       # Optimized batch size for Windows memory
ULTRA_FAST_INFER_LENGTH = 50      # Minimal inference for ultra-fast writes
WINDOWS_STREAMING_CHUNK_SIZE = 50000  # Optimized for Windows memory patterns

# Compression Settings (Windows-optimized)
SKIP_STATISTICS = True            # Skip Parquet statistics for speed
USE_ZSTD_COMPRESSION = True       # Fast compression with good ratio
DEFAULT_COMPRESSION = "zstd"      # Default compression type
WINDOWS_COMPRESSION_LEVEL = 3     # Balanced compression for Windows

# Memory and Threading (Windows-specific)
DUCKDB_THREADS = WINDOWS_OPTIMAL_THREADS  # Use optimal thread count for Windows
DUCKDB_MEMORY_LIMIT = "8192MB"   # Memory allocation (DuckDB expects MB format)
ARROW_MEMORY_POOL_SIZE = "4096MB" # Arrow memory pool
WINDOWS_MEMORY_OPTIMIZATION = True  # Enable Windows memory optimizations

# Windows-specific I/O settings
WINDOWS_IO_COMPLETION_PORTS = True  # Use Windows I/O completion ports
WINDOWS_OVERLAPPED_IO = True        # Enable overlapped I/O
WINDOWS_FILE_BUFFERING = "optimal"  # Optimal file buffering for Windows

# Data Configuration (Windows paths)
DATA_DIR = "data"                    # Data directory
TEMP_DIR = "temp"                    # Temporary directory
CACHE_DIR = os.path.join(DATA_DIR, "cache")  # Cache directory
SCHEMAS_DIR = os.path.join(DATA_DIR, "schemas")  # Schemas directory

# File Templates
PARQUET_FILE_TEMPLATE = "{schema_name}_data_{N_ROWS}K.parquet"
FEATHER_FILE_TEMPLATE = "{schema_name}_data_{N_ROWS}K.feather"

# Server Configuration
API_PORT = 8080                  # API port as requested by user
API_HOST = "127.0.0.1"          # Local-first deployment
N_ROWS = 1000                    # Default row count for file templates

# ============================================================================
# WINDOWS ULTRA-FAST WRITE OPTIMIZATION SETTINGS
# ============================================================================

# Windows-optimized ultra-fast write configurations
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
WINDOWS_POLARS_CONFIG = {
    "streaming_chunk_size": WINDOWS_STREAMING_CHUNK_SIZE,
    "fmt_str_lengths": 50,
    "tbl_rows": 100,
    "tbl_cols": 20,
    "tbl_width_chars": 120,
    "verbose": False,  # Disable verbose output for performance
}

# Windows-specific DuckDB configurations
WINDOWS_DUCKDB_CONFIG = {
    "threads": DUCKDB_THREADS,
    "memory_limit": "8192MB",
    "max_memory": "8192MB",
    "temp_directory": TEMP_DIR,
    "enable_progress_bar": False,
    "threads_per_task": 2,
    "preserve_insertion_order": False,  # Better performance
    "enable_optimizer": True,
    "enable_profiling": False,  # Disable for production performance
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
    return os.path.join(DATA_DIR, PARQUET_FILE_TEMPLATE.format(
        schema_name=schema_name, N_ROWS=N_ROWS
    ))

def get_write_parquet_path(schema_name: str, suffix: str = "") -> str:
    """Generate Windows-optimized path for write operations with optional suffix."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{schema_name}_write_{timestamp}{suffix}.parquet"
    # Create schema-specific directory if it doesn't exist
    schema_dir = os.path.join(DATA_DIR, schema_name)
    os.makedirs(schema_dir, exist_ok=True)
    return os.path.join(schema_dir, filename)

def get_write_feather_path(schema_name: str, suffix: str = "") -> str:
    """Generate Windows-optimized path for feather write operations with optional suffix."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{schema_name}_write_{timestamp}{suffix}.feather"
    # Create schema-specific directory if it doesn't exist
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
    """Ensure all required directories exist with Windows-optimized creation."""
    directories = [DATA_DIR, TEMP_DIR, CACHE_DIR, SCHEMAS_DIR]
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