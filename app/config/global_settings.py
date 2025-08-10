"""
Global High-Performance Configuration for Data Forge API.
Single source of truth for all configuration values.

This module replaces all scattered config files with a centralized,
performance-optimized configuration system designed for SOTA APIs.
"""
import os
import sys
import multiprocessing
from typing import Dict, Any, Optional, Union
from pathlib import Path


# ============================================================================
# SYSTEM DETECTION AND OPTIMIZATION
# ============================================================================

def detect_system_config() -> Dict[str, Any]:
    """Detect system configuration for optimal performance."""
    cpu_count = multiprocessing.cpu_count()
    is_windows = sys.platform.startswith('win')
    
    # Reserve 1-2 cores for OS on multi-core systems
    optimal_threads = max(1, cpu_count - (2 if cpu_count > 4 else 1))
    
    return {
        'platform': 'windows' if is_windows else 'unix',
        'cpu_count': cpu_count,
        'optimal_threads': optimal_threads,
        'memory_gb': _get_system_memory_gb(),
        'is_windows': is_windows,
    }


def _get_system_memory_gb() -> float:
    """Get system memory in GB."""
    try:
        import psutil
        return round(psutil.virtual_memory().total / (1024**3), 1)
    except ImportError:
        # Fallback estimation
        return 8.0


# System configuration
SYSTEM = detect_system_config()


# ============================================================================
# CORE API CONFIGURATION
# ============================================================================

class APIConfig:
    """Core API server configuration."""
    # Server settings
    HOST: str = "0.0.0.0"
    PORT: int = 8080
    DEBUG: bool = False
    
    # Resource limits for high performance
    MAX_MEMORY_BUFFER_MB: int = min(8192, int(SYSTEM['memory_gb'] * 1024 * 0.7))  # 70% of system RAM
    PARALLEL_WORKER_THREADS: int = SYSTEM['optimal_threads']
    
    # Request handling
    MAX_REQUEST_SIZE_MB: int = 1024  # 1GB max request
    REQUEST_TIMEOUT_SECONDS: int = 300  # 5 minutes for large operations
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_DIR: str = "logs"


# ============================================================================
# DATA PROCESSING CONFIGURATION
# ============================================================================

class DataConfig:
    """High-performance data processing configuration."""
    
    # Directory structure
    DATA_DIR: str = "data"
    TABLES_DIR: str = os.path.join(DATA_DIR, "tables")
    SCHEMAS_DIR: str = os.path.join(DATA_DIR, "schemas")
    TESTS_DIR: str = os.path.join(DATA_DIR, "tests")
    CACHE_DIR: str = os.path.join(DATA_DIR, "cache")
    TEMP_DIR: str = "temp"
    
    # Default data parameters
    DEFAULT_N_ROWS: int = 1000
    
    # File templates
    PARQUET_FILE_TEMPLATE: str = "{schema_name}_data_{n_rows}K.parquet"
    FEATHER_FILE_TEMPLATE: str = "{schema_name}_data_{n_rows}K.feather"


# ============================================================================
# PERFORMANCE OPTIMIZATION CONFIGURATION
# ============================================================================

class PerformanceConfig:
    """Ultra-high performance settings optimized for 10M+ rows/second."""
    
    # Memory and threading
    DUCKDB_THREADS: int = SYSTEM['optimal_threads']
    DUCKDB_MEMORY_LIMIT_GB: int = min(8, int(SYSTEM['memory_gb'] * 0.6))  # 60% of system RAM
    
    # Adaptive Arrow memory allocation based on system capacity
    ARROW_MEMORY_POOL_SIZE_GB: int = (
        int(SYSTEM['memory_gb'] * 0.3) if SYSTEM['memory_gb'] < 15 else  # 30% for systems < 15GB RAM
        int(SYSTEM['memory_gb'] * 0.6)                                   # 60% for high-memory systems ≥ 15GB
    )
    
    # I/O optimization
    PARQUET_ROW_GROUP_SIZE: int = 1_000_000  # Optimal for both read/write performance
    DEFAULT_BATCH_SIZE: int = 900_000  # High-throughput batch processing
    STREAMING_CHUNK_SIZE: int = 1_000_000  # Large chunks for streaming operations
    
    # Schema inference (minimal for speed)
    POLARS_INFER_SCHEMA_LENGTH: int = 20
    ULTRA_FAST_INFER_LENGTH: int = 50
    
    # Compression settings
    DEFAULT_COMPRESSION: str = "zstd"
    FAST_COMPRESSION_LEVEL: int = 1  # Fastest zstd
    BALANCED_COMPRESSION_LEVEL: int = 3  # Balanced speed/ratio
    
    # Validation settings
    MAX_VALIDATION_ERRORS: int = 100
    VALIDATION_BATCH_SIZE: int = 10_000
    SKIP_VALIDATION_THRESHOLD: int = 1_000_000  # Skip validation for very large datasets


# ============================================================================
# WRITE OPTIMIZATION PROFILES
# ============================================================================

class WriteProfiles:
    """Optimized write configurations for different use cases."""
    
    ULTRA_FAST = {
        "compression": PerformanceConfig.DEFAULT_COMPRESSION,
        "compression_level": PerformanceConfig.FAST_COMPRESSION_LEVEL,
        "row_group_size": 25_000,  # Smaller groups for ultra-fast writes
        "use_pyarrow": True,
        "statistics": False,  # Skip statistics for speed
        "infer_schema_length": PerformanceConfig.ULTRA_FAST_INFER_LENGTH,
        "memory_map": True,
        "pre_buffer": True,
    }
    
    BALANCED = {
        "compression": "snappy",  # Good speed/compression balance
        "compression_level": None,
        "row_group_size": PerformanceConfig.PARQUET_ROW_GROUP_SIZE,
        "use_pyarrow": True,
        "statistics": True,
        "infer_schema_length": PerformanceConfig.POLARS_INFER_SCHEMA_LENGTH,
        "memory_map": True,
        "pre_buffer": True,
    }
    
    HIGH_COMPRESSION = {
        "compression": PerformanceConfig.DEFAULT_COMPRESSION,
        "compression_level": 6,  # Higher compression
        "row_group_size": PerformanceConfig.PARQUET_ROW_GROUP_SIZE,
        "use_pyarrow": True,
        "statistics": True,
        "infer_schema_length": PerformanceConfig.POLARS_INFER_SCHEMA_LENGTH,
        "memory_map": True,
        "pre_buffer": True,
    }


# ============================================================================
# LIBRARY-SPECIFIC CONFIGURATIONS
# ============================================================================

class LibraryConfig:
    """Configuration for external libraries."""
    
    @staticmethod
    def get_polars_config() -> Dict[str, Any]:
        """Get optimized Polars configuration."""
        return {
            "streaming_chunk_size": PerformanceConfig.STREAMING_CHUNK_SIZE,
            "fmt_str_lengths": 50,
            "tbl_rows": -1,  # No display limits
            "tbl_cols": -1,
            "tbl_width_chars": 1000,
            "verbose": False,
        }
    
    @staticmethod
    def get_duckdb_config() -> Dict[str, Any]:
        """Get optimized DuckDB configuration."""
        return {
            "threads": PerformanceConfig.DUCKDB_THREADS,
            "memory_limit": f"{PerformanceConfig.DUCKDB_MEMORY_LIMIT_GB}GB",
            "max_memory": f"{PerformanceConfig.DUCKDB_MEMORY_LIMIT_GB}GB",
            "temp_directory": DataConfig.TEMP_DIR,
            "enable_progress_bar": False,
            "preserve_insertion_order": False,
            # Fixed DuckDB settings based on current version compatibility
            # "enable_optimizer": True,  # Removed - not recognized in current DuckDB version
            "perfect_ht_threshold": 12,  # Fixed: was "perfect_hash_threshold" 
            # "checkpoint_threshold": "1GB",  # Removed - syntax not supported
            # "wal_autocheckpoint": 10000,   # Removed - unit format not supported
            # "enable_profiling": False,     # Removed - boolean not supported, needs format
            "enable_http_metadata_cache": True,
            "http_timeout": 120000,        # 2 minutes for large operations
            # "streaming_buffer_size": "128MB",  # Removed - syntax not supported
        }
    
    @staticmethod
    def get_arrow_config() -> Dict[str, Any]:
        """Get optimized Arrow configuration."""
        return {
            "memory_pool_size": f"{PerformanceConfig.ARROW_MEMORY_POOL_SIZE_GB}GB",
            "use_threads": True,
            "thread_count": PerformanceConfig.DUCKDB_THREADS,
            "io_thread_count": PerformanceConfig.DUCKDB_THREADS * 2,
        }
    
    @staticmethod
    def apply_advanced_optimizations():
        """Apply advanced library optimizations for SOTA performance."""
        try:
            # Advanced Polars optimizations
            import polars as pl
            polars_config = LibraryConfig.get_polars_config()
            
            for key, value in polars_config.items():
                try:
                    if hasattr(pl.Config, f"set_{key}"):
                        getattr(pl.Config, f"set_{key}")(value)
                except (AttributeError, Exception):
                    pass  # Skip if setting doesn't exist or fails
            
            # Additional advanced Polars settings
            try:
                pl.Config.set_auto_structify(True)
            except Exception:
                pass
            
            # Advanced PyArrow optimizations
            import pyarrow as pa
            arrow_config = LibraryConfig.get_arrow_config()
            
            # Set CPU count for maximum utilization
            try:
                pa.set_cpu_count(arrow_config["thread_count"])
            except Exception:
                pass
            
            # Set I/O thread count if available
            try:
                if hasattr(pa, 'set_io_thread_count'):
                    pa.set_io_thread_count(arrow_config["io_thread_count"])
            except Exception:
                pass
            
            # Memory pool optimization
            try:
                pa.set_memory_pool(pa.system_memory_pool())
            except Exception:
                pass
            
            return True
            
        except ImportError:
            return False


# ============================================================================
# PLATFORM-SPECIFIC OPTIMIZATIONS
# ============================================================================

class PlatformOptimizations:
    """Platform-specific optimizations."""
    
    @staticmethod
    def apply_system_optimizations():
        """Apply system-specific optimizations."""
        # Set environment variables for optimal performance
        os.environ['POLARS_MAX_THREADS'] = str(PerformanceConfig.DUCKDB_THREADS)
        os.environ['RAYON_NUM_THREADS'] = str(PerformanceConfig.DUCKDB_THREADS)
        
        if SYSTEM['is_windows']:
            PlatformOptimizations._apply_windows_optimizations()
        else:
            PlatformOptimizations._apply_unix_optimizations()
    
    @staticmethod
    def _apply_windows_optimizations():
        """Apply Windows-specific optimizations."""
        # Windows I/O optimizations
        os.environ['POLARS_ASYNC_MODE'] = 'true'
        
        # Memory optimization for Windows
        os.environ['ARROW_DEFAULT_MEMORY_POOL'] = 'system'
        
        # Configure asyncio for Windows
        try:
            import asyncio
            if hasattr(asyncio, 'WindowsProactorEventLoopPolicy'):
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        except Exception:
            pass  # Fallback gracefully
    
    @staticmethod
    def _apply_unix_optimizations():
        """Apply Unix-specific optimizations."""
        # Unix-specific optimizations
        os.environ['ARROW_USE_SIMD'] = 'true'
        
        # Try to use uvloop for better performance
        try:
            import uvloop
            uvloop.install()
        except ImportError:
            pass  # uvloop not available


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def ensure_directories():
    """Ensure all required directories exist."""
    directories = [
        DataConfig.DATA_DIR,
        DataConfig.TABLES_DIR,
        DataConfig.SCHEMAS_DIR,
        DataConfig.TESTS_DIR,
        DataConfig.CACHE_DIR,
        DataConfig.TEMP_DIR,
        APIConfig.LOG_DIR,
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)


def get_file_path(schema_name: str, file_type: str = "parquet", n_rows: Optional[int] = None) -> str:
    """Generate file paths with consistent naming."""
    n_rows = n_rows or DataConfig.DEFAULT_N_ROWS
    
    if file_type == "parquet":
        template = DataConfig.PARQUET_FILE_TEMPLATE
        base_dir = DataConfig.TABLES_DIR
    elif file_type == "feather":
        template = DataConfig.FEATHER_FILE_TEMPLATE
        base_dir = DataConfig.TABLES_DIR
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    
    filename = template.format(schema_name=schema_name, n_rows=n_rows)
    return os.path.join(base_dir, filename)


def get_write_path(schema_name: str, file_type: str = "parquet", suffix: str = "") -> str:
    """Generate timestamped paths for write operations."""
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{schema_name}_write_{timestamp}{suffix}.{file_type}"
    
    # Create schema-specific directory
    schema_dir = os.path.join(DataConfig.TABLES_DIR, schema_name)
    Path(schema_dir).mkdir(parents=True, exist_ok=True)
    
    return os.path.join(schema_dir, filename)


def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB."""
    try:
        return os.path.getsize(file_path) / (1024 * 1024)
    except OSError:
        return 0.0


def get_system_info() -> Dict[str, Any]:
    """Get comprehensive system information."""
    try:
        import psutil
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('.')
        
        return {
            **SYSTEM,
            "memory_total_gb": round(memory.total / (1024**3), 2),
            "memory_available_gb": round(memory.available / (1024**3), 2),
            "memory_usage_percent": memory.percent,
            "disk_total_gb": round(disk.total / (1024**3), 2),
            "disk_free_gb": round(disk.free / (1024**3), 2),
            "disk_usage_percent": round((disk.used / disk.total) * 100, 2),
        }
    except ImportError:
        return SYSTEM


def apply_performance_optimizations():
    """Apply all performance optimizations."""
    ensure_directories()
    PlatformOptimizations.apply_system_optimizations()
    
    # Apply advanced library optimizations
    LibraryConfig.apply_advanced_optimizations()


def create_optimized_duckdb_connection(memory_db: bool = True, database_path: str = ":memory:"):
    """
    Create a DuckDB connection optimized for SOTA performance.
    
    Args:
        memory_db: Whether to use in-memory database for maximum speed
        database_path: Path to database file (ignored if memory_db=True)
    
    Returns:
        Optimized DuckDB connection
    """
    try:
        import duckdb
    except ImportError:
        raise ImportError("DuckDB is required for optimized connections")
    
    # Get optimal configuration
    config = LibraryConfig.get_duckdb_config()
    
    # Create connection
    db_path = ":memory:" if memory_db else database_path
    conn = duckdb.connect(db_path)
    
    # Apply high-performance settings
    for setting, value in config.items():
        try:
            if setting == "temp_directory":
                conn.execute(f"SET {setting}='{value}'")
            elif isinstance(value, bool):
                conn.execute(f"SET {setting}={str(value).lower()}")
            elif setting in ["memory_limit", "max_memory"]:
                conn.execute(f"SET {setting}='{value}'")
            else:
                conn.execute(f"SET {setting}={value}")
        except Exception:
            # Skip settings that might not be available in all DuckDB versions
            pass
    
    return conn


# ============================================================================
# BACKWARDS COMPATIBILITY EXPORTS
# ============================================================================

# For existing code compatibility, export commonly used values
API_HOST = APIConfig.HOST
API_PORT = APIConfig.PORT
DEBUG = APIConfig.DEBUG

DATA_DIR = DataConfig.DATA_DIR
TABLES_DIR = DataConfig.TABLES_DIR
SCHEMAS_DIR = DataConfig.SCHEMAS_DIR
TEMP_DIR = DataConfig.TEMP_DIR
N_ROWS = DataConfig.DEFAULT_N_ROWS

DUCKDB_THREADS = PerformanceConfig.DUCKDB_THREADS
DUCKDB_MEMORY_LIMIT = f"{PerformanceConfig.DUCKDB_MEMORY_LIMIT_GB}GB"
ARROW_MEMORY_POOL_SIZE = f"{PerformanceConfig.ARROW_MEMORY_POOL_SIZE_GB}GB"

PARQUET_ROW_GROUP_SIZE = PerformanceConfig.PARQUET_ROW_GROUP_SIZE
DEFAULT_BATCH_SIZE = PerformanceConfig.DEFAULT_BATCH_SIZE
POLARS_INFER_SCHEMA_LENGTH = PerformanceConfig.POLARS_INFER_SCHEMA_LENGTH
ULTRA_FAST_INFER_LENGTH = PerformanceConfig.ULTRA_FAST_INFER_LENGTH

# Write configurations
ULTRA_FAST_WRITE_CONFIG = WriteProfiles.ULTRA_FAST
STANDARD_WRITE_CONFIG = WriteProfiles.BALANCED

# Validation settings
MAX_VALIDATION_ERRORS = PerformanceConfig.MAX_VALIDATION_ERRORS
VALIDATION_BATCH_SIZE = PerformanceConfig.VALIDATION_BATCH_SIZE
SKIP_VALIDATION_THRESHOLD = PerformanceConfig.SKIP_VALIDATION_THRESHOLD

# File templates
PARQUET_FILE_TEMPLATE = DataConfig.PARQUET_FILE_TEMPLATE
FEATHER_FILE_TEMPLATE = DataConfig.FEATHER_FILE_TEMPLATE

# Functions
get_optimized_duckdb_connection = create_optimized_duckdb_connection


# ============================================================================
# INITIALIZATION
# ============================================================================

# Auto-apply optimizations when module is imported
if __name__ != "__main__":
    try:
        apply_performance_optimizations()
    except Exception:
        pass  # Fail silently during import to avoid breaking the application


if __name__ == "__main__":
    # Print configuration summary when run directly
    print("=== Data Forge API Configuration ===")
    print(f"Platform: {SYSTEM['platform']}")
    print(f"CPU Cores: {SYSTEM['cpu_count']} (using {SYSTEM['optimal_threads']} for processing)")
    print(f"Memory: {SYSTEM['memory_gb']} GB")
    print(f"DuckDB Threads: {DUCKDB_THREADS}")
    print(f"DuckDB Memory: {DUCKDB_MEMORY_LIMIT}")
    print(f"Arrow Memory Pool: {ARROW_MEMORY_POOL_SIZE}")
    print(f"Batch Size: {DEFAULT_BATCH_SIZE:,}")
    print(f"Row Group Size: {PARQUET_ROW_GROUP_SIZE:,}")
    print("=====================================")
