"""
High-performance application initialization for millions of rows processing.
Optimized for maximum read/write throughput with minimal overhead.
"""
import asyncio
import sys
import os
import threading
from typing import Dict, Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb  # pragma: no cover

# Lazy imports for performance - only import when needed
from app.config.logging_utils import log_application_event


class HighPerformanceInit:
    """
    High-performance initialization manager for SOTA data processing.
    Optimized for millions of rows with minimal latency overhead.
    """
    
    _initialized = False
    _lock = threading.Lock()
    _optimization_status: Dict[str, Any] = {}
    
    @classmethod
    def lazy_optimize_libraries(cls) -> Dict[str, Any]:
        """
        Lazy initialization of performance libraries - only when first needed.
        Prevents import-time overhead while maximizing runtime performance.
        
        Returns:
            Dict[str, Any]: Library configuration status
        """
        if cls._initialized:
            return cls._optimization_status
        
        with cls._lock:
            if cls._initialized:  # Double-check locking pattern
                return cls._optimization_status
            
            optimization_status = {}
            
            try:
                # Establish thread env vars early (may still help downstream libs)
                cpu_count = os.cpu_count() or 4
                os.environ.setdefault('POLARS_MAX_THREADS', str(cpu_count))
                os.environ.setdefault('RAYON_NUM_THREADS', str(cpu_count))
                os.environ.setdefault('ARROW_NUM_THREADS', str(cpu_count))

                # Polars HIGH-PERFORMANCE optimizations
                import polars as pl

                # Critical for millions of rows
                try:
                    pl.Config.set_streaming_chunk_size(1_000_000)  # 1M rows per chunk
                except Exception:
                    pass
                try:
                    # Use broadly-supported config keys
                    if hasattr(pl.Config, 'set_fmt_str_lengths'):
                        pl.Config.set_fmt_str_lengths(50)
                except Exception:
                    pass
                try:
                    pl.Config.set_tbl_rows(-1)  # No row limits
                    pl.Config.set_tbl_cols(-1)  # No column limits
                    pl.Config.set_tbl_width_chars(1000)
                except Exception:
                    pass
                try:
                    pl.Config.set_auto_structify(True)
                except Exception:
                    pass
                try:
                    pl.Config.set_verbose(False)
                except Exception:
                    pass

                optimization_status['polars'] = {
                    'status': 'high_performance',
                    'streaming_chunk_size': 1_000_000,
                    'memory_optimized': True
                }

                # PyArrow ULTRA-FAST optimizations
                import pyarrow as pa

                # Maximum CPU utilization for I/O
                try:
                    pa.set_cpu_count(cpu_count)
                except Exception:
                    pass
                # Newer Arrow may expose io thread count; guard for compatibility
                try:
                    if hasattr(pa, 'set_io_thread_count'):
                        pa.set_io_thread_count(cpu_count * 2)
                except Exception:
                    pass

                # Memory pool optimization for large datasets
                try:
                    if hasattr(pa, 'set_memory_pool'):
                        pa.set_memory_pool(pa.system_memory_pool())
                except Exception:
                    pass

                optimization_status['pyarrow'] = {
                    'status': 'ultra_fast',
                    'cpu_threads': cpu_count,
                    'io_threads': cpu_count * 2,
                    'memory_pool': 'system_optimized'
                }

                # DuckDB connection-specific optimizations (will be set per connection)
                optimization_status['duckdb'] = {
                    'status': 'connection_optimized',
                    'note': 'Per-connection settings for maximum throughput'
                }

                log_application_event("HIGH-PERFORMANCE libraries optimized for millions of rows")

            except Exception as e:
                log_application_event(f"Performance optimization error: {e}")
                optimization_status['error'] = str(e)
            
            cls._optimization_status = optimization_status
            cls._initialized = True
            return optimization_status
    
    @classmethod
    def setup_high_performance_event_loop(cls) -> None:
        """
        Setup ultra-fast event loop for maximum I/O throughput.
        Critical for handling millions of rows with minimal latency.
        """
        if sys.platform.startswith('win'):
            # Windows ProactorEventLoop for maximum I/O performance
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            log_application_event("Windows ProactorEventLoop optimized for high throughput")
        else:
            # Unix: Use uvloop if available for maximum performance
            try:
                import uvloop  # type: ignore
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
                log_application_event("uvloop optimized for maximum Unix performance")
            except ImportError:
                log_application_event("Using default asyncio (consider installing uvloop for better performance)")
    
    @classmethod
    def get_duckdb_high_performance_config(cls) -> Dict[str, Any]:
        """
        Get DuckDB configuration optimized for millions of rows.
        
        Returns:
            Dict[str, Any]: DuckDB connection parameters for maximum performance
        """
        cpu_count = os.cpu_count() or 4
        # Calculate optimal memory (80% of available, minimum 4GB for large datasets)
        import psutil
        available_memory = psutil.virtual_memory().available
        optimal_memory = max(4 * 1024**3, int(available_memory * 0.8))  # 4GB minimum
        
        return {
            'threads': cpu_count,
            'memory_limit': f'{optimal_memory // (1024**3)}GB',
            'max_memory': f'{optimal_memory // (1024**3)}GB',
            'enable_progress_bar': False,  # Disable for performance
            'enable_profiling': False,     # Disable for production speed
            'checkpoint_threshold': '1GB', # Larger checkpoints for better performance
            'wal_autocheckpoint': 10000,   # Less frequent checkpoints
            'temp_directory': os.environ.get('TEMP', '/tmp'),
            'preserve_insertion_order': False,  # Faster queries
            'enable_optimizer': True,
            'perfect_hash_threshold': 12,  # Optimize joins for large data
        }

"""Event loop policy is set during FastAPI lifespan to avoid import-time side effects."""

# Library optimization will be done lazily when first needed
def ensure_high_performance_init() -> Dict[str, Any]:
    """
    Ensure high-performance initialization is complete.
    Call this before processing large datasets.
    """
    return HighPerformanceInit.lazy_optimize_libraries()


def create_optimized_duckdb_connection(memory_db: bool = True, database_path: str = ":memory:") -> Any:
    """
    Create a DuckDB connection optimized for millions of rows processing.
    
    Args:
        memory_db: Whether to use in-memory database for maximum speed
        database_path: Path to database file (ignored if memory_db=True)
    
    Returns:
        Optimized DuckDB connection
    """
    import duckdb
    
    # Ensure high-performance settings are initialized
    ensure_high_performance_init()
    
    # Get optimal configuration
    config = HighPerformanceInit.get_duckdb_high_performance_config()
    
    # Create connection
    db_path = ":memory:" if memory_db else database_path
    conn = duckdb.connect(db_path)
    
    # Apply high-performance settings
    try:
        conn.execute(f"SET threads={config['threads']}")
        conn.execute(f"SET memory_limit='{config['memory_limit']}'")
        conn.execute(f"SET max_memory='{config['max_memory']}'")
        conn.execute(f"SET enable_progress_bar={config['enable_progress_bar']}")
        conn.execute(f"SET enable_profiling={config['enable_profiling']}")
        conn.execute(f"SET checkpoint_threshold='{config['checkpoint_threshold']}'")
        conn.execute(f"SET wal_autocheckpoint={config['wal_autocheckpoint']}")
        conn.execute(f"SET temp_directory='{config['temp_directory']}'")
        conn.execute(f"SET preserve_insertion_order={config['preserve_insertion_order']}")
        conn.execute(f"SET enable_optimizer={config['enable_optimizer']}")
        conn.execute(f"SET perfect_hash_threshold={config['perfect_hash_threshold']}")
        
        # Additional performance settings for millions of rows
        conn.execute("SET enable_http_metadata_cache=true")
        conn.execute("SET http_timeout=120000")  # 2 minutes for large operations
        conn.execute("SET streaming_buffer_size='128MB'")  # Larger buffer for streaming
        
    except Exception as e:
        # Log but don't fail - some settings might not be available in all DuckDB versions
        from app.config.logging_utils import log_application_event
        log_application_event(f"DuckDB optimization warning: {e}")
    
    return conn
