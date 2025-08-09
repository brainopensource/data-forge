"""
Professional startup manager for Data Forge API.
Handles all application initialization, configuration, and optimization setup.
"""
import asyncio
import os
import sys
from typing import Dict, Any, Optional
from contextlib import asynccontextmanager

import polars as pl
import pyarrow as pa
import duckdb

from app.config.logging_config import logger
from app.config.logging_utils import log_application_event
if sys.platform.startswith('win'):
    from app.core.config_windows import (
        WINDOWS_DUCKDB_CONFIG as PLATFORM_DUCKDB_CONFIG,
        ensure_directories,
        apply_windows_optimizations,
        get_windows_system_info,
    )
else:
    # Fallback to generic settings and light optimizations on non-Windows
    from app.core.config import ensure_directories  # type: ignore
    from app.config.settings import settings

    def apply_windows_optimizations():  # type: ignore
        # Minimal cross-platform polars tuning
        try:
            import polars as pl
            if hasattr(pl.Config, 'set_streaming_chunk_size'):
                pl.Config.set_streaming_chunk_size(1_000_000)
        except Exception:
            pass
        # Thread envs based on CPU count
        cpu = os.cpu_count() or 4
        os.environ.setdefault('POLARS_MAX_THREADS', str(cpu))
        os.environ.setdefault('RAYON_NUM_THREADS', str(cpu))
        return True

    def get_windows_system_info():  # type: ignore
        import platform, psutil
        return {
            "os": platform.system(),
            "os_version": platform.version(),
            "architecture": platform.architecture()[0],
            "cpu_count": os.cpu_count() or 1,
            "total_memory_gb": round(psutil.virtual_memory().total / (1024**3), 2),
            "available_memory_gb": round(psutil.virtual_memory().available / (1024**3), 2),
        }

    # Construct a minimal DuckDB config from settings
    PLATFORM_DUCKDB_CONFIG = {
        "threads": settings.duckdb_threads,
        # Expect settings.duckdb_memory_limit like '8GB' or '8192MB'
        # We'll parse in _configure_database
        "memory_limit": settings.duckdb_memory_limit,
        "max_memory": settings.duckdb_memory_limit,
        "temp_directory": settings.temp_dir,
        "enable_progress_bar": False,
        "preserve_insertion_order": False,
    }


class StartupManager:
    """
    Professional startup manager for application initialization.
    Separates concerns and makes initialization testable and modular.
    """
    
    def __init__(self):
        self.initialization_status: Dict[str, bool] = {}
        self.startup_metrics: Dict[str, float] = {}
        
    async def initialize_application(self) -> Dict[str, Any]:
        """
        Initialize the entire application with proper error handling and metrics.
        
        Returns:
            Dict containing initialization status and metrics
        """
        import time
        start_time = time.time()
        
        log_application_event("Starting application initialization")
        
        try:
            # Initialize directories
            await self._initialize_directories()
            
            # Configure async environment
            await self._configure_async_environment()
            
            # Configure memory and performance
            await self._configure_memory_optimization()
            
            # Configure libraries
            await self._configure_libraries()
            
            # Configure database connections
            await self._configure_database()
            
            total_time = time.time() - start_time
            self.startup_metrics['total_initialization_time'] = total_time
            
            log_application_event(f"Application initialization completed in {total_time:.3f}s")
            
            return {
                'status': 'success',
                'metrics': self.startup_metrics,
                'initialization_status': self.initialization_status,
                'system_info': get_windows_system_info()
            }
            
        except Exception as e:
            log_application_event(f"Application initialization failed: {e}")
            logger.error(f"Application initialization failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'partial_initialization': self.initialization_status
            }
    
    async def _initialize_directories(self):
        """Initialize required directories."""
        import time
        start_time = time.time()
        
        try:
            ensure_directories()
            self.initialization_status['directories'] = True
            log_application_event("Required directories initialized")
        except Exception as e:
            self.initialization_status['directories'] = False
            logger.error(f"Directory initialization failed: {e}")
            raise
        finally:
            self.startup_metrics['directory_init_time'] = time.time() - start_time
    
    async def _configure_async_environment(self):
        """Configure async environment for Windows optimization."""
        import time
        start_time = time.time()
        
        try:
            if hasattr(asyncio, 'WindowsProactorEventLoopPolicy'):
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
                self.initialization_status['async_optimization'] = True
                log_application_event("Windows ProactorEventLoop policy configured")
            else:
                self.initialization_status['async_optimization'] = False
                log_application_event("Windows ProactorEventLoop not available")
                logger.warning("Windows ProactorEventLoop not available")
        except Exception as e:
            self.initialization_status['async_optimization'] = False
            logger.error(f"Async environment configuration failed: {e}")
            raise
        finally:
            self.startup_metrics['async_config_time'] = time.time() - start_time
    
    async def _configure_memory_optimization(self):
        """Configure memory optimization settings."""
        import time
        start_time = time.time()
        
        try:
            # Configure Arrow memory pool
            pa.set_memory_pool(pa.system_memory_pool())
            self.initialization_status['arrow_memory'] = True
            log_application_event("Arrow memory pool configured for Windows performance")
        except Exception as e:
            self.initialization_status['arrow_memory'] = False
            logger.error(f"Arrow memory configuration failed: {e}")
            raise
        finally:
            self.startup_metrics['memory_config_time'] = time.time() - start_time
    
    async def _configure_libraries(self):
        """Configure performance libraries."""
        import time
        start_time = time.time()
        
        try:
            # Apply Windows-specific optimizations for Polars
            apply_windows_optimizations()
            self.initialization_status['polars_optimization'] = True
            log_application_event("Polars configured for Windows-optimized performance")
        except Exception as e:
            self.initialization_status['polars_optimization'] = False
            logger.error(f"Polars optimization failed: {e}")
            raise
        finally:
            self.startup_metrics['library_config_time'] = time.time() - start_time
    
    async def _configure_database(self):
        """Configure database connections and settings."""
        import time
        start_time = time.time()
        
        try:
            # Configure DuckDB for high-performance operations
            test_connection = duckdb.connect(":memory:")
            
            successful_settings = []
            failed_settings = []
            
            def _to_mb_string(v):
                s = str(v).strip().upper()
                if s.endswith('GB'):
                    try:
                        num = float(s[:-2])
                        return f"{int(num*1024)}MB"
                    except Exception:
                        return s
                if s.endswith('MB'):
                    return s
                # assume it's numeric MB already
                try:
                    return f"{int(float(s))}MB"
                except Exception:
                    return s

            for setting, value in PLATFORM_DUCKDB_CONFIG.items():
                try:
                    if setting == "temp_directory":
                        test_connection.execute(f"SET {setting}='{value}'")
                    elif isinstance(value, bool):
                        test_connection.execute(f"SET {setting}={str(value).lower()}")
                    elif setting in ["memory_limit", "max_memory"]:
                        # Ensure DuckDB receives MB unit strings in quotes
                        mb = _to_mb_string(value)
                        test_connection.execute(f"SET {setting}='{mb}'")
                    else:
                        test_connection.execute(f"SET {setting}={value}")
                    successful_settings.append(setting)
                except Exception as setting_error:
                    failed_settings.append({"setting": setting, "error": str(setting_error)})
                    logger.warning(f"DuckDB setting '{setting}' failed: {setting_error}")
            
            test_connection.close()
            
            # Report configuration results
            if successful_settings:
                log_application_event(f"DuckDB configured successfully with {len(successful_settings)} settings: {', '.join(successful_settings)}")
            
            if failed_settings:
                log_application_event(f"DuckDB had {len(failed_settings)} configuration warnings (non-critical)")
                logger.warning(f"DuckDB configuration warnings: {failed_settings}")
            
            self.initialization_status['duckdb_optimization'] = True
            self.startup_metrics['duckdb_successful_settings'] = len(successful_settings)
            self.startup_metrics['duckdb_failed_settings'] = len(failed_settings)
            
            log_application_event("DuckDB optimized for Windows high-performance operations")
            
        except Exception as e:
            self.initialization_status['duckdb_optimization'] = False
            logger.warning(f"DuckDB optimization failed: {e}")
            # Don't raise - DuckDB optimization failure shouldn't stop the app
        finally:
            self.startup_metrics['database_config_time'] = time.time() - start_time
    
    async def cleanup_application(self):
        """Cleanup application resources during shutdown."""
        log_application_event("Starting application cleanup")
        
        try:
            # Add any cleanup logic here
            # For example: close connections, flush buffers, etc.
            log_application_event("Application cleanup completed successfully")
        except Exception as e:
            logger.error(f"Application cleanup failed: {e}")


# Global startup manager instance
startup_manager = StartupManager()


@asynccontextmanager
async def create_lifespan_manager():
    """
    Create a lifespan context manager using the professional startup manager.
    This should be used in main.py instead of the current lifespan function.
    """
    # Startup
    initialization_result = await startup_manager.initialize_application()
    
    if initialization_result['status'] != 'success':
        logger.error("Application startup failed")
        raise RuntimeError("Application initialization failed")
    
    try:
        yield initialization_result
    finally:
        # Shutdown
        await startup_manager.cleanup_application()


# Convenience function for testing and direct usage
async def initialize_for_testing():
    """Initialize application for testing purposes."""
    return await startup_manager.initialize_application() 