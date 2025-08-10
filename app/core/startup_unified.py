"""
Professional startup manager for Data Forge API.
Handles all application initialization, configuration, and optimization setup.
"""
import time
import duckdb
import asyncio
from typing import Dict, Any
from contextlib import asynccontextmanager

# Use global configuration
from app.config.global_settings import (
    LibraryConfig,
    apply_performance_optimizations,
    get_system_info,
    SYSTEM
)
from app.config.logging_config import logger
from app.config.logging_utils import log_application_event


class StartupManager:
    """
    Professional startup manager for application initialization.
    Optimized for high-performance SOTA API.
    """
    
    def __init__(self):
        self.initialization_status: Dict[str, bool] = {}
        self.startup_metrics: Dict[str, float] = {}
        
    async def initialize_application(self) -> Dict[str, Any]:
        """
        Initialize the entire application with proper error handling and metrics.
        """
        start_time = time.time()
        log_application_event("Starting Data Forge API initialization")
        
        try:
            # Apply all performance optimizations (directories, libraries, platform-specific)
            await self._apply_optimizations()
            
            # Configure async environment
            await self._configure_async_environment()
            
            # Configure database connections
            await self._configure_database()
            
            total_time = time.time() - start_time
            self.startup_metrics['total_initialization_time'] = total_time
            
            log_application_event(f"Application initialization completed in {total_time:.3f}s")
            
            return {
                'status': 'success',
                'metrics': self.startup_metrics,
                'initialization_status': self.initialization_status,
                'system_info': get_system_info()
            }
            
        except Exception as e:
            total_time = time.time() - start_time
            self.startup_metrics['total_initialization_time'] = total_time
            log_application_event(f"Application initialization failed: {e}")
            logger.error(f"Application initialization failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'partial_initialization': self.initialization_status,
                'time_to_failure': total_time
            }
    
    async def _apply_optimizations(self):
        """Apply all performance optimizations."""
        start_time = time.time()
        
        try:
            apply_performance_optimizations()
            self.initialization_status['performance_optimizations'] = True
            log_application_event("Performance optimizations applied successfully")
        except Exception as e:
            self.initialization_status['performance_optimizations'] = False
            logger.error(f"Performance optimization failed: {e}")
            raise
        finally:
            self.startup_metrics['optimization_time'] = time.time() - start_time
    
    async def _configure_async_environment(self):
        """Configure async environment for optimal performance."""
        start_time = time.time()
        
        try:
            if SYSTEM['is_windows'] and hasattr(asyncio, 'WindowsProactorEventLoopPolicy'):
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
                self.initialization_status['async_optimization'] = True
                log_application_event("Windows ProactorEventLoop configured for maximum I/O performance")
            else:
                self.initialization_status['async_optimization'] = True
                log_application_event("Default asyncio event loop configured")
                
        except Exception as e:
            self.initialization_status['async_optimization'] = False
            logger.error(f"Async environment configuration failed: {e}")
            raise
        finally:
            self.startup_metrics['async_config_time'] = time.time() - start_time
    
    async def _configure_database(self):
        """Configure database connections and settings."""
        start_time = time.time()
        
        try:
            # Test DuckDB configuration
            test_connection = duckdb.connect(":memory:")
            duckdb_config = LibraryConfig.get_duckdb_config()
            
            successful_settings = []
            failed_settings = []
            
            for setting, value in duckdb_config.items():
                try:
                    if setting == "temp_directory":
                        test_connection.execute(f"SET {setting}='{value}'")
                    elif isinstance(value, bool):
                        test_connection.execute(f"SET {setting}={str(value).lower()}")
                    elif setting in ["memory_limit", "max_memory"]:
                        test_connection.execute(f"SET {setting}='{value}'")
                    else:
                        test_connection.execute(f"SET {setting}={value}")
                    successful_settings.append(setting)
                except Exception as setting_error:
                    failed_settings.append({"setting": setting, "error": str(setting_error)})
                    logger.warning(f"DuckDB setting '{setting}' failed: {setting_error}")
            
            test_connection.close()
            
            # Report configuration results
            if successful_settings:
                log_application_event(f"DuckDB configured with {len(successful_settings)} settings: {', '.join(successful_settings)}")
            
            if failed_settings:
                log_application_event(f"DuckDB had {len(failed_settings)} configuration warnings (non-critical)")
            
            self.initialization_status['duckdb_optimization'] = True
            self.startup_metrics['duckdb_successful_settings'] = len(successful_settings)
            self.startup_metrics['duckdb_failed_settings'] = len(failed_settings)
            
        except Exception as e:
            self.initialization_status['duckdb_optimization'] = False
            logger.warning(f"DuckDB optimization failed: {e}")
            # Don't raise - DuckDB config failure is not critical
        finally:
            self.startup_metrics['duckdb_config_time'] = time.time() - start_time
    
    async def cleanup_application(self):
        """Cleanup application resources during shutdown."""
        log_application_event("Starting application cleanup")
        
        try:
            # Perform any necessary cleanup
            log_application_event("Application cleanup completed successfully")
        except Exception as e:
            logger.error(f"Application cleanup failed: {e}")


# Global startup manager instance
startup_manager = StartupManager()


@asynccontextmanager
async def create_lifespan_manager():
    """
    Create a lifespan context manager using the professional startup manager.
    """
    # Startup
    initialization_result = await startup_manager.initialize_application()
    
    if initialization_result['status'] != 'success':
        raise RuntimeError(f"Application initialization failed: {initialization_result.get('error', 'Unknown error')}")
    
    try:
        yield initialization_result
    finally:
        # Cleanup
        await startup_manager.cleanup_application()


# Convenience function for testing and direct usage
async def initialize_for_testing():
    """Initialize application for testing purposes."""
    return await startup_manager.initialize_application()


if __name__ == "__main__":
    # Test the startup manager
    async def test_startup():
        result = await initialize_for_testing()
        print("Startup test result:", result)
    
    asyncio.run(test_startup())
