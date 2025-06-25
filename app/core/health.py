"""
Health monitoring and system status utilities.
Provides comprehensive health checks and system monitoring.
"""
import time
import asyncio
from typing import Dict, Any, Optional
import psutil
import duckdb
import polars as pl
import pyarrow as pa

from app.config.logging_config import logger
from app.core.config_windows import get_windows_system_info, DATA_DIR, TEMP_DIR
from app.core.startup import startup_manager


class HealthMonitor:
    """
    Comprehensive health monitoring for the application.
    """
    
    def __init__(self):
        self.start_time = time.time()
        self.health_checks = {}
    
    async def get_comprehensive_health(self) -> Dict[str, Any]:
        """
        Get comprehensive health status of the application.
        """
        health_status = {
            "status": "healthy",
            "timestamp": time.time(),
            "uptime_seconds": time.time() - self.start_time,
            "checks": {}
        }
        
        # Run all health checks
        checks = [
            ("system_resources", self._check_system_resources),
            ("disk_space", self._check_disk_space),
            ("memory_usage", self._check_memory_usage),
            ("library_status", self._check_library_status),
            ("database_connectivity", self._check_database_connectivity),
            ("startup_status", self._check_startup_status),
        ]
        
        overall_healthy = True
        
        for check_name, check_func in checks:
            try:
                check_result = await check_func()
                health_status["checks"][check_name] = check_result
                
                if not check_result.get("healthy", True):
                    overall_healthy = False
                    
            except Exception as e:
                health_status["checks"][check_name] = {
                    "healthy": False,
                    "error": str(e),
                    "check_failed": True
                }
                overall_healthy = False
        
        health_status["status"] = "healthy" if overall_healthy else "unhealthy"
        return health_status
    
    async def _check_system_resources(self) -> Dict[str, Any]:
        """Check system resource availability."""
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            
            # Define thresholds
            cpu_threshold = 90.0  # 90% CPU usage
            memory_threshold = 85.0  # 85% memory usage
            
            healthy = cpu_percent < cpu_threshold and memory.percent < memory_threshold
            
            return {
                "healthy": healthy,
                "cpu_usage_percent": cpu_percent,
                "memory_usage_percent": memory.percent,
                "memory_available_gb": round(memory.available / (1024**3), 2),
                "thresholds": {
                    "cpu_threshold": cpu_threshold,
                    "memory_threshold": memory_threshold
                },
                "warnings": [] if healthy else [
                    f"High CPU usage: {cpu_percent:.1f}%" if cpu_percent >= cpu_threshold else None,
                    f"High memory usage: {memory.percent:.1f}%" if memory.percent >= memory_threshold else None
                ]
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}
    
    async def _check_disk_space(self) -> Dict[str, Any]:
        """Check disk space availability."""
        try:
            data_disk = psutil.disk_usage(DATA_DIR)
            temp_disk = psutil.disk_usage(TEMP_DIR)
            
            # 90% threshold for disk usage
            threshold = 90.0
            
            data_healthy = data_disk.percent < threshold
            temp_healthy = temp_disk.percent < threshold
            healthy = data_healthy and temp_healthy
            
            return {
                "healthy": healthy,
                "data_directory": {
                    "path": DATA_DIR,
                    "usage_percent": data_disk.percent,
                    "free_gb": round(data_disk.free / (1024**3), 2),
                    "healthy": data_healthy
                },
                "temp_directory": {
                    "path": TEMP_DIR,
                    "usage_percent": temp_disk.percent,
                    "free_gb": round(temp_disk.free / (1024**3), 2),
                    "healthy": temp_healthy
                },
                "threshold": threshold
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}
    
    async def _check_memory_usage(self) -> Dict[str, Any]:
        """Check application memory usage."""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            system_memory = psutil.virtual_memory()
            
            # Calculate memory usage percentages
            app_memory_mb = memory_info.rss / (1024 * 1024)
            app_memory_percent = (memory_info.rss / system_memory.total) * 100
            
            # 70% of system memory threshold for the application
            threshold = 70.0
            healthy = app_memory_percent < threshold
            
            return {
                "healthy": healthy,
                "application_memory_mb": round(app_memory_mb, 2),
                "application_memory_percent": round(app_memory_percent, 2),
                "system_memory_total_gb": round(system_memory.total / (1024**3), 2),
                "threshold_percent": threshold,
                "warning": f"High application memory usage: {app_memory_percent:.1f}%" if not healthy else None
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}
    
    async def _check_library_status(self) -> Dict[str, Any]:
        """Check the status of critical libraries."""
        try:
            library_status = {}
            
            # Test Polars
            try:
                test_df = pl.DataFrame({"test": [1, 2, 3]})
                library_status["polars"] = {"healthy": True, "version": pl.__version__}
            except Exception as e:
                library_status["polars"] = {"healthy": False, "error": str(e)}
            
            # Test PyArrow
            try:
                test_table = pa.table({"test": [1, 2, 3]})
                library_status["pyarrow"] = {"healthy": True, "version": pa.__version__}
            except Exception as e:
                library_status["pyarrow"] = {"healthy": False, "error": str(e)}
            
            # Test DuckDB
            try:
                conn = duckdb.connect(":memory:")
                conn.execute("SELECT 1")
                conn.close()
                library_status["duckdb"] = {"healthy": True, "version": duckdb.__version__}
            except Exception as e:
                library_status["duckdb"] = {"healthy": False, "error": str(e)}
            
            overall_healthy = all(lib["healthy"] for lib in library_status.values())
            
            return {
                "healthy": overall_healthy,
                "libraries": library_status
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}
    
    async def _check_database_connectivity(self) -> Dict[str, Any]:
        """Check database connectivity and performance."""
        try:
            start_time = time.time()
            
            # Test DuckDB connection and basic query
            conn = duckdb.connect(":memory:")
            result = conn.execute("SELECT 1 as test").fetchone()
            conn.close()
            
            query_time = time.time() - start_time
            
            # 1 second threshold for basic query
            threshold = 1.0
            healthy = query_time < threshold and result[0] == 1
            
            return {
                "healthy": healthy,
                "query_time_seconds": round(query_time, 4),
                "threshold_seconds": threshold,
                "result_correct": result[0] == 1 if result else False
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}
    
    async def _check_startup_status(self) -> Dict[str, Any]:
        """Check the status of application startup."""
        try:
            return {
                "healthy": True,
                "initialization_status": startup_manager.initialization_status,
                "startup_metrics": startup_manager.startup_metrics
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}
    
    async def get_quick_health(self) -> Dict[str, Any]:
        """Get quick health status for basic monitoring."""
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            
            return {
                "status": "healthy",
                "uptime_seconds": time.time() - self.start_time,
                "cpu_usage_percent": cpu_percent,
                "memory_usage_percent": memory.percent,
                "timestamp": time.time()
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": time.time()
            }


# Global health monitor instance
health_monitor = HealthMonitor() 