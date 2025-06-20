"""
Performance utilities and optimizations for Data Forge API.
Contains performance monitoring, profiling, and optimization utilities.
"""
import time
import psutil
import gc
from typing import Dict, Any, Optional, Callable
from functools import wraps
from contextlib import contextmanager
from app.config.logging_utils import log_application_event


class PerformanceMonitor:
    """
    Performance monitoring and profiling utilities.
    """
    
    def __init__(self):
        self.metrics = {}
        self.start_time = time.time()
    
    def record_metric(self, name: str, value: float, unit: str = "ms"):
        """Record a performance metric."""
        if name not in self.metrics:
            self.metrics[name] = []
        self.metrics[name].append({
            "value": value,
            "unit": unit,
            "timestamp": time.time()
        })
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get current system performance metrics."""
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('.')
        
        return {
            "cpu_usage_percent": cpu_percent,
            "memory_usage_percent": memory.percent,
            "memory_available_gb": round(memory.available / (1024**3), 2),
            "memory_used_gb": round(memory.used / (1024**3), 2),
            "disk_usage_percent": disk.percent,
            "disk_free_gb": round(disk.free / (1024**3), 2),
            "uptime_seconds": time.time() - self.start_time
        }
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary with metrics."""
        summary = {
            "system": self.get_system_metrics(),
            "recorded_metrics": {}
        }
        
        for name, values in self.metrics.items():
            if values:
                latest = values[-1]
                avg_value = sum(v["value"] for v in values) / len(values)
                summary["recorded_metrics"][name] = {
                    "latest": latest["value"],
                    "average": round(avg_value, 3),
                    "unit": latest["unit"],
                    "count": len(values)
                }
        
        return summary


# Global performance monitor
performance_monitor = PerformanceMonitor()


def performance_timer(operation_name: str):
    """
    Decorator to time function execution and record metrics.
    """
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                execution_time = (time.time() - start_time) * 1000  # Convert to ms
                performance_monitor.record_metric(f"{operation_name}_time", execution_time, "ms")
                return result
            except Exception as e:
                execution_time = (time.time() - start_time) * 1000
                performance_monitor.record_metric(f"{operation_name}_error_time", execution_time, "ms")
                raise
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                execution_time = (time.time() - start_time) * 1000
                performance_monitor.record_metric(f"{operation_name}_time", execution_time, "ms")
                return result
            except Exception as e:
                execution_time = (time.time() - start_time) * 1000
                performance_monitor.record_metric(f"{operation_name}_error_time", execution_time, "ms")
                raise
        
        return async_wrapper if hasattr(func, '__code__') and func.__code__.co_flags & 0x80 else sync_wrapper
    return decorator


@contextmanager
def performance_context(operation_name: str):
    """
    Context manager for timing operations.
    """
    start_time = time.time()
    try:
        yield
    finally:
        execution_time = (time.time() - start_time) * 1000
        performance_monitor.record_metric(f"{operation_name}_context", execution_time, "ms")


def optimize_memory():
    """
    Force garbage collection and memory optimization.
    """
    gc.collect()  # Force garbage collection
    log_application_event("Memory optimization performed")


def get_throughput_metrics(records_processed: int, time_taken: float) -> Dict[str, Any]:
    """
    Calculate throughput metrics.
    """
    if time_taken <= 0:
        return {
            "records_per_second": 0,
            "mb_per_second": 0,
            "time_per_1m_records": 0
        }
    
    records_per_second = records_processed / time_taken
    time_per_1m_records = 1_000_000 / records_per_second if records_per_second > 0 else 0
    
    # Estimate MB/s (assuming ~100 bytes per record average)
    estimated_bytes = records_processed * 100
    mb_per_second = (estimated_bytes / (1024 * 1024)) / time_taken
    
    return {
        "records_per_second": int(records_per_second),
        "mb_per_second": round(mb_per_second, 2),
        "time_per_1m_records": round(time_per_1m_records, 2),
        "efficiency_score": min(100, (records_per_second / 10_000_000) * 100)  # Score based on 10M target
    }


class PerformanceProfiler:
    """
    Advanced performance profiler for detailed analysis.
    """
    
    def __init__(self):
        self.profiles = {}
    
    def start_profile(self, name: str):
        """Start profiling an operation."""
        self.profiles[name] = {
            "start_time": time.time(),
            "start_memory": psutil.Process().memory_info().rss,
            "start_cpu": psutil.Process().cpu_percent()
        }
    
    def end_profile(self, name: str) -> Dict[str, Any]:
        """End profiling and return results."""
        if name not in self.profiles:
            return {}
        
        profile = self.profiles[name]
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss
        end_cpu = psutil.Process().cpu_percent()
        
        result = {
            "duration_ms": (end_time - profile["start_time"]) * 1000,
            "memory_delta_mb": (end_memory - profile["start_memory"]) / (1024 * 1024),
            "cpu_usage_percent": end_cpu,
            "timestamp": end_time
        }
        
        del self.profiles[name]
        return result


# Global profiler instance
profiler = PerformanceProfiler()


def benchmark_operation(func: Callable, *args, **kwargs) -> Dict[str, Any]:
    """
    Benchmark a single operation and return detailed metrics.
    """
    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss
    
    try:
        if hasattr(func, '__code__') and func.__code__.co_flags & 0x80:
            # Async function
            import asyncio
            result = asyncio.run(func(*args, **kwargs))
        else:
            # Sync function
            result = func(*args, **kwargs)
        
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss
        
        duration = end_time - start_time
        memory_delta = (end_memory - start_memory) / (1024 * 1024)  # MB
        
        return {
            "success": True,
            "duration_seconds": duration,
            "duration_ms": duration * 1000,
            "memory_delta_mb": memory_delta,
            "result": result
        }
    
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        
        return {
            "success": False,
            "duration_seconds": duration,
            "duration_ms": duration * 1000,
            "error": str(e),
            "result": None
        } 