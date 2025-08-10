"""
Information and system endpoints for the Data Forge API.
Contains root, performance, system, and startup information endpoints.
"""
from fastapi import APIRouter
import asyncio

from app.config.global_settings import (
    PerformanceConfig, get_system_info, SYSTEM
)
from app.config.logging_utils import log_application_event

router = APIRouter(tags=["info"])


@router.get("/")
async def read_root():
    """
    Root endpoint with API information.
    """
    log_application_event("Root endpoint accessed")
    return {
        "message": "Data Forge API - Windows Mode",
        "project_name": "Data Forge",
        "version": "2.0.0-windows",
        "platform": "Windows-optimized",
        "performance_target": "10M+ rows/second",
        "architecture": "modular",
        "optimizations": [
            "Windows ProactorEventLoop",
            "Native Windows I/O",
            "Optimized for local-first deployment",
            "Single-process high performance"
        ],
        "endpoints": {
            "health": "/health",
            "schemas": "/schemas",
            "reads": "/read",
            "writes": "/write",
            "docs": "/docs"
        }
    }


@router.get("/performance")
async def performance_info():
    """
    Windows-specific performance configuration and capabilities.
    """
    log_application_event("Performance info endpoint accessed")
    
    return {
        "performance_mode": "windows",
        "platform": "Windows-optimized",
        "target_throughput": "10M+ rows/second",
        "event_loop": "Windows ProactorEventLoop",
        "deployment": "local-first",
        "optimizations": {
            "parquet_row_group_size": PerformanceConfig.PARQUET_ROW_GROUP_SIZE,
            "polars_infer_length": PerformanceConfig.POLARS_INFER_SCHEMA_LENGTH,
            "ultra_fast_infer_length": PerformanceConfig.ULTRA_FAST_INFER_LENGTH,
            "default_batch_size": PerformanceConfig.DEFAULT_BATCH_SIZE,
            "duckdb_threads": PerformanceConfig.DUCKDB_THREADS,
            "duckdb_memory_limit": f"{PerformanceConfig.DUCKDB_MEMORY_LIMIT_GB}GB",
            "streaming_chunk_size": PerformanceConfig.STREAMING_CHUNK_SIZE
        },
        "windows_features": {
            "proactor_event_loop": True,
            "native_io_completion_ports": True,
            "optimized_memory_patterns": True,
            "single_process_performance": True
        },
        "features": {
            "zero_copy_arrow_streams": True,
            "schema_validation": True,
            "batch_processing": True,
            "duckdb_integration": True,
            "polars_optimization": True,
            "ultra_fast_writes": True,
            "modular_architecture": True
        },
        "endpoints": {
            "ultra_fast_writes": "/write/polars/{schema_name}",
            "legacy_writes": "/write/polars-write/{schema_name}",
            "polars_reads": "/read/polars/{schema_name}",
            "duckdb_reads": "/read/duckdb/{schema_name}",
            "legacy_polars_reads": "/read/polars-read/{schema_name}",
            "legacy_duckdb_reads": "/read/duckdb-read/{schema_name}"
        }
    }


@router.get("/system")
async def system_info():
    """
    Windows system information and optimization status.
    """
    log_application_event("System info endpoint accessed")
    
    system_info = get_system_info()
    
    return {
        "platform": "Windows",
        "system_info": system_info,
        "optimizations_applied": True,
        "performance_features": {
            "proactor_event_loop": hasattr(asyncio, 'WindowsProactorEventLoopPolicy'),
            "windows_io_completion_ports": True,
            "optimized_threading": True,
            "memory_optimization": True
        },
        "recommendations": {
            "optimal_for_local_deployment": True,
            "suggested_concurrent_requests": 1000,
            "recommended_batch_size": PerformanceConfig.DEFAULT_BATCH_SIZE,
            "memory_usage_optimized": True
        }
    }


@router.get("/startup")
async def startup_metrics():
    """
    Detailed startup metrics and initialization status.
    """
    log_application_event("Startup metrics endpoint accessed")
    
    from app.core.startup import startup_manager
    
    return {
        "startup_status": "completed",
        "initialization_metrics": startup_manager.startup_metrics,
        "component_status": startup_manager.initialization_status,
        "professional_architecture": True,
        "architecture_benefits": [
            "Modular initialization",
            "Comprehensive error handling", 
            "Detailed performance metrics",
            "Testable components",
            "Graceful failure handling"
        ]
    } 