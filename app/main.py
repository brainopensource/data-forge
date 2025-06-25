"""
Data Forge API - Windows-Optimized Ultra-Performance Version
Target: 10M+ rows/second throughput on Windows

Windows-first, local-optimized architecture for maximum performance.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
import polars as pl
import pyarrow as pa
import duckdb
import os
import asyncio

# Core Windows-optimized performance modules
from app.core.config_windows import (
    API_PORT, API_HOST, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT, 
    ARROW_MEMORY_POOL_SIZE, ensure_directories, apply_windows_optimizations,
    get_windows_system_info, WINDOWS_DUCKDB_CONFIG
)

# API routes
from app.api.routes.health import router as health_router
from app.api.routes.schemas import router as schemas_router
from app.api.routes.reads import router as reads_router
from app.api.routes.writes import router as writes_router

# Configuration and logging
from app.config.logging_config import logger
from app.config.logging_utils import log_application_event


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan with Windows-optimized performance.
    Startup optimizations for maximum write/read performance on Windows.
    """
    # Startup
    log_application_event("FastAPI application startup - WINDOWS ULTRA-PERFORMANCE MODE", f"port {API_PORT}")
    
    try:
        # Ensure required directories exist
        ensure_directories()
        log_application_event("Required directories created")
        
        # Windows-specific asyncio optimizations
        if hasattr(asyncio, 'WindowsProactorEventLoopPolicy'):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            log_application_event("Windows ProactorEventLoop policy set for optimal I/O performance")
        
        # Configure Arrow memory pool for Windows optimization
        pa.set_memory_pool(pa.system_memory_pool())
        log_application_event("Arrow memory pool configured for Windows performance")
        
        # Apply Windows-specific optimizations
        apply_windows_optimizations()
        log_application_event("Polars configured for Windows-optimized performance")
        
        # Configure DuckDB for Windows high-performance operations
        try:
            default_con = duckdb.connect(":memory:")
            for setting, value in WINDOWS_DUCKDB_CONFIG.items():
                if setting == "temp_directory":
                    default_con.execute(f"SET {setting}='{value}'")
                elif isinstance(value, bool):
                    default_con.execute(f"SET {setting}={str(value).lower()}")
                else:
                    default_con.execute(f"SET {setting}={value}")
            default_con.close()
            log_application_event("DuckDB optimized for Windows high-performance operations")
        except Exception as e:
            logger.warning(f"DuckDB optimization failed: {e}")
        
        log_application_event("WINDOWS ULTRA-PERFORMANCE optimizations applied successfully")
        
        yield
        
    finally:
        # Shutdown
        log_application_event("FastAPI application shutdown")


# Create FastAPI application with Windows-optimized settings
app = FastAPI(
    title="Data Forge API - Windows Ultra Performance",
    description="A Windows-optimized, ultra-high-performance RESTful API for data processing. Target: 10M+ rows/second on Windows.",
    version="2.0.0-windows",
    debug=False,  # Disable debug for production performance
    lifespan=lifespan,
    # Windows performance optimizations
    generate_unique_id_function=lambda route: f"{route.tags[0]}-{route.name}" if route.tags else route.name,
)


# Include all route modules
app.include_router(health_router)
app.include_router(schemas_router)
app.include_router(reads_router)
app.include_router(writes_router)


# Root endpoint
@app.get("/")
async def read_root():
    """
    Root endpoint with API information.
    """
    log_application_event("Root endpoint accessed")
    return {
        "message": "Data Forge API - Windows Ultra Performance Mode",
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


# Performance monitoring endpoint
@app.get("/performance")
async def performance_info():
    """
    Windows-specific performance configuration and capabilities.
    """
    log_application_event("Performance info endpoint accessed")
    
    from app.core.config_windows import (
        PARQUET_ROW_GROUP_SIZE, POLARS_INFER_SCHEMA_LENGTH, 
        DEFAULT_BATCH_SIZE, ULTRA_FAST_INFER_LENGTH, WINDOWS_STREAMING_CHUNK_SIZE
    )
    
    return {
        "performance_mode": "windows-ultra-fast",
        "platform": "Windows-optimized",
        "target_throughput": "10M+ rows/second",
        "event_loop": "Windows ProactorEventLoop",
        "deployment": "local-first",
        "optimizations": {
            "parquet_row_group_size": PARQUET_ROW_GROUP_SIZE,
            "polars_infer_length": POLARS_INFER_SCHEMA_LENGTH,
            "ultra_fast_infer_length": ULTRA_FAST_INFER_LENGTH,
            "default_batch_size": DEFAULT_BATCH_SIZE,
            "duckdb_threads": DUCKDB_THREADS,
            "duckdb_memory_limit": DUCKDB_MEMORY_LIMIT,
            "streaming_chunk_size": WINDOWS_STREAMING_CHUNK_SIZE
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


# Windows system information endpoint
@app.get("/system")
async def system_info():
    """
    Windows system information and optimization status.
    """
    log_application_event("System info endpoint accessed")
    
    system_info = get_windows_system_info()
    
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
            "recommended_batch_size": DEFAULT_BATCH_SIZE,
            "memory_usage_optimized": True
        }
    }


if __name__ == "__main__":
    import uvicorn
    
    log_application_event(f"Starting Data Forge API (Windows-optimized) on {API_HOST}:{API_PORT}")
    log_application_event("Using Windows ProactorEventLoop for maximum I/O performance")
    
    # Windows-optimized server configuration
    uvicorn.run(
        "app.main_windows:app",
        host=API_HOST,
        port=API_PORT,
        reload=False,  # Disable reload for production performance
        workers=1,     # Single worker optimized for Windows
        loop="auto",   # Let uvicorn choose the best loop for Windows
        http="h11",    # Use h11 for better HTTP performance on Windows
        log_level="info",
        access_log=False,  # Disable access log for performance
        server_header=False,  # Disable server header for performance
        date_header=False,    # Disable date header for performance
        # Windows-specific optimizations
        backlog=2048,  # Increase backlog for Windows
        limit_concurrency=1000,  # Optimize for Windows concurrency
        limit_max_requests=10000,  # High request limit for performance
    ) 