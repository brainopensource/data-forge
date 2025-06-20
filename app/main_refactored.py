"""
Data Forge API - Refactored Ultra-Performance Version
Target: 10M+ rows/second throughput

Modular, testable, and professional architecture optimized for extreme performance.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
import polars as pl
import pyarrow as pa
import duckdb
import os

# Core performance modules
from app.core.config import (
    API_PORT, API_HOST, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT, 
    ARROW_MEMORY_POOL_SIZE, ensure_directories
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
    Application lifespan with performance optimizations.
    Startup optimizations for maximum write/read performance.
    """
    # Startup
    log_application_event("FastAPI application startup with ULTRA-PERFORMANCE OPTIMIZATIONS", f"port {API_PORT}")
    
    try:
        # Ensure required directories exist
        ensure_directories()
        log_application_event("Required directories created")
        
        # Configure Arrow memory pool for better performance
        pa.set_memory_pool(pa.system_memory_pool())
        log_application_event("Arrow memory pool configured for optimal performance")
        
        # Set Polars optimizations
        pl.Config.set_tbl_rows(100)  # Limit table display for speed
        pl.Config.set_fmt_str_lengths(50)  # Limit string formatting
        log_application_event("Polars configured for optimized performance")
        
        # Configure DuckDB for high-performance operations
        try:
            default_con = duckdb.connect(":memory:")
            default_con.execute(f"SET threads={DUCKDB_THREADS}")
            default_con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
            default_con.execute(f"SET max_memory='{DUCKDB_MEMORY_LIMIT}'")
            default_con.execute("SET temp_directory='temp'")
            default_con.close()
            log_application_event("DuckDB optimized for high-performance operations")
        except Exception as e:
            logger.warning(f"DuckDB optimization failed: {e}")
        
        log_application_event("ULTRA-PERFORMANCE optimizations applied successfully")
        
        yield
        
    finally:
        # Shutdown
        log_application_event("FastAPI application shutdown")


# Create FastAPI application with optimized settings
app = FastAPI(
    title="Data Forge API - Ultra Performance",
    description="A modular, ultra-high-performance RESTful API for data processing and analysis. Target: 10M+ rows/second.",
    version="2.0.0",
    debug=False,  # Disable debug for production performance
    lifespan=lifespan,
    # Performance optimizations
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
        "message": "Data Forge API - Ultra Performance Mode",
        "project_name": "Data Forge",
        "version": "2.0.0",
        "performance_target": "10M+ rows/second",
        "architecture": "modular",
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
    Performance configuration and capabilities.
    """
    log_application_event("Performance info endpoint accessed")
    
    from app.core.config import (
        PARQUET_ROW_GROUP_SIZE, POLARS_INFER_SCHEMA_LENGTH, 
        DEFAULT_BATCH_SIZE, ULTRA_FAST_INFER_LENGTH
    )
    
    return {
        "performance_mode": "ultra-fast",
        "target_throughput": "10M+ rows/second",
        "optimizations": {
            "parquet_row_group_size": PARQUET_ROW_GROUP_SIZE,
            "polars_infer_length": POLARS_INFER_SCHEMA_LENGTH,
            "ultra_fast_infer_length": ULTRA_FAST_INFER_LENGTH,
            "default_batch_size": DEFAULT_BATCH_SIZE,
            "duckdb_threads": DUCKDB_THREADS,
            "duckdb_memory_limit": DUCKDB_MEMORY_LIMIT
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
            "ultra_fast_writes": "/write/ultra-fast/{schema_name}",
            "validated_writes": "/write/fast-validated/{schema_name}",
            "feather_writes": "/write/feather/{schema_name}",
            "batch_writes": "/write/batch/{schema_name}",
            "duckdb_writes": "/write/duckdb/{table_name}",
            "polars_reads": "/read/polars/{schema_name}",
            "duckdb_reads": "/read/duckdb/{schema_name}",
            "latest_reads": "/read/latest/{schema_name}"
        }
    }


if __name__ == "__main__":
    import uvicorn
    
    log_application_event(f"Starting Data Forge API on {API_HOST}:{API_PORT}")
    
    # Determine the best event loop
    loop_type = "auto"
    try:
        import platform
        if platform.system() != 'Windows':
            import uvloop
            loop_type = "uvloop"
            log_application_event("Using uvloop for enhanced performance")
        else:
            log_application_event("Windows detected, using default asyncio event loop (uvloop not supported)")
    except ImportError:
        log_application_event("uvloop not available, using default event loop")
    
    # Ultra-high performance server configuration
    uvicorn.run(
        "app.main_refactored:app",
        host=API_HOST,
        port=API_PORT,
        reload=False,  # Disable reload for production performance
        workers=1,     # Single worker for maximum performance
        loop=loop_type, # Use uvloop if available
        http="h11",    # Use h11 for better HTTP performance
        log_level="info",
        access_log=False,  # Disable access log for performance
        server_header=False,  # Disable server header for performance
        date_header=False,    # Disable date header for performance
    ) 