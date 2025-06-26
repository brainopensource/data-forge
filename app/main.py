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
    DEFAULT_BATCH_SIZE, get_windows_system_info
)

# API routes
from app.api.routes.health import router as health_router
from app.api.routes.schemas import router as schemas_router
from app.api.routes.reads import router as reads_router
from app.api.routes.writes import router as writes_router
from app.api.routes.info import router as info_router

# Configuration and logging
from app.config.logging_config import logger, stop_logging
from app.config.logging_utils import log_application_event


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan using professional startup manager.
    Handles all initialization through modular, testable components.
    """
    from app.core.startup import startup_manager
    
    # Startup using professional startup manager
    log_application_event("FastAPI application startup - WINDOWS MODE", f"port {API_PORT}")
    
    initialization_result = await startup_manager.initialize_application()
    
    if initialization_result['status'] != 'success':
        logger.error("Application startup failed")
        raise RuntimeError("Application initialization failed")
    
    log_application_event("Windows optimizations applied successfully")
    
    try:
        yield initialization_result
    finally:
        # Shutdown using professional cleanup
        await startup_manager.cleanup_application()
        log_application_event("FastAPI application shutdown - stopping log listener")
        stop_logging()


# Create FastAPI application with Windows-optimized settings
app = FastAPI(
    title="Data Forge API - Windows",
    description="A Windows-optimized, high-performance RESTful API for data processing. Target: 10M+ rows/second on Windows.",
    version="0.0.2-windows",
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
app.include_router(info_router)


if __name__ == "__main__":
    import uvicorn
    
    log_application_event(f"Starting Data Forge API (Windows-optimized) on {API_HOST}:{API_PORT}")
    log_application_event("Using Windows ProactorEventLoop for maximum I/O performance")
    
    # Windows-optimized server configuration
    uvicorn.run(
        "app.main:app",
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
