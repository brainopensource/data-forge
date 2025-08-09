"""
Application factory for creating and configuring the FastAPI application.
Separates application creation from server configuration for better testing and modularity.
"""
from contextlib import asynccontextmanager
from typing import Dict, Any
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import os

# Initialize performance libraries and optimizations (lazy loading)
from app.core.init import ensure_high_performance_init, HighPerformanceInit

# Core modules
from app.core.config_windows import API_PORT
from app.config.logging_config import logger, stop_logging
from app.config.logging_utils import log_application_event

# API route imports
from app.api.routes.health import router as health_router
from app.api.routes.schemas import router as schemas_router
from app.api.routes.reads import router as reads_router
from app.api.routes.writes import router as writes_router
from app.api.routes.info import router as info_router
from app.api.routes.docs import router as docs_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan using professional startup manager.
    Handles all initialization through modular, testable components.
    """
    # High-performance runtime setup (event loop + library tuning)
    try:
        HighPerformanceInit.setup_high_performance_event_loop()
        ensure_high_performance_init()
        log_application_event("High-performance runtime initialized (event loop + libs)")
    except Exception as e:
        logger.warning(f"High-performance init encountered an issue: {e}")

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


def create_application() -> FastAPI:
    """
    Application factory function.
    Creates and configures the FastAPI application with all routes and middleware.
    
    Returns:
        FastAPI: Configured application instance
    """
    # Create FastAPI application with Windows-optimized settings
    app = FastAPI(
        title="Data Forge API - Windows",
        description="A Windows-optimized, high-performance RESTful API for data processing. Target: 10M+ rows/second on Windows.",
        version="0.0.2-windows",
        debug=False,  # Disable debug for production performance
        lifespan=lifespan,
        # Windows performance optimizations
        generate_unique_id_function=lambda route: f"{route.tags[0]}-{route.name}" if route.tags else route.name,
        # Disable default docs to customize with favicon
        docs_url=None,
        redoc_url="/redoc",
    )
    
    # Include all route modules in logical order
    _include_routers(app)
    
    # Mount static files
    _mount_static_files(app)
    
    return app


def _include_routers(app: FastAPI) -> None:
    """Include all API routers in the application."""
    routers = [
        health_router,      # System health endpoints
        docs_router,        # Documentation endpoints
        schemas_router,     # Schema management
        reads_router,       # Data reading operations
        writes_router,      # Data writing operations
        info_router,        # Information endpoints
    ]
    
    for router in routers:
        app.include_router(router)


def _mount_static_files(app: FastAPI) -> None:
    """Mount static file directories."""
    static_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
        'static'
    )
    
    if os.path.exists(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
        log_application_event(f"Static files mounted from: {static_dir}")
    else:
        logger.warning(f"Static directory not found: {static_dir}")


# Create the application instance
app = create_application()
