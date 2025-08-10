"""
Application factory for creating and configuring the FastAPI application.
Separates application creation from server configuration for better testing and modularity.
"""
from contextlib import asynccontextmanager
from typing import Dict, Any
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import os

# Use global configuration
from app.config.global_settings import APIConfig, DEBUG
from app.config.logging_config import logger, stop_logging
from app.config.logging_utils import log_application_event

# Use the new global startup manager
from app.core.startup import create_lifespan_manager

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
    Application lifespan using global startup manager.
    """
    log_application_event("FastAPI application startup", f"port {APIConfig.PORT}")
    
    # Use the global startup manager
    async with create_lifespan_manager() as initialization_result:
        log_application_event("Global startup manager completed successfully")
        yield initialization_result
    
    log_application_event("FastAPI application shutdown - stopping log listener")
    stop_logging()


def create_application() -> FastAPI:
    """
    Application factory function.
    Creates and configures the FastAPI application with all routes and middleware.
    
    Returns:
        FastAPI: Configured application instance
    """
    # Create FastAPI application with portable settings
    app = FastAPI(
        title="Data Forge API",
        description="High-performance RESTful API for data processing (machine-adaptive).",
        version="0.0.2",
        debug=DEBUG,  # Use global configuration
        lifespan=lifespan,
        # Performance optimizations
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
