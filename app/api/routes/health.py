"""
Health check and system status endpoints.
Enhanced with professional health monitoring system.
"""
from fastapi import APIRouter
from app.config.logging_utils import log_application_event
from app.api.responses.response import FastJSONResponse
from app.core.health import health_monitor

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/")
async def health_check():
    """
    Quick health check endpoint to verify the API is operational.
    """
    log_application_event("Health check endpoint accessed")
    health_data = await health_monitor.get_quick_health()
    
    return FastJSONResponse({
        **health_data,
        "project_name": "Data Forge",
        "version": "2.0.0-windows",
        "performance_mode": "ultra-fast"
    })


@router.get("/status")
async def system_status():
    """
    Detailed system status with comprehensive health checks.
    """
    log_application_event("System status endpoint accessed")
    
    # Get comprehensive health status
    health_data = await health_monitor.get_comprehensive_health()
    
    # Add additional context
    status = {
        **health_data,
        "project_name": "Data Forge",
        "version": "2.0.0-windows",
        "performance": {
            "target_throughput": "10M+ rows/second",
            "optimization_level": "ultra-fast",
            "validation_mode": "optional"
        },
        "features": {
            "arrow_streaming": True,
            "duckdb_integration": True,
            "polars_optimization": True,
            "schema_validation": True,
            "professional_startup": True,
            "comprehensive_monitoring": True
        }
    }
    
    return FastJSONResponse(status)


@router.get("/")
async def root():
    """
    Root endpoint with basic API information.
    """
    log_application_event("Root endpoint accessed")
    return FastJSONResponse({
        "message": "Data Forge API - Ultra Performance Mode", 
        "project_name": "Data Forge",
        "version": "2.0.0",
        "docs_url": "/docs",
        "health_url": "/health"
    }) 