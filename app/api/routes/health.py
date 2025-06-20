"""
Health check and system status endpoints.
"""
from fastapi import APIRouter
from app.config.logging_utils import log_application_event
from app.api.responses.response import FastJSONResponse

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/")
async def health_check():
    """
    Health check endpoint to verify the API is operational.
    """
    log_application_event("Health check endpoint accessed")
    return FastJSONResponse({
        "status": "healthy", 
        "project_name": "Data Forge",
        "version": "2.0.0",
        "performance_mode": "ultra-fast"
    })


@router.get("/status")
async def system_status():
    """
    Detailed system status with performance metrics.
    """
    log_application_event("System status endpoint accessed")
    
    # Try to get system metrics, fallback if psutil not available
    try:
        import psutil
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('.')
        
        system_metrics = {
            "cpu_usage_percent": cpu_percent,
            "memory_usage_percent": memory.percent,
            "memory_available_gb": round(memory.available / (1024**3), 2),
            "disk_usage_percent": disk.percent,
            "disk_free_gb": round(disk.free / (1024**3), 2)
        }
    except ImportError:
        system_metrics = {
            "cpu_usage_percent": "N/A (psutil not installed)",
            "memory_usage_percent": "N/A (psutil not installed)",
            "memory_available_gb": "N/A (psutil not installed)",
            "disk_usage_percent": "N/A (psutil not installed)",
            "disk_free_gb": "N/A (psutil not installed)"
        }
    
    status = {
        "status": "operational",
        "system": system_metrics,
        "performance": {
            "target_throughput": "10M+ rows/second",
            "optimization_level": "ultra-fast",
            "validation_mode": "optional"
        },
        "features": {
            "arrow_streaming": True,
            "duckdb_integration": True,
            "polars_optimization": True,
            "schema_validation": True
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