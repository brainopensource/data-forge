"""
Documentation and favicon endpoints.
Handles custom Swagger UI with favicon and static file serving.
"""
import os
from fastapi import APIRouter
from fastapi.responses import FileResponse
from fastapi.openapi.docs import get_swagger_ui_html
from app.config.logging_utils import log_application_event

router = APIRouter(tags=["docs"])


@router.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    """Custom Swagger UI with our favicon."""
    log_application_event("Custom Swagger UI accessed")
    
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="Data Forge API - Documentation",
        swagger_favicon_url="/static/images/favicon.ico",
    )


@router.get('/favicon.ico', include_in_schema=False)
async def favicon():
    """Serve the favicon.ico file from static/images/."""
    log_application_event("Favicon requested")
    
    # Get the static directory path
    static_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'static')
    favicon_path = os.path.join(static_dir, 'images', 'favicon.ico')
    
    return FileResponse(favicon_path, media_type='image/x-icon')
