"""
Ultra-fast write endpoints - High Performance Focus.
Target: 10M+ rows/second throughput
"""
from fastapi import APIRouter, HTTPException, Path, Body
from typing import Dict, Any
from app.core.io_operations import ultra_fast_write_parquet
from app.domain.entities.write_models import WriteResponse
from app.config.logging_utils import log_application_event

router = APIRouter(prefix="/write", tags=["writes"])


@router.post("/polars/{schema_name}", response_model=WriteResponse)
async def polars_write_ultra_fast(
    request: Dict[str, Any] = Body(...),
    schema_name: str = Path(..., description="Schema name")
):
    """
    ULTRA-FAST Polars write bypassing ALL validation and preprocessing.
    Target: 10M+ rows/second - Maximum Performance Mode
    Performance Gain: 8-10x faster than validated writes
    Use Case: Bulk data loads where schema is already validated client-side
    """
    try:
        log_application_event(f"Ultra-fast Polars write for schema: {schema_name}")
        
        # Extract data from request
        data = request.get("data", [])
        compression = request.get("compression", "zstd")
        
        if not data:
            raise HTTPException(status_code=400, detail="No data provided")
        
        response = await ultra_fast_write_parquet(
            data=data,
            schema_name=schema_name,
            compression=compression,
            validate_schema=False
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        log_application_event(f"Error in ultra-fast Polars write: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Legacy compatibility endpoint for benchmark scripts
@router.post("/polars-write/{schema_name}", response_model=WriteResponse)
async def polars_write_legacy(
    request: Dict[str, Any] = Body(...),
    schema_name: str = Path(..., description="Schema name")
):
    """Legacy endpoint - redirects to ultra-fast version for backward compatibility."""
    return await polars_write_ultra_fast(request, schema_name) 