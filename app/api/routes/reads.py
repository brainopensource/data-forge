"""
High-Performance read endpoints
"""
from fastapi import APIRouter, HTTPException, Path
from app.core.io_operations import (
    ultra_fast_polars_read, 
    ultra_fast_duckdb_read
)
from app.api.responses.response import ArrowResponse
from app.config.logging_utils import log_application_event

router = APIRouter(prefix="/read", tags=["reads"])


@router.get("/polars/{schema_name}")
async def polars_read_ultra_fast(schema_name: str = Path(..., description="Schema name")):
    """
    Polars read operation.
    Returns: Apache Arrow IPC stream
    """
    try:
        log_application_event(f"Polars read for schema: {schema_name}")
        arrow_table = await ultra_fast_polars_read(schema_name)
        
        return ArrowResponse(
            arrow_table, 
            filename=f"{schema_name}_polars.arrow"
        )
        
    except FileNotFoundError as e:
        log_application_event(f"File not found for schema {schema_name}: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log_application_event(f"Error in Polars read: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")


@router.get("/duckdb/{schema_name}")
async def duckdb_read_ultra_fast(schema_name: str = Path(..., description="Schema name")):
    """
    DuckDB read operation.
    Returns: Apache Arrow IPC stream
    """
    try:
        log_application_event(f"DuckDB read for schema: {schema_name}")
        arrow_table = await ultra_fast_duckdb_read(schema_name)
        
        return ArrowResponse(
            arrow_table, 
            filename=f"{schema_name}_duckdb.arrow"
        )
        
    except FileNotFoundError as e:
        log_application_event(f"File not found for schema {schema_name}: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log_application_event(f"Error in DuckDB read: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")


# Legacy compatibility endpoints for benchmark scripts
@router.get("/polars-read/{schema_name}")
async def polars_read_legacy(schema_name: str = Path(..., description="Schema name")):
    """Legacy endpoint - redirects to version for backward compatibility."""
    return await polars_read_ultra_fast(schema_name)


@router.get("/duckdb-read/{schema_name}")
async def duckdb_read_legacy(schema_name: str = Path(..., description="Schema name")):
    """Legacy endpoint - redirects to version for backward compatibility."""
    return await duckdb_read_ultra_fast(schema_name) 