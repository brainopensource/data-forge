"""
Read endpoints. Blocking engine work runs in FastAPI's threadpool (sync handler).
"""
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query

from app.api.responses.response import ArrowResponse, SchemaName
from app.core.io_operations import read_table

router = APIRouter(prefix="/read", tags=["reads"])


@router.get("/{engine}/{schema_name}")
def read(
    engine: Literal["polars", "arrow", "duckdb"],
    schema_name: SchemaName,
    columns: Optional[str] = Query(None, description="Comma-separated column projection"),
    limit: Optional[int] = Query(None, ge=0),
):
    """Scan all parquet parts of a schema and return an Arrow IPC stream."""
    try:
        table = read_table(schema_name, engine, columns.split(",") if columns else None, limit)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return ArrowResponse(table, filename=f"{schema_name}_{engine}.arrow")
