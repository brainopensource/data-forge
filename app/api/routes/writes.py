"""
Write endpoints. Body is an Arrow IPC stream (fast path) or JSON {"data": [...]}.
"""
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from starlette.concurrency import run_in_threadpool

from app.api.responses.response import SchemaName
from app.core.io_operations import ARROW_STREAM, write_table
from app.domain.entities.write_models import WriteResponse

router = APIRouter(prefix="/write", tags=["writes"])


@router.post("/{engine}/{schema_name}", response_model=WriteResponse, openapi_extra={
    "requestBody": {"content": {ARROW_STREAM: {}, "application/json": {}}, "required": True},
})
async def write(
    engine: Literal["polars", "duckdb"],
    schema_name: SchemaName,
    request: Request,
    compression: Optional[str] = Query(None),
):
    """Append the payload to the schema's dataset as a new parquet part."""
    body = await request.body()
    try:
        return await run_in_threadpool(
            write_table, schema_name, engine, body, request.headers.get("content-type", ""), compression
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
