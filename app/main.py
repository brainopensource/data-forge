from fastapi import FastAPI, HTTPException
import logging
from app.config.logging_config import logger
import polars as pl
import os
from fastapi.responses import StreamingResponse, Response
import io
import pyarrow as pa
import pyarrow.ipc as ipc
import duckdb
import pyarrow.parquet as pq


# Global configuration for data source
DATA_DIR = "data"
PARQUET_FILE_TEMPLATE = "{schema_name}_data_1M.parquet"

def get_parquet_path(schema_name: str) -> str:
    return os.path.join(DATA_DIR, PARQUET_FILE_TEMPLATE.format(schema_name=schema_name))


app = FastAPI(
    title='Data Forge API',
    description='A RESTful API for the Data Forge project, providing endpoints for data processing and analysis.',
    debug=True,
    version="0.4.0"
)

@app.on_event("startup")
async def startup_event():
    logger.info("FastAPI application startup.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("FastAPI application shutdown.")

@app.get("/")
async def read_root():
    logger.info("Root endpoint accessed.")
    """
    A simple endpoint to check if the API is running.
    """
    return {"message": "Platform operational", "project_name": 'Data Forge'}

@app.get("/health")
async def health_check():
    logger.info("Health check endpoint accessed.")
    """
    Health check endpoint to verify the API is operational.
    """
    return {"status": "healthy", "project_name": 'Data Forge'}


@app.get("/polars-read-1/{schema_name}")
async def polars_read_1(schema_name: str):
    """
    Bulk read endpoint to fetch all records for a given schema from a Parquet file.
    """
    parquet_path = get_parquet_path(schema_name)
    if not os.path.exists(parquet_path):
        logger.warning(f"Parquet file not found for schema: {schema_name}")
        raise HTTPException(status_code=404, detail=f"No data found for schema '{schema_name}'")
    try:
        df = pl.read_parquet(parquet_path)
        logger.info(f"Read {len(df)} records from {parquet_path}")
        return df.to_dicts()
    except Exception as e:
        logger.error(f"Error reading parquet for schema {schema_name}: {e}")
        raise HTTPException(status_code=500, detail="Error reading data file.")


@app.get("/polars-read-2/{schema_name}")
async def polars_read_2(schema_name: str):
    """
    Serves a Polars DataFrame as an Apache Arrow IPC stream using a custom ArrowResponse.
    """
    parquet_path = get_parquet_path(schema_name)
    if not os.path.exists(parquet_path):
        logger.warning(f"Parquet file not found for schema: {schema_name}")
        raise HTTPException(status_code=404, detail=f"No data found for schema '{schema_name}'")
    try:
        df = pl.read_parquet(parquet_path)
        arrow_table = df.to_arrow()
        logger.info(f"[polars-read-2] Read from {parquet_path}")
        return ArrowResponse(arrow_table, headers={"Content-Disposition": f"attachment; filename={schema_name}.arrow"})
    except Exception as e:
        logger.error(f"Error reading parquet for schema {schema_name} (polars-read-3): {e}")
        raise HTTPException(status_code=500, detail="Error reading data file.")


@app.get("/duckdb-read/{schema_name}")
async def duckdb_read(schema_name: str):
    """
    Ultra-fast bulk read using DuckDB's optimized Parquet reader.
    """
    parquet_path = get_parquet_path(schema_name)
    if not os.path.exists(parquet_path):
        logger.warning(f"Parquet file not found for schema: {schema_name}")
        raise HTTPException(status_code=404, detail=f"No data found for schema '{schema_name}'")
    
    try:
        # DuckDB can read Parquet directly and output Arrow
        conn = duckdb.connect()
        arrow_table = conn.execute(f"SELECT * FROM read_parquet('{parquet_path}')").fetch_arrow_table()
        logger.info(f"[duckdb-read] Read {len(arrow_table)} records from {parquet_path}")
        return ArrowResponse(arrow_table, headers={"Content-Disposition": f"attachment; filename={schema_name}.arrow"})
    except Exception as e:
        logger.error(f"Error reading parquet with DuckDB for schema {schema_name}: {e}")
        raise HTTPException(status_code=500, detail="Error reading data file.")


@app.get("/pyarrow-read/{schema_name}")
async def pyarrow_read(schema_name: str):
    """
    Direct PyArrow Parquet reading - often fastest for pure Arrow operations.
    """
    parquet_path = get_parquet_path(schema_name)
    if not os.path.exists(parquet_path):
        logger.warning(f"Parquet file not found for schema: {schema_name}")
        raise HTTPException(status_code=404, detail=f"No data found for schema '{schema_name}'")
    
    try:
        # Direct PyArrow read - no conversions
        arrow_table = pq.read_table(parquet_path)
        logger.info(f"[pyarrow-read] Read {len(arrow_table)} records from {parquet_path}")
        return ArrowResponse(arrow_table, headers={"Content-Disposition": f"attachment; filename={schema_name}.arrow"})
    except Exception as e:
        logger.error(f"Error reading parquet with PyArrow for schema {schema_name}: {e}")
        raise HTTPException(status_code=500, detail="Error reading data file.")


class ArrowResponse(Response):
    media_type = "application/vnd.apache.arrow.stream"
    def __init__(self, table: pa.Table, **kwargs):
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        content = sink.getvalue().to_pybytes()
        super().__init__(content=content, media_type=self.media_type, **kwargs)