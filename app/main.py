from fastapi import FastAPI, HTTPException
import logging
from app.config.logging_config import logger
import polars as pl
import os
from fastapi.responses import StreamingResponse, Response, FileResponse
import io
import pyarrow as pa
import pyarrow.ipc as ipc
import duckdb
import pyarrow.parquet as pq
import tempfile


# Global configuration for data source
DATA_DIR = "data"
PARQUET_FILE_TEMPLATE = "{schema_name}_data_100K.parquet"
FEATHER_FILE_TEMPLATE = "{schema_name}_data_100K.feather"
#PARQUET_ZSTD_FILE_TEMPLATE = "{schema_name}_data_100K_zstd.parquet"

def get_parquet_path(schema_name: str) -> str:
    return os.path.join(DATA_DIR, PARQUET_FILE_TEMPLATE.format(schema_name=schema_name))

def get_feather_path(schema_name: str) -> str:
    return os.path.join(DATA_DIR, FEATHER_FILE_TEMPLATE.format(schema_name=schema_name))

#def get_parquet_zstd_path(schema_name: str) -> str:
#    return os.path.join(DATA_DIR, PARQUET_ZSTD_FILE_TEMPLATE.format(schema_name=schema_name))


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


@app.get("/polars-read/{schema_name}")
async def polars_read(schema_name: str):
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
        logger.info(f"[duckdb-read] Read records from {parquet_path}")
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
        logger.info(f"[pyarrow-read] Read from {parquet_path}")
        return ArrowResponse(arrow_table, headers={"Content-Disposition": f"attachment; filename={schema_name}.arrow"})
    except Exception as e:
        logger.error(f"Error reading parquet with PyArrow for schema {schema_name}: {e}")
        raise HTTPException(status_code=500, detail="Error reading data file.")


@app.get("/feather-read/{schema_name}")
async def feather_read(schema_name: str):
    """
    Streams the Feather (Arrow IPC file format) file directly from disk for true benchmarking.
    """
    feather_path = get_feather_path(schema_name)
    if not os.path.exists(feather_path):
        logger.warning(f"Feather file not found for schema: {schema_name}")
        raise HTTPException(status_code=404, detail=f"No feather file found for schema '{schema_name}'")
    try:
        logger.info(f"[feather-read] Streaming feather file from {feather_path}")
        return FileResponse(
            feather_path,
            media_type="application/vnd.apache.feather",
            filename=f"{schema_name}.feather",
            headers={"Content-Disposition": f"attachment; filename={schema_name}.feather"}
        )
    except Exception as e:
        logger.error(f"Error streaming feather file for schema {schema_name} (feather-read): {e}")
        raise HTTPException(status_code=500, detail="Error reading feather file.")


class ArrowResponse(Response):
    media_type = "application/vnd.apache.arrow.stream"
    def __init__(self, table: pa.Table, **kwargs):
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        content = sink.getvalue().to_pybytes()
        super().__init__(content=content, media_type=self.media_type, **kwargs)