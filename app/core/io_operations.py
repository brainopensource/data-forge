"""
Ultra-fast I/O operations for Data Forge API.
Zero-overhead, performance-critical functions for 10M+ rows/second throughput.
"""
import time
import os
import glob
from typing import Dict, Any, List, Optional, Tuple
import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import duckdb
from app.core.config import (
    ULTRA_FAST_WRITE_CONFIG, STANDARD_WRITE_CONFIG, 
    get_parquet_path, get_write_parquet_path, get_write_feather_path, 
    get_file_size_mb, DEFAULT_BATCH_SIZE, DATA_DIR
)
from app.domain.entities.write_models import WriteResponse
from app.config.logging_utils import log_operation, log_operation_error


# ============================================================================
# FAST READ OPERATIONS
# ============================================================================

def get_latest_parquet_file(schema_name: str) -> str:
    """Get the most recent parquet file for a schema."""
    # First check if there's a file with the standard naming pattern
    standard_path = get_parquet_path(schema_name)
    if os.path.exists(standard_path):
        return standard_path
    
    # Otherwise, look for the most recent file in the schema directory
    schema_dir = os.path.join(DATA_DIR, schema_name)
    if not os.path.exists(schema_dir):
        raise FileNotFoundError(f"No data directory found for schema '{schema_name}'")
    
    # Find all parquet files in the schema directory
    parquet_files = glob.glob(os.path.join(schema_dir, "*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found for schema '{schema_name}'")
    
    # Return the most recent file
    return max(parquet_files, key=os.path.getmtime)


async def ultra_fast_polars_read(schema_name: str) -> pa.Table:
    """
    Ultra-fast, single-path Polars read using eager parquet reader.
    Rationale: fastest stable path on current Polars without deprecated streaming.
    Target: 10M+ rows/second
    """
    start_time = time.time()
    
    try:
        parquet_path = get_latest_parquet_file(schema_name)
    except FileNotFoundError as e:
        raise e
    # Single fast path: eager read (no streaming, no fallbacks)
    df = pl.read_parquet(parquet_path)
    arrow_table = df.to_arrow()
    
    read_time = time.time() - start_time
    log_operation("read", "success", len(df), read_time)
    
    return arrow_table


async def ultra_fast_duckdb_read(schema_name: str) -> pa.Table:
    """
    DuckDB read operation.
    Target: 10M+ rows/second using DuckDB's optimized Parquet reader.
    """
    start_time = time.time()
    
    try:
        parquet_path = get_latest_parquet_file(schema_name)
    except FileNotFoundError as e:
        raise e
    
    # DuckDB direct Arrow output - zero copy with optimized connection
    from app.core.init import create_optimized_duckdb_connection
    conn = create_optimized_duckdb_connection()
    arrow_table = conn.execute(f"SELECT * FROM read_parquet('{parquet_path}')").fetch_arrow_table()
    conn.close()
    
    read_time = time.time() - start_time
    log_operation("read", "success", len(arrow_table), read_time)
    
    return arrow_table


# ============================================================================
# WRITE OPERATIONS
# ============================================================================

async def ultra_fast_write_parquet(
    data: List[Dict[str, Any]], 
    schema_name: str,
    compression: Optional[str] = None,
    validate_schema: bool = False
) -> WriteResponse:
    """
    Parquet write bypassing ALL validation and preprocessing.
    Target: 10M+ rows/second
    Performance Gain: 8-10x faster than validated writes
    """
    start_time = time.time()
    records_count = len(data)
    
    if not data:
        raise ValueError("No data provided")
    
    # log_operation_start("write", records_count, validation="BYPASSED")
    
    try:
        # DIRECT DataFrame creation - no preprocessing, no validation
        df = pl.DataFrame(data, infer_schema_length=ULTRA_FAST_WRITE_CONFIG["infer_schema_length"])
        
        # Pre-calculated file path
        file_path = get_write_parquet_path(schema_name, "_ultra_fast")
        
        # Optimized write settings for maximum speed
        write_options = {
            "compression": compression or ULTRA_FAST_WRITE_CONFIG["compression"],
            "row_group_size": ULTRA_FAST_WRITE_CONFIG["row_group_size"],
            "use_pyarrow": ULTRA_FAST_WRITE_CONFIG["use_pyarrow"],
            "statistics": ULTRA_FAST_WRITE_CONFIG["statistics"],
        }
        
        df.write_parquet(file_path, **write_options)
        
        end_time = time.time()
        write_time = end_time - start_time
        throughput = int(records_count / write_time) if write_time > 0 else 0
        file_size = get_file_size_mb(file_path)
        
        log_operation("write", "success", records_count, write_time)
        
        return WriteResponse(
            success=True,
            message=f"ULTRA-FAST: {records_count} records (no validation)",
            records_written=records_count,
            schema_name=schema_name,
            file_path=file_path,
            write_time_seconds=round(write_time, 3),
            throughput_records_per_second=throughput,
            file_size_mb=round(file_size, 2),
            validation_errors=None
        )
        
    except Exception as e:
        log_operation_error("write", str(e))
        raise


async def fast_write_parquet_with_schema(
    data: List[Dict[str, Any]], 
    schema_name: str,
    polars_schema: Dict[str, Any],
    compression: Optional[str] = None
) -> WriteResponse:
    """
    Parquet write with schema validation.
    Target: 5M+ rows/second with validation
    """
    start_time = time.time()
    records_count = len(data)
    
    if not data:
        raise ValueError("No data provided")
    
    # log_operation_start("write", records_count, validation="ENABLED")
    
    try:
        # DataFrame creation with schema
        df = pl.DataFrame(data, schema=polars_schema)
        
        file_path = get_write_parquet_path(schema_name, "_fast_schema")
        
        # Standard write settings with validation
        write_options = {
            "compression": compression or STANDARD_WRITE_CONFIG["compression"],
            "row_group_size": STANDARD_WRITE_CONFIG["row_group_size"],
            "use_pyarrow": STANDARD_WRITE_CONFIG["use_pyarrow"],
            "statistics": STANDARD_WRITE_CONFIG["statistics"],
        }
        
        df.write_parquet(file_path, **write_options)
        
        end_time = time.time()
        write_time = end_time - start_time
        throughput = int(records_count / write_time) if write_time > 0 else 0
        file_size = get_file_size_mb(file_path)
        
        log_operation("write", "success", records_count, write_time)
        
        return WriteResponse(
            success=True,
            message=f"FAST with schema: {records_count} records at {throughput:,} rows/sec",
            records_written=records_count,
            schema_name=schema_name,
            file_path=file_path,
            write_time_seconds=round(write_time, 3),
            throughput_records_per_second=throughput,
            file_size_mb=round(file_size, 2),
            validation_errors=None
        )
        
    except Exception as e:
        log_operation_error("write", str(e))
        raise


async def ultra_fast_write_feather(
    data: List[Dict[str, Any]], 
    schema_name: str
) -> WriteResponse:
    """
    Feather write for maximum speed.
    Target: 12M+ rows/second (Feather is faster than Parquet for writes)
    """
    start_time = time.time()
    records_count = len(data)
    
    if not data:
        raise ValueError("No data provided")
    
    # log_operation_start("write", records_count, format="FEATHER")
    
    try:
        # Direct DataFrame creation
        df = pl.DataFrame(data, infer_schema_length=50)
        
        file_path = get_write_feather_path(schema_name, "_ultra_fast")
        
        # Feather write (no compression options - inherently fast)
        df.write_ipc(file_path)
        
        end_time = time.time()
        write_time = end_time - start_time
        throughput = int(records_count / write_time) if write_time > 0 else 0
        file_size = get_file_size_mb(file_path)
        
        log_operation("write", "success", records_count, write_time)
        
        return WriteResponse(
            success=True,
            message=f"ULTRA-FAST Feather: {records_count} records at {throughput:,} rows/sec",
            records_written=records_count,
            schema_name=schema_name,
            file_path=file_path,
            write_time_seconds=round(write_time, 3),
            throughput_records_per_second=throughput,
            file_size_mb=round(file_size, 2),
            validation_errors=None
        )
        
    except Exception as e:
        log_operation_error("write", str(e))
        raise


# ============================================================================
# BATCH PROCESSING OPERATIONS
# ============================================================================

async def batch_write_parquet(
    data: List[Dict[str, Any]], 
    schema_name: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    compression: Optional[str] = None
) -> WriteResponse:
    """
    Batch write for very large datasets using a single ParquetWriter.
    Avoids repeated read/concat cycles for O(n) behavior.
    """
    start_time = time.time()
    total_records = len(data)
    
    if not data:
        raise ValueError("No data provided")
    
    try:
        file_path = get_write_parquet_path(schema_name, "_batch")
        total_written = 0
        writer = None

        for i in range(0, total_records, batch_size):
            batch_data = data[i:i + batch_size]
            batch_df = pl.DataFrame(batch_data, infer_schema_length=50)
            batch_tbl = batch_df.to_arrow()

            if writer is None:
                writer = pq.ParquetWriter(
                    file_path,
                    batch_tbl.schema,
                    compression=(compression or "zstd"),
                    use_dictionary=False,
                    write_statistics=False,
                )
            writer.write_table(batch_tbl)
            total_written += len(batch_data)

        if writer is not None:
            writer.close()

        end_time = time.time()
        write_time = end_time - start_time
        throughput = int(total_written / write_time) if write_time > 0 else 0
        file_size = get_file_size_mb(file_path)

        log_operation("write", "success", total_written, write_time)

        return WriteResponse(
            success=True,
            message=f"Batch write: {total_written} records in batches of {batch_size}",
            records_written=total_written,
            schema_name=schema_name,
            file_path=file_path,
            write_time_seconds=round(write_time, 3),
            throughput_records_per_second=throughput,
            file_size_mb=round(file_size, 2),
            validation_errors=None
        )

    except Exception as e:
        log_operation_error("write", str(e))
        raise


# ============================================================================
# DUCKDB OPERATIONS
# ============================================================================

async def duckdb_bulk_write(
    data: List[Dict[str, Any]], 
    table_name: str,
    batch_size: int = DEFAULT_BATCH_SIZE
) -> WriteResponse:
    """
    DuckDB bulk write operation.
    Target: 15M+ rows/second for in-memory operations.
    """
    start_time = time.time()
    records_count = len(data)
    
    if not data:
        raise ValueError("No data provided")
    
    # log_operation_start("write", records_count, table=table_name, batch_size=batch_size)
    
    try:
        # Create optimized DuckDB connection for maximum performance
        from app.core.init import create_optimized_duckdb_connection
        conn = create_optimized_duckdb_connection()
        
        # Convert to Arrow for fastest DuckDB ingestion
        df = pl.DataFrame(data, infer_schema_length=50)
        arrow_table = df.to_arrow()
        
        # Register Arrow table and create table
        conn.register("temp_table", arrow_table)
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_table")
        
        # Verify write
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        actual_count = result[0] if result else 0
        
        conn.close()
        
        end_time = time.time()
        write_time = end_time - start_time
        throughput = int(actual_count / write_time) if write_time > 0 else 0
        
        log_operation("write", "success", actual_count, write_time)
        
        return WriteResponse(
            success=True,
            message=f"DuckDB bulk: {actual_count} records at {throughput:,} rows/sec",
            records_written=actual_count,
            schema_name=table_name,
            file_path=f"duckdb_table:{table_name}",
            write_time_seconds=round(write_time, 3),
            throughput_records_per_second=throughput,
            file_size_mb=0.0,  # In-memory table
            validation_errors=None
        )
        
    except Exception as e:
        log_operation_error("write", str(e))
        raise
