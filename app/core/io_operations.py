"""
Parquet dataset I/O. Each schema is a directory of immutable part files:
data/tables/<schema>/*.parquet. Writes add a part; reads scan all parts.
"""
import glob
import os
import threading
import time
import uuid
from typing import Callable, Dict, List, Optional

import orjson
import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from app.config.global_settings import DataConfig, WriteProfiles, create_optimized_duckdb_connection
from app.config.logging_utils import log_operation
from app.domain.entities.write_models import WriteResponse

ARROW_STREAM = "application/vnd.apache.arrow.stream"
COMPRESSIONS = {"zstd", "snappy", "lz4", "gzip", "brotli", "uncompressed"}
PROFILE = WriteProfiles.ULTRA_FAST

_duckdb = None
_duckdb_lock = threading.Lock()


def duckdb_cursor():
    """Per-call cursor on one shared, pre-configured DuckDB database (cursors are thread-safe)."""
    global _duckdb
    if _duckdb is None:
        with _duckdb_lock:
            if _duckdb is None:
                _duckdb = create_optimized_duckdb_connection()
    return _duckdb.cursor()


def dataset_files(schema_name: str) -> List[str]:
    files = sorted(glob.glob(os.path.join(DataConfig.TABLES_DIR, schema_name, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No parquet files found for schema '{schema_name}'")
    return files


# ============================================================================
# READS
# ============================================================================

def _read_polars(files: List[str], columns: Optional[List[str]], limit: Optional[int]) -> pa.Table:
    lf = pl.scan_parquet(files)
    if columns:
        lf = lf.select(columns)
    if limit is not None:
        lf = lf.head(limit)
    # Newest compat level keeps Polars' string views: no conversion copy on export.
    return lf.collect().to_arrow(compat_level=pl.CompatLevel.newest())


def _read_arrow(files: List[str], columns: Optional[List[str]], limit: Optional[int]) -> pa.Table:
    table = pq.read_table(files, columns=columns, memory_map=True)
    return table if limit is None else table.slice(0, limit)


def _read_duckdb(files: List[str], columns: Optional[List[str]], limit: Optional[int]) -> pa.Table:
    with duckdb_cursor() as cur:
        rel = cur.read_parquet(files)
        if columns:
            rel = rel.select(*(f'"{c.replace(chr(34), chr(34) * 2)}"' for c in columns))
        if limit is not None:
            rel = rel.limit(limit)
        return rel.arrow()


READERS: Dict[str, Callable[..., pa.Table]] = {
    "polars": _read_polars,
    "arrow": _read_arrow,
    "duckdb": _read_duckdb,
}


def read_table(schema_name: str, engine: str, columns: Optional[List[str]] = None,
               limit: Optional[int] = None) -> pa.Table:
    start = time.perf_counter()
    table = READERS[engine](dataset_files(schema_name), columns, limit)
    log_operation("read", engine, table.num_rows, time.perf_counter() - start)
    return table


# ============================================================================
# WRITES
# ============================================================================

def parse_body(body: bytes, content_type: str) -> tuple[pl.DataFrame, Optional[str]]:
    """Arrow IPC stream body (zero-copy) or JSON {"data": [...], "compression": ...}."""
    if content_type.startswith(ARROW_STREAM):
        return pl.from_arrow(ipc.open_stream(body).read_all()), None
    payload = orjson.loads(body)
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not rows:
        raise ValueError("No data provided")
    return pl.from_dicts(rows, infer_schema_length=PROFILE["infer_schema_length"]), payload.get("compression")


def _write_polars(df: pl.DataFrame, path: str, compression: str) -> None:
    df.write_parquet(path, compression=compression, compression_level=_level(compression), statistics=True)


def _write_duckdb(df: pl.DataFrame, path: str, compression: str) -> None:
    with duckdb_cursor() as cur:
        cur.register("incoming", df.to_arrow())
        cur.execute(f"COPY incoming TO '{path}' (FORMAT PARQUET, COMPRESSION {compression.upper()})")


WRITERS: Dict[str, Callable[[pl.DataFrame, str, str], None]] = {
    "polars": _write_polars,
    "duckdb": _write_duckdb,
}


def _level(compression: str) -> Optional[int]:
    return PROFILE["compression_level"] if compression == "zstd" else None


def write_table(schema_name: str, engine: str, body: bytes, content_type: str,
                compression: Optional[str] = None) -> WriteResponse:
    start = time.perf_counter()
    df, body_compression = parse_body(body, content_type)
    compression = (compression or body_compression or PROFILE["compression"]).lower()
    if compression not in COMPRESSIONS:
        raise ValueError(f"Compression must be one of {sorted(COMPRESSIONS)}")

    directory = os.path.join(DataConfig.TABLES_DIR, schema_name)
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"part-{uuid.uuid4().hex}.parquet")
    tmp = path + ".tmp"  # readers glob *.parquet, so partial files are never visible
    WRITERS[engine](df, tmp, compression)
    os.replace(tmp, path)

    elapsed = time.perf_counter() - start
    log_operation("write", engine, df.height, elapsed)
    return WriteResponse(
        success=True,
        message=f"{engine}: {df.height} records",
        records_written=df.height,
        schema_name=schema_name,
        file_path=path,
        write_time_seconds=round(elapsed, 3),
        throughput_records_per_second=int(df.height / elapsed) if elapsed > 0 else 0,
        file_size_mb=round(os.path.getsize(path) / 2**20, 2),
    )
