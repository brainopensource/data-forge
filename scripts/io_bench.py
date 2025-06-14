import duckdb
import pandas as pd
import json
import time
import tempfile
import os
from pathlib import Path
from typing import List, Dict, Any
import psutil

# --- CONFIGURABLE PARAMETERS ---
# Path to the mocked response file (change for different test sizes)
MOCKED_JSON_PATH = "external/mocked_response_100K-4.json"  # e.g. "external/mocked_response_100K-4.json"

# DuckDB table name
DUCKDB_TABLE_NAME = "well_production"

# DuckDB file suffix
DUCKDB_FILE_SUFFIX = ".duckdb"

# Number of test records to generate (set to 0 to use file)
TEST_DATA_SIZE = 900_000  # Set to 0 to use file, >0 to use generated data

# --- END CONFIGURABLE PARAMETERS ---

# --- Load schema from schemas_description.py ---
from app.infrastructure.metadata.schemas_description import SCHEMAS_METADATA

# Find the well_production schema
def get_well_production_schema():
    for schema in SCHEMAS_METADATA:
        if schema["name"] == "well_production":
            return schema
    raise ValueError("well_production schema not found")


schema = get_well_production_schema()

# --- Prepare DuckDB table DDL from schema ---
def duckdb_type(py_type, db_type):
    # Map schema types to DuckDB types
    if db_type == "BIGINT":
        return "BIGINT"
    if db_type == "DOUBLE":
        return "DOUBLE"
    if db_type == "TIMESTAMP":
        return "TIMESTAMP"
    if db_type == "VARCHAR":
        return "VARCHAR"
    return "VARCHAR"  # fallback


def make_ddl(schema):
    cols = []
    for prop in schema["properties"]:
        col = f'"{prop["name"]}" {duckdb_type(prop["type"], prop["db_type"])}'
        cols.append(col)
    pk = schema.get("primary_key", [])
    pk_clause = f', PRIMARY KEY ({", ".join(pk)})' if pk else ''
    return f'CREATE TABLE {DUCKDB_TABLE_NAME} ({", ".join(cols)}{pk_clause});'


table_ddl = make_ddl(schema)


def test_data() -> List[Dict[str, Any]]:
    """Generate test data with unique composite primary keys, supporting fast repetition with offset."""
    data = []
    from datetime import datetime, timedelta
    base_date = datetime(2024, 1, 1)
    # Generate a base chunk of unique records
    CHUNK_SIZE = 1_000_000
    chunk = []
    for i in range(CHUNK_SIZE):
        prod_date = base_date + timedelta(seconds=i)
        record = {
            "field_code": i % 1000,
            "_field_name": f"Field_{i % 1000}",
            "well_code": i % 100,
            "_well_reference": f"WELL_REF_{i % 100:03d}",
            "well_name": f"Well_{i % 100}",
            "production_period": prod_date.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "days_on_production": 30,
            "oil_production_kbd": round(100.0 + (i * 0.1), 2),
            "gas_production_mmcfd": round(50.0 + (i * 0.05), 2),
            "liquids_production_kbd": round(25.0 + (i * 0.025), 2),
            "water_production_kbd": round(75.0 + (i * 0.075), 2),
            "data_source": "performance_test",
            "source_data": json.dumps({"test": f"data_{i}"}),
            "partition_0": f"partition_{i % 10}"
        }
        chunk.append(record)
    # Repeat the chunk, offsetting production_period for each repetition
    for rep in range(TEST_DATA_SIZE // CHUNK_SIZE):
        offset = rep * CHUNK_SIZE
        for rec in chunk:
            rec_copy = rec.copy()
            # Offset production_period by CHUNK_SIZE seconds per repetition
            from datetime import datetime
            prod_dt = datetime.strptime(rec_copy["production_period"], "%Y-%m-%dT%H:%M:%S+00:00")
            prod_dt = prod_dt + timedelta(seconds=offset)
            rec_copy["production_period"] = prod_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            data.append(rec_copy)
    # Add any remaining records if TEST_DATA_SIZE is not a multiple of CHUNK_SIZE
    remainder = TEST_DATA_SIZE % CHUNK_SIZE
    if remainder:
        offset = (TEST_DATA_SIZE // CHUNK_SIZE) * CHUNK_SIZE
        for i in range(remainder):
            rec = chunk[i].copy()
            prod_dt = datetime.strptime(rec["production_period"], "%Y-%m-%dT%H:%M:%S+00:00")
            prod_dt = prod_dt + timedelta(seconds=offset)
            rec["production_period"] = prod_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            data.append(rec)
    return data

# --- Load data from mocked_response.json or generate test data ---
def get_benchmark_dataset():
    if TEST_DATA_SIZE > 0:
        print(f"Generating {TEST_DATA_SIZE} test records...")
        start_df = time.time()
        records = test_data()
        df = pd.DataFrame(records)
        end_df = time.time()
    else:
        data_path = Path(MOCKED_JSON_PATH)
        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Use the list under the 'value' key
        records = data["value"]
        start_df = time.time()
        df = pd.DataFrame(records)
        end_df = time.time()
    return df, start_df, end_df


def print_results_table(results):
    # Only show selected columns
    selected_keys = ["Bulk insert (s)", "Bulk read (s)", "Total time (s)"]
    header = ["Benchmark"] + selected_keys
    rows = []
    for bench_name, bench_results in results.items():
        row = [bench_name]
        # Calculate total time if possible
        bulk_insert = bench_results.get("Bulk insert (s)", None)
        bulk_read = bench_results.get("Bulk read (s)", None)
        if isinstance(bulk_insert, float) and isinstance(bulk_read, float):
            total_time = bulk_insert + bulk_read
        else:
            total_time = "-"
        for key in selected_keys:
            if key == "Total time (s)":
                val = total_time
            else:
                val = bench_results.get(key, "-")
            if isinstance(val, float):
                val = f"{val:.4f}"
            row.append(str(val))
        rows.append(row)

    # Calculate column widths
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(header, *rows)]

    # Print header
    def print_row(row):
        print("| " + " | ".join(cell.ljust(width) for cell, width in zip(row, col_widths)) + " |")

    print("\nBenchmark Results:")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    print_row(header)
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    for row in rows:
        print_row(row)
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")


def print_memory_cpu_tables(results):
    # Table 1: Insert phase
    insert_keys = [
        "Bulk insert (s)",
        "Insert CPU time (s)",
        "Insert RAM used (MB)"
    ]
    header_insert = ["Benchmark"] + insert_keys
    rows_insert = []
    for bench_name, bench_results in results.items():
        row = [bench_name]
        for key in insert_keys:
            val = bench_results.get(key, "-")
            if isinstance(val, float):
                val = f"{val:.4f}"
            row.append(str(val))
        rows_insert.append(row)
    col_widths_insert = [max(len(str(cell)) for cell in col) for col in zip(header_insert, *rows_insert)]
    def print_row(row, col_widths):
        print("| " + " | ".join(cell.ljust(width) for cell, width in zip(row, col_widths)) + " |")
    print("\nInsert Phase Resource Usage:")
    print("+" + "+".join("-" * (w + 2) for w in col_widths_insert) + "+")
    print_row(header_insert, col_widths_insert)
    print("+" + "+".join("-" * (w + 2) for w in col_widths_insert) + "+")
    for row in rows_insert:
        print_row(row, col_widths_insert)
    print("+" + "+".join("-" * (w + 2) for w in col_widths_insert) + "+")

    # Table 2: Read phase
    read_keys = [
        "Bulk read (s)",
        "Read CPU time (s)",
        "Read RAM used (MB)"
    ]
    header_read = ["Benchmark"] + read_keys
    rows_read = []
    for bench_name, bench_results in results.items():
        row = [bench_name]
        for key in read_keys:
            val = bench_results.get(key, "-")
            if isinstance(val, float):
                val = f"{val:.4f}"
            row.append(str(val))
        rows_read.append(row)
    col_widths_read = [max(len(str(cell)) for cell in col) for col in zip(header_read, *rows_read)]
    print("\nRead Phase Resource Usage:")
    print("+" + "+".join("-" * (w + 2) for w in col_widths_read) + "+")
    print_row(header_read, col_widths_read)
    print("+" + "+".join("-" * (w + 2) for w in col_widths_read) + "+")
    for row in rows_read:
        print_row(row, col_widths_read)
    print("+" + "+".join("-" * (w + 2) for w in col_widths_read) + "+")


def monitor_resource_usage(func):
    """Decorator to monitor CPU and RAM usage for a function."""
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        cpu_start = process.cpu_times()
        mem_start = process.memory_info().rss
        result = func(*args, **kwargs)
        cpu_end = process.cpu_times()
        mem_end = process.memory_info().rss
        cpu_used = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
        mem_used = mem_end - mem_start
        if isinstance(result, dict):
            result['CPU time (s)'] = cpu_used
            result['RAM used (MB)'] = mem_used / (1024 * 1024)
        return result
    return wrapper

# --- Benchmark functions with resource monitoring ---
@monitor_resource_usage
def e2e_bench_1(shared_df, start_df, end_df):
    import pyarrow as pa
    df = shared_df
    # --- Convert DataFrame to Arrow Table ---
    start_arrow = time.time()
    arrow_table = pa.Table.from_pandas(df)
    end_arrow = time.time()

    # --- Create temp DuckDB and table ---
    db_fd, db_path = tempfile.mkstemp(suffix=".duckdb")
    os.close(db_fd)  # Close the file descriptor, DuckDB will create the file
    os.remove(db_path)  # Remove the empty file so DuckDB can create it
    con = duckdb.connect(db_path)

    start_create = time.time()
    con.execute(table_ddl)
    end_create = time.time()

    # --- Bulk insert using Arrow Table ---
    process = psutil.Process(os.getpid())
    mem_before_insert = process.memory_info().rss
    cpu_before_insert = process.cpu_times()
    start_insert = time.time()
    con.register("arrow_table", arrow_table)
    con.execute(f"INSERT INTO {DUCKDB_TABLE_NAME} SELECT * FROM arrow_table;")
    end_insert = time.time()
    cpu_after_insert = process.cpu_times()
    mem_after_insert = process.memory_info().rss
    insert_cpu = (cpu_after_insert.user - cpu_before_insert.user) + (cpu_after_insert.system - cpu_before_insert.system)
    insert_mem = mem_after_insert - mem_before_insert
    # --- Bulk read ---
    mem_before_read = process.memory_info().rss
    cpu_before_read = process.cpu_times()
    start_read = time.time()
    df_out = con.execute(f"SELECT * FROM {DUCKDB_TABLE_NAME};").fetchdf()
    end_read = time.time()
    cpu_after_read = process.cpu_times()
    mem_after_read = process.memory_info().rss
    read_cpu = (cpu_after_read.user - cpu_before_read.user) + (cpu_after_read.system - cpu_before_read.system)
    read_mem = mem_after_read - mem_before_read

    # --- Print timings ---
    print("[e2e_bench_1]")
    print(f"DataFrame creation: {end_df - start_df:.4f} seconds")
    print(f"Arrow Table conversion: {end_arrow - start_arrow:.4f} seconds")
    print(f"DuckDB table creation: {end_create - start_create:.4f} seconds")
    print(f"Bulk insert (Arrow): {end_insert - start_insert:.4f} seconds")
    print(f"Bulk read: {end_read - start_read:.4f} seconds")
    print(f"Rows written: {len(df)} | Rows read: {len(df_out)}")

    results = {
        "DataFrame creation (s)": end_df - start_df,
        "Arrow Table conversion (s)": end_arrow - start_arrow,
        "DuckDB table creation (s)": end_create - start_create,
        "Bulk insert (s)": end_insert - start_insert,
        "Bulk read (s)": end_read - start_read,
        "Insert CPU time (s)": insert_cpu,
        "Insert RAM used (MB)": insert_mem / (1024 * 1024),
        "Read CPU time (s)": read_cpu,
        "Read RAM used (MB)": read_mem / (1024 * 1024),
        "Rows written": len(df),
        "Rows read": len(df_out)
    }

    # --- Cleanup ---
    con.close()
    os.remove(db_path)

    return results


@monitor_resource_usage
def e2e_bench_3(shared_df, start_df, end_df):
    import pyarrow as pa
    # --- Convert DataFrame to Arrow Table ---
    start_arrow = time.time()
    arrow_table = pa.Table.from_pandas(shared_df)
    end_arrow = time.time()

    # --- Create temp DuckDB and table ---
    db_fd, db_path = tempfile.mkstemp(suffix=".duckdb")
    os.close(db_fd)
    os.remove(db_path)
    con = duckdb.connect(db_path)

    start_create = time.time()
    con.execute(table_ddl)
    end_create = time.time()

    # --- Bulk insert using Arrow Table (most efficient way) ---
    process = psutil.Process(os.getpid())
    mem_before_insert = process.memory_info().rss
    cpu_before_insert = process.cpu_times()
    start_insert = time.time()
    con.register("arrow_table", arrow_table)
    con.execute(f"INSERT INTO {DUCKDB_TABLE_NAME} SELECT * FROM arrow_table;")
    end_insert = time.time()
    cpu_after_insert = process.cpu_times()
    mem_after_insert = process.memory_info().rss
    insert_cpu = (cpu_after_insert.user - cpu_before_insert.user) + (cpu_after_insert.system - cpu_before_insert.system)
    insert_mem = mem_after_insert - mem_before_insert
    mem_before_read = process.memory_info().rss
    cpu_before_read = process.cpu_times()
    start_read = time.time()
    df_out = con.execute(f"SELECT * FROM {DUCKDB_TABLE_NAME};").fetchdf()
    end_read = time.time()
    cpu_after_read = process.cpu_times()
    mem_after_read = process.memory_info().rss
    read_cpu = (cpu_after_read.user - cpu_before_read.user) + (cpu_after_read.system - cpu_before_read.system)
    read_mem = mem_after_read - mem_before_read

    # --- Print timings ---
    print("[e2e_bench_4]")
    print(f"DataFrame creation: {end_df - start_df:.4f} seconds")
    print(f"Arrow Table conversion: {end_arrow - start_arrow:.4f} seconds")
    print(f"DuckDB table creation: {end_create - start_create:.4f} seconds")
    print(f"Bulk insert (Arrow): {end_insert - start_insert:.4f} seconds")
    print(f"Bulk read: {end_read - start_read:.4f} seconds")
    print(f"Rows written: {len(shared_df)} | Rows read: {len(df_out)}")

    results = {
        "DataFrame creation (s)": end_df - start_df,
        "Arrow Table conversion (s)": end_arrow - start_arrow,
        "DuckDB table creation (s)": end_create - start_create,
        "Bulk insert (s)": end_insert - start_insert,
        "Bulk read (s)": end_read - start_read,
        "Insert CPU time (s)": insert_cpu,
        "Insert RAM used (MB)": insert_mem / (1024 * 1024),
        "Read CPU time (s)": read_cpu,
        "Read RAM used (MB)": read_mem / (1024 * 1024),
        "Rows written": len(shared_df),
        "Rows read": len(df_out)
    }

    # --- Cleanup ---
    con.close()
    os.remove(db_path)

    return results


# --- Looping runner for all e2e_bench functions ---
def run_all_benchmarks(shared_df, start_df, end_df):
    results = {}
    for i in range(1, 5):
        func_name = f"e2e_bench_{i}"
        func = globals().get(func_name)
        if func:
            print(f"Running {func_name}...")
            results[func_name] = func(shared_df, start_df, end_df)
            print(f"{func_name} completed.")
        else:
            print(f"Function {func_name} not found.")
    return results

if __name__ == "__main__":
    shared_df, start_df, end_df = get_benchmark_dataset()
    results = run_all_benchmarks(shared_df, start_df, end_df)
    print_results_table(results)
    print_memory_cpu_tables(results)