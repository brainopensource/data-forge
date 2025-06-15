\
import asyncio
import aiohttp
import time
import psutil
import os
import random
import uuid
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"  # Change as needed. This will also be used as table_name for DuckDB.
NUM_RECORDS_TO_WRITE = 10000  # Number of records to generate and write for each test
DEFAULT_API_BATCH_SIZE = NUM_RECORDS_TO_WRITE # Default batch_size for API payload or DuckDB query param
NUM_RUNS_PER_ENDPOINT = 1     # Number of times to run each specific benchmark configuration
DELAY_BETWEEN_RUNS_S = 0.1      # Small delay between runs

# --- Sample Data Generation ---
def generate_sample_data(num_records: int) -> List[Dict[str, Any]]:
    data = []
    base_prod_date = datetime(2020, 1, 1) # Base for production_period

    for i in range(num_records):
        # Ensure created_at and production_period are ISO 8601 strings for JSON
        created_at_dt = datetime.now() - timedelta(days=random.randint(0, 365)) # Random past date
        prod_date_dt = base_prod_date + timedelta(days=i, hours=random.randint(0,23))

        record = {
            "id": str(uuid.uuid4()),
            "created_at": created_at_dt.isoformat() + "Z", # ISO 8601 format
            "version": 1,
            "field_code": random.randint(1, 1000), # Using random for more variability
            "_field_name": f"Field_{random.randint(1, 1000)}",
            "well_code": random.randint(1, 100),
            "_well_reference": f"WELL_REF_{random.randint(1,100):03d}",
            "well_name": f"Well_{random.randint(1,100)}",
            "production_period": prod_date_dt.isoformat() + "Z", # ISO 8601 format
            "days_on_production": random.randint(15, 30),
            "oil_production_kbd": round(random.uniform(10.0, 500.0) + (i * 0.01), 2),
            "gas_production_mmcfd": round(random.uniform(5.0, 200.0) + (i * 0.005), 2),
            "liquids_production_kbd": round(random.uniform(2.0, 100.0) + (i * 0.0025), 2),
            "water_production_kbd": round(random.uniform(20.0, 1000.0) + (i * 0.0075), 2),
            "data_source": "performance_test_v2",
            "source_data": json.dumps({"test_run_id": str(uuid.uuid4()), "iteration": i}),
            "partition_0": f"partition_{random.randint(0,9)}"
        }
        data.append(record)
    return data

# --- Endpoints to Benchmark ---
# Each tuple: (Operation Name, Endpoint Template, Compression, Validate Schema)
WRITE_ENDPOINTS: List[Tuple[str, str, Optional[str], bool]] = [
    ("Polars Write Parquet (ZSTD)", "/polars-write/{schema_name}/parquet", "zstd", True),
    ("Polars Write Parquet (None)", "/polars-write/{schema_name}/parquet", None, True),
    ("Polars Write Feather (ZSTD)", "/polars-write/{schema_name}/feather", "zstd", True),
    ("Polars Write Feather (None)", "/polars-write/{schema_name}/feather", None, True),
    ("DuckDB Write to Table", "/duckdb-write/{schema_name}", None, True), # schema_name used as table_name
]

async def benchmark_write(
    session: aiohttp.ClientSession,
    endpoint_template: str,
    op_name: str,
    num_records: int,
    compression: Optional[str], 
    validate_schema: bool 
) -> dict:
    process = psutil.Process(os.getpid()) # Get current process
    process.cpu_percent(interval=None)    # Prime CPU usage calculation

    payload_data = generate_sample_data(num_records)
    
    actual_url: str
    post_payload: Any

    if "/duckdb-write/" in endpoint_template:
        actual_url = f"{BASE_URL}{endpoint_template.format(schema_name=SCHEMA_NAME)}?batch_size={DEFAULT_API_BATCH_SIZE}"
        post_payload = payload_data
        print(f"\n--- Starting Benchmark: {op_name} ({num_records} records, batch_size_param: {DEFAULT_API_BATCH_SIZE}) ({actual_url}) ---")
    else:
        actual_url = f"{BASE_URL}{endpoint_template.format(schema_name=SCHEMA_NAME)}"
        post_payload = {
            "data": payload_data,
            "batch_size": DEFAULT_API_BATCH_SIZE,
            "compression": compression,
            "validate_schema": validate_schema,
            "append_mode": False 
        }
        print(f"\n--- Starting Benchmark: {op_name} ({num_records} records, compr: {compression}) ({actual_url}) ---")

    memory_start_mb = process.memory_info().rss / (1024 * 1024)
    start_time = time.perf_counter()
    records_written = 0
    file_path_written = ""
    file_size_mb = 0.0

    try:
        async with session.post(actual_url, json=post_payload, timeout=aiohttp.ClientTimeout(total=600)) as response:
            # Ensure response is read before timing end for more accurate CPU during I/O
            response_content = await response.json() # Changed from response_json to avoid confusion
            
            # Metrics captured after the core operation
            # CPU usage during the operation (since the priming call to cpu_percent)
            # Memory after the operation
            # Note: The time for process.cpu_percent and process.memory_info is minimal 
            # but included in the duration. For very fast ops, this could be a factor.

            if response.status >= 400:
                error_message = response_content.get('detail', response_content.get('message', 'Unknown error'))
                print(f"Write failed: {response.status}, message='{error_message}', url='{actual_url}'")
                response.raise_for_status()

            if "/duckdb-write/" in endpoint_template:
                records_written = response_content.get("records_written", 0)  # Changed from records_inserted
                file_path_written = response_content.get("file_path", "")
                file_size_mb = response_content.get("file_size_mb", 0.0)
                print(f"Write successful: {records_written} records to DuckDB table '{SCHEMA_NAME}' (File: {file_path_written}, {file_size_mb:.2f} MB)")
            else:
                records_written = response_content.get("records_written", 0)
                file_path_written = response_content.get("file_path", "")
                file_size_mb = response_content.get("file_size_mb", 0.0)
                print(f"Write successful: {records_written} records to {file_path_written} ({file_size_mb:.2f} MB)")

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"Write operation failed for {op_name}: {e}")
        raise
    finally:
        # Capture metrics as close to the operation end as possible
        end_time = time.perf_counter() # Moved here to be after all awaitables in try
        cpu_usage_op_percent = process.cpu_percent(interval=None) 
        memory_end_mb = process.memory_info().rss / (1024 * 1024)

    duration = end_time - start_time
    return {
        "operation": op_name,
        "duration_s": duration,
        "records_written": records_written,
        "throughput_rps": records_written / duration if duration > 0 else 0,
        "file_path": file_path_written,
        "file_size_mb": file_size_mb,
        "compression": compression,
        "cpu_usage_op_percent": cpu_usage_op_percent,
        "memory_start_mb": memory_start_mb,
        "memory_end_mb": memory_end_mb,
        "memory_usage_diff_mb": memory_end_mb - memory_start_mb, # Still available if needed
    }

def print_results_table(results: List[Dict[str, Any]]):
    headers = [
        "Operation", "Compression", "Duration (s)", "Records Written",
        "Throughput (rps)", "CPU Op (%)", "Client RAM Post-Op (MB)"
    ]
    rows = []
    for result in results:
        rows.append([
            result.get("operation", "N/A"),
            str(result.get("compression", "N/A")),
            f"{result.get('duration_s', 0):.2f}",
            result.get("records_written", 0),
            f"{result.get('throughput_rps', 0):.2f}",
            f"{result.get('cpu_usage_op_percent', 0.0):.2f}",
            f"{result.get('memory_end_mb', 0.0):.2f}"
        ])

    if not rows:
        print("\\nNo results to display.")
        return

    # Calculate column widths
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*([headers] + rows))]

    # Print table
    print("\\nBenchmark Results (Writes):")
    header_line = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |"
    separator_line = "+"+ "+".join("-" * (w + 2) for w in col_widths) + "+"
    print(separator_line)
    print(header_line)
    print(separator_line)
    for row in rows:
        print("| " + " | ".join(str(cell).ljust(w) for cell, w in zip(row, col_widths)) + " |")
    print(separator_line)

async def run_benchmark():
    timeout = aiohttp.ClientTimeout(total=900) # Increased timeout for potentially long writes
    async with aiohttp.ClientSession(timeout=timeout) as session:
        all_results = []
        # num_runs_per_endpoint = 1 # Number of times to run each specific benchmark configuration # Moved to top

        for op_name_template, endpoint_template, compression, validate_schema in WRITE_ENDPOINTS:
            op_name = op_name_template # op_name is already specific enough
            
            run_metrics_list = []
            print(f"\\nPreparing to benchmark: {op_name} (Compression: {compression})")
            for i in range(NUM_RUNS_PER_ENDPOINT):
                print(f"  Run {i+1}/{NUM_RUNS_PER_ENDPOINT} for {op_name}")
                try:
                    result = await benchmark_write(
                        session, endpoint_template, op_name,
                        NUM_RECORDS_TO_WRITE, compression, validate_schema
                    )
                    run_metrics_list.append(result)
                    await asyncio.sleep(DELAY_BETWEEN_RUNS_S) # Small delay between runs
                except Exception as e:
                    print(f"  Run {i+1}/{NUM_RUNS_PER_ENDPOINT} for {op_name} failed: {e}")
            
            if run_metrics_list:
                # Average the metrics for this specific endpoint configuration
                avg_duration = sum(r["duration_s"] for r in run_metrics_list) / len(run_metrics_list)
                avg_records = sum(r["records_written"] for r in run_metrics_list) / len(run_metrics_list)
                avg_file_size = sum(r["file_size_mb"] for r in run_metrics_list) / len(run_metrics_list)
                avg_cpu_op = sum(r["cpu_usage_op_percent"] for r in run_metrics_list) / len(run_metrics_list)
                avg_mem_start = sum(r["memory_start_mb"] for r in run_metrics_list) / len(run_metrics_list)
                avg_mem_end = sum(r["memory_end_mb"] for r in run_metrics_list) / len(run_metrics_list)

                avg_result = {
                    "operation": op_name,
                    "compression": compression,
                    "duration_s": avg_duration,
                    "records_written": int(avg_records),
                    "throughput_rps": avg_records / avg_duration if avg_duration > 0 else 0,
                    "file_size_mb": avg_file_size,
                    "cpu_usage_op_percent": avg_cpu_op,
                    "memory_start_mb": avg_mem_start,
                    "memory_end_mb": avg_mem_end,
                    "memory_usage_diff_mb": avg_mem_end - avg_mem_start, # Consistent diff calculation
                }
                all_results.append(avg_result)
            else:
                print(f"No successful runs for {op_name} (Compression: {compression}) to average.")

        print_results_table(all_results)

if __name__ == "__main__":
    print(f"Starting API Bulk Write Benchmarks (Writing {NUM_RECORDS_TO_WRITE} records per test)...")
    # Example: uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
    asyncio.run(run_benchmark())
