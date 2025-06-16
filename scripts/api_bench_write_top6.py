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
NUM_RECORDS_TO_WRITE = 100_000  # Number of records to generate and write for each test
DEFAULT_API_BATCH_SIZE = NUM_RECORDS_TO_WRITE # Default batch_size for API payload or DuckDB query param
NUM_RUNS_PER_ENDPOINT = 1     # Number of times to run each specific benchmark configuration
DELAY_BETWEEN_RUNS_S = 1.0    # Delay between runs to reduce noise

# Debug level - set to False to disable verbose output
DEBUG = False

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
            "field_name": f"Field_{random.randint(1, 1000)}",
            "well_code": random.randint(1, 100),
            "well_reference": f"WELL_REF_{random.randint(1,100):03d}",
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

# --- TOP 6 ENDPOINTS ONLY (Based on Benchmark Results) ---
# Each tuple: (Operation Name, Endpoint Template, Compression, Validate Schema, Is Ultra Fast)
WRITE_ENDPOINTS: List[Tuple[str, str, Optional[str], bool, bool]] = [

    # TOP 3 WITHOUT VALIDATION (FASTEST OVERALL)
    ("Arrow Write OPTIMIZED Feather", "/arrow-write-optimized/{schema_name}/feather", None, False, True),  # 859,650 rps
    ("Polars Write Batch Parquet", "/polars-write-batch/{schema_name}/parquet", "zstd", False, True),      # 854,733 rps
    ("Bulk Write OPTIMIZED (Feather, None)", "/bulk-write-optimized/{schema_name}?format=feather&validation_mode=none", None, False, True),  # 831,230 rps

    # TOP 3 WITH VALIDATION (BEST VALIDATED PERFORMANCE)
    ("Bulk Write OPTIMIZED (Feather, Vectorized)", "/bulk-write-optimized/{schema_name}?format=feather&validation_mode=vectorized", None, True, False),  # 849,455 rps
    ("Bulk Write OPTIMIZED (Parquet, Vectorized)", "/bulk-write-optimized/{schema_name}?format=parquet&validation_mode=vectorized", "snappy", True, False),  # 553,895 rps
    ("Polars Write OPTIMIZED Parquet", "/polars-write-optimized/{schema_name}/parquet", "snappy", True, False),  # 527,835 rps
]

async def benchmark_write(
    session: aiohttp.ClientSession,
    endpoint_template: str,
    op_name: str,
    num_records: int,
    compression: Optional[str], 
    validate_schema: bool,
    is_ultra_fast: bool = False
) -> dict:
    process = psutil.Process(os.getpid()) # Get current process
    process.cpu_percent(interval=None)    # Prime CPU usage calculation
    
    # --- Start total test timer ---
    total_start_time = time.perf_counter()
    # Generate data but don't print it
    payload_data = generate_sample_data(num_records)
    
    # Handle different endpoint types
    if "/bulk-write-optimized/" in endpoint_template:
        # Handle bulk-write-optimized endpoints with query parameters already in template
        actual_url = f"{BASE_URL}{endpoint_template.format(schema_name=SCHEMA_NAME)}"
        post_payload = {
            "data": payload_data,
            "batch_size": DEFAULT_API_BATCH_SIZE,
            "validate_schema": validate_schema
        }
    elif "/polars-write-optimized/" in endpoint_template or "/arrow-write-optimized/" in endpoint_template:
        # Handle new optimized endpoints
        actual_url = f"{BASE_URL}{endpoint_template.format(schema_name=SCHEMA_NAME)}"
        post_payload = {
            "data": payload_data,
            "batch_size": DEFAULT_API_BATCH_SIZE,
            "validate_schema": validate_schema
        }
    elif "/polars-write-batch/" in endpoint_template:
        # Handle batch endpoints
        actual_url = f"{BASE_URL}{endpoint_template.format(schema_name=SCHEMA_NAME)}"
        post_payload = {
            "data": payload_data,
            "batch_size": DEFAULT_API_BATCH_SIZE,
            "compression": compression,
            "validate_schema": validate_schema,
            "append_mode": False 
        }
    else:
        actual_url = f"{BASE_URL}{endpoint_template.format(schema_name=SCHEMA_NAME)}"
        post_payload = {
            "data": payload_data,
            "batch_size": DEFAULT_API_BATCH_SIZE,
            "compression": compression,
            "validate_schema": validate_schema,
            "append_mode": False 
        }

    memory_start_mb = process.memory_info().rss / (1024 * 1024)
    # --- Start API POST timer ---
    post_start_time = time.perf_counter()
    records_written = 0
    file_path_written = ""
    file_size_mb = 0.0

    try:
        if DEBUG:
            print(f"Sending request to {actual_url}")
        async with session.post(actual_url, json=post_payload, timeout=aiohttp.ClientTimeout(total=600)) as response:
            response_content = await response.json()
            if response.status >= 400:
                error_message = response_content.get('detail', response_content.get('message', 'Unknown error'))
                print(f"❌ FAILED: {response.status}")
                response.raise_for_status()
            records_written = response_content.get("records_written", 0)
            file_path_written = response_content.get("file_path", "")
            file_size_mb = response_content.get("file_size_mb", 0.0)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"❌ {op_name} FAILED: {e}")
        raise
    finally:
        # --- End API POST timer ---
        post_end_time = time.perf_counter()
        # Capture metrics as close to the operation end as possible
        end_time = time.perf_counter()
        cpu_usage_op_percent = process.cpu_percent(interval=None) 
        memory_end_mb = process.memory_info().rss / (1024 * 1024)

    duration = end_time - total_start_time  # Total test time
    post_duration = post_end_time - post_start_time  # Only POST time
    return {
        "operation": op_name,
        "duration_s": duration,
        "post_duration_s": post_duration,
        "records_written": records_written,
        "throughput_rps": records_written / duration if duration > 0 else 0,
        "file_path": file_path_written,
        "file_size_mb": file_size_mb,
        "compression": compression,
        "cpu_usage_op_percent": cpu_usage_op_percent,
        "memory_start_mb": memory_start_mb,
        "memory_end_mb": memory_end_mb,
        "memory_usage_diff_mb": memory_end_mb - memory_start_mb,
    }

def print_results_table(results: List[Dict[str, Any]]):
    headers = [
        "Operation", "Validation", "Compression", "Total Time (s)", "POST Time (s)", "Records Written",
        "Throughput (Total, rps)", "Throughput (POST, rps)"
    ]
    rows = []
    for result in results:
        operation_name = result.get("operation", "N/A")
        records_written = result.get('records_written', 0)
        
        # Determine validation status from operation name
        validation_status = "✅ YES" if any(x in operation_name for x in ["Vectorized", "OPTIMIZED Parquet"]) else "❌ NO"
        
        try:
            total_time = float(result.get('duration_s', 0))
            post_time = float(result.get('post_duration_s', 0))
            throughput_total = (int(records_written) / total_time) if total_time and records_written != 'N/A' else 'N/A'
            throughput_post = (int(records_written) / post_time) if post_time and records_written != 'N/A' else 'N/A'
        except Exception:
            throughput_total = 'N/A'
            throughput_post = 'N/A'
        rows.append([
            operation_name,
            validation_status,
            str(result.get("compression", "None")),
            f"{result.get('duration_s', 0):.3f}" if isinstance(result.get('duration_s', 0), (float, int)) else result.get('duration_s', 0),
            f"{result.get('post_duration_s', 0):.3f}" if isinstance(result.get('post_duration_s', 0), (float, int)) else result.get('post_duration_s', 0),
            f"{records_written:,}" if isinstance(records_written, (int, float)) else records_written,
            f"{throughput_total:,.0f}" if isinstance(throughput_total, (int, float)) else throughput_total,
            f"{throughput_post:,.0f}" if isinstance(throughput_post, (int, float)) else throughput_post
        ])

    if not rows:
        print("No results to display.")
        return

    # Calculate column widths
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*([headers] + rows))]

    # Print table with performance summary
    print("\n" + "="*120)
    print("TOP 6 WRITE PERFORMANCE BENCHMARK RESULTS")
    print("="*120)
    print("🏆 TOP 3 WITHOUT VALIDATION + TOP 3 WITH VALIDATION")
    print("Target: 500K+ records/second")
    print("="*120)
    
    header_line = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |"
    separator_line = "+"+ "+".join("-" * (w + 2) for w in col_widths) + "+"
    print(separator_line)
    print(header_line)
    print(separator_line)
    for row in rows:
        print("| " + " | ".join(str(cell).ljust(w) for cell, w in zip(row, col_widths)) + " |")
    print(separator_line)
    
    # Performance summary
    best_throughput_total = max((float(r[6].replace(',', '')) for r in rows if r[6] != 'N/A'), default=0)
    best_throughput_post = max((float(r[7].replace(',', '')) for r in rows if r[7] != 'N/A'), default=0)
    target_met = "✅ TARGET MET!" if best_throughput_post >= 500000 else "❌ Target not met"
    print(f"\n🚀 Best Throughput (POST): {best_throughput_post:,.0f} records/second {target_met}")
    print(f"🚀 Best Throughput (Total): {best_throughput_total:,.0f} records/second")
    
    # Count validation vs non-validation
    with_validation = sum(1 for r in rows if "✅ YES" in r[1])
    without_validation = sum(1 for r in rows if "❌ NO" in r[1])
    print(f"\n📊 Endpoints tested: {len(rows)} total ({with_validation} with validation, {without_validation} without validation)")
    print("="*120)

async def run_benchmark():
    """Run all top 6 benchmark endpoints"""
    timeout = aiohttp.ClientTimeout(total=900)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        all_results = []
        failed_results = []
        
        print(f"\n🚀 RUNNING TOP 6 PERFORMANCE ENDPOINTS")
        print(f"Records per test: {NUM_RECORDS_TO_WRITE:,}")
        print(f"Target throughput: 500,000+ records/second")
        print("="*80)
        
        # Run all endpoints
        for idx, (op_name, endpoint_template, compression, validate_schema, is_ultra_fast) in enumerate(WRITE_ENDPOINTS):
            validation_str = "WITH validation" if validate_schema else "WITHOUT validation"
            comp_str = f" ({compression})" if compression else " (no compression)"
            
            print(f"\n[{idx+1}/6] Running: {op_name}")
            print(f"        Validation: {validation_str}")
            print(f"        Compression: {compression or 'None'}")
            
            try:
                result = await benchmark_write(
                    session, endpoint_template, op_name,
                    NUM_RECORDS_TO_WRITE, compression, validate_schema, is_ultra_fast
                )
                all_results.append(result)
                print(f"        ✅ Complete: {result['throughput_rps']:,.0f} records/sec in {result['duration_s']:.3f}s")
                await asyncio.sleep(DELAY_BETWEEN_RUNS_S)
            except Exception as e:
                print(f"        ❌ Failed: {str(e)}")
                failed_results.append({
                    "operation": op_name,
                    "compression": compression,
                    "duration_s": 'N/A',
                    "post_duration_s": 'N/A',
                    "records_written": 'N/A',
                    "throughput_rps": 'N/A',
                    "cpu_usage_op_percent": 'N/A',
                    "memory_end_mb": 'N/A',
                })
        
        # Print ASCII table with all results (including failures)
        if all_results or failed_results:
            print_results_table(all_results + failed_results)
            print(f"\n🎯 Benchmark completed! Tested {len(all_results) + len(failed_results)} top-performing endpoints with {NUM_RECORDS_TO_WRITE:,} records each.")
        else:
            print("No results to display.")

def print_endpoint_summary():
    """Print summary of selected endpoints"""
    print("\n" + "="*80)
    print("TOP 6 ENDPOINT SELECTION SUMMARY")
    print("="*80)
    print("Based on previous benchmark results, selected:")
    print("\n🏆 TOP 3 WITHOUT VALIDATION (Fastest Overall):")
    print("   1. Arrow Write OPTIMIZED Feather     - 859,650 rps")
    print("   2. Polars Write Batch Parquet        - 854,733 rps") 
    print("   3. Bulk Write OPTIMIZED (Feather)    - 831,230 rps")
    print("\n✅ TOP 3 WITH VALIDATION (Best Validated):")
    print("   1. Bulk Write OPTIMIZED (Feather)    - 849,455 rps")
    print("   2. Bulk Write OPTIMIZED (Parquet)    - 553,895 rps")
    print("   3. Polars Write OPTIMIZED Parquet    - 527,835 rps")
    print("="*80)

if __name__ == "__main__":
    print("🚀 STARTING TOP 6 API WRITE BENCHMARKS")
    print_endpoint_summary()
    
    # Run the benchmark
    asyncio.run(run_benchmark()) 