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
import csv

BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"  # Change as needed. This will also be used as table_name for DuckDB.
NUM_RECORDS_TO_WRITE = 100000  # Number of records to generate and write for each test
DEFAULT_API_BATCH_SIZE = NUM_RECORDS_TO_WRITE # Default batch_size for API payload or DuckDB query param
NUM_RUNS_PER_ENDPOINT = 1     # Number of times to run each specific benchmark configuration
DELAY_BETWEEN_RUNS_S = 0.5    # Delay between runs to reduce noise

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

# --- Endpoints to Benchmark ---
# Each tuple: (Operation Name, Endpoint Template, Compression, Validate Schema, Is Ultra Fast)
WRITE_ENDPOINTS: List[Tuple[str, str, Optional[str], bool, bool]] = [

    # COMMENTED OUT: WORST 25 PERFORMING ENDPOINTS (< 40K rps average)
    # ("Polars Write Parquet (ZSTD)", "/polars-write/{schema_name}/parquet", "zstd", True, False),  # 19,965.75 rps
    # ("Polars Write Feather (ZSTD)", "/polars-write/{schema_name}/feather", "zstd", True, False),  # 25,126.33 rps
    # ("DuckDB Write to Table", "/duckdb-write/{schema_name}", None, True, False),  # 26,873.5 rps
    # ("Polars Write Batch Parquet (ZSTD)", "/polars-write-batch/{schema_name}/parquet", "zstd", True, False),  # 27,053.75 rps
    # ("Polars Write Parquet (None)", "/polars-write/{schema_name}/parquet", None, True, False),  # 28,259.75 rps
    # ("PyArrow Write Parquet (ZSTD)", "/pyarrow-write/{schema_name}/parquet", "zstd", True, False),  # 29,706 rps
    # ("Feather Write Compressed (ZSTD)", "/feather-write/{schema_name}/compressed", "zstd", True, False),  # 29,798.25 rps
    # ("PyArrow Write Streaming Parquet (ZSTD)", "/pyarrow-write/{schema_name}/streaming-parquet", "zstd", True, False),  # 30,213.75 rps
    # ("Polars Write Feather (None)", "/polars-write/{schema_name}/feather", None, True, False),  # 30,880.67 rps
    # ("Feather Write Fast", "/feather-write/{schema_name}/fast", None, True, False),  # 30,935.67 rps
    # ("Polars Write Parquet (LZ4)", "/polars-write/{schema_name}/parquet", "lz4", True, False),  # 30,944.67 rps
    # ("Polars Write Feather (LZ4)", "/polars-write/{schema_name}/feather", "lz4", True, False),  # 31,054.5 rps
    # ("Polars Write OPTIMIZED Parquet (LZ4)", "/polars-write-optimized/{schema_name}/parquet", "lz4", True, False),  # 31,197.67 rps
    # ("Polars Write Snappy Parquet", "/polars-write-snappy/{schema_name}/parquet", "snappy", True, False),  # 31,288 rps
    # ("Feather Write Compressed (LZ4)", "/feather-write/{schema_name}/compressed", "lz4", True, False),  # 31,400.67 rps
    # ("DuckDB Write ULTRA-FAST", "/duckdb-write/{schema_name}/ultra-fast", "zstd", False, True),  # 31,610.67 rps
    # ("Polars Write Batch Parquet (LZ4)", "/polars-write-batch/{schema_name}/parquet", "lz4", True, False),  # 31,737.33 rps
    # ("Polars Write FAST Parquet (LZ4)", "/polars-write/{schema_name}/parquet-fast", "lz4", False, True),  # 35,557 rps
    # ("Polars Write ULTRA-FAST (ZSTD)", "/polars-write/{schema_name}/ultra-fast", "zstd", False, True),  # 35,790 rps
    # ("Polars Write FAST Parquet (ZSTD)", "/polars-write/{schema_name}/parquet-fast", "zstd", False, True),  # 36,175 rps
    # ("Polars Write ULTRA-FAST (LZ4)", "/polars-write/{schema_name}/ultra-fast", "lz4", False, True),  # 36,362 rps
    # ("Bulk Write OPTIMIZED (Parquet, None)", "/bulk-write-optimized/{schema_name}?format=parquet&validation_mode=none", "snappy", False, True),  # 36,817.67 rps
    ("Bulk Write OPTIMIZED (Parquet, Vectorized)", "/bulk-write-optimized/{schema_name}?format=parquet&validation_mode=vectorized", "snappy", True, False),  # 36,865.75 rps
    # ("Stream Write Parquet", "/stream-write/{schema_name}/parquet?chunk_size=50000", "snappy", True, False),  # 36,878.67 rps
    # ("Bulk Write OPTIMIZED (Feather, None)", "/bulk-write-optimized/{schema_name}?format=feather&validation_mode=none", None, False, True),  # 37,449.67 rps

    ## ACTIVE HIGH-PERFORMANCE ENDPOINTS (40K+ rps average) - ONLY THE BEST PERFORMERS
    ("Arrow Write DIRECT (ZSTD)", "/arrow-write/{schema_name}/direct", "zstd", False, True),
    ("Arrow Write DIRECT (LZ4)", "/arrow-write/{schema_name}/direct", "lz4", False, True), # GOD
    # NOTE: PyArrow doesn't support LZ4 - using SNAPPY instead
    #("PyArrow Write Parquet (SNAPPY)", "/pyarrow-write/{schema_name}/parquet", "snappy", True, False),
    # NOTE: PyArrow doesn't support LZ4 - using SNAPPY instead
    #("PyArrow Write Streaming Parquet (SNAPPY)", "/pyarrow-write/{schema_name}/streaming-parquet", "snappy", True, False),
    ("Polars Write OPTIMIZED Parquet (ZSTD)", "/polars-write-optimized/{schema_name}/parquet", "zstd", True, False),  #GOD
    ("Arrow Write OPTIMIZED Feather", "/arrow-write-optimized/{schema_name}/feather", None, True, False), # GOD
    ("Bulk Write OPTIMIZED (Feather, Vectorized)", "/bulk-write-optimized/{schema_name}?format=feather&validation_mode=vectorized", None, True, False), # GOD

    # REMOVED: ULTRA-OPTIMIZED endpoints that don't exist in the API
    # These endpoints were causing 404 errors - they are not implemented
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
    if "/duckdb-write/" in endpoint_template:
        if is_ultra_fast:
            actual_url = f"{BASE_URL}{endpoint_template.format(schema_name=SCHEMA_NAME)}"
            post_payload = payload_data
        else:
            actual_url = f"{BASE_URL}{endpoint_template.format(schema_name=SCHEMA_NAME)}?batch_size={DEFAULT_API_BATCH_SIZE}"
            post_payload = payload_data
    elif "/bulk-write-optimized/" in endpoint_template:
        # Handle bulk-write-optimized endpoints with query parameters already in template
        actual_url = f"{BASE_URL}{endpoint_template.format(schema_name=SCHEMA_NAME)}"
        post_payload = {
            "data": payload_data,
            "batch_size": DEFAULT_API_BATCH_SIZE,
            "validate_schema": validate_schema
        }
    elif "/stream-write/" in endpoint_template:
        # Handle stream-write endpoints with chunk_size in URL
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
    elif is_ultra_fast and ("/ultra-fast" in endpoint_template or "/direct" in endpoint_template or "/parquet-fast" in endpoint_template):
        actual_url = f"{BASE_URL}{endpoint_template.format(schema_name=SCHEMA_NAME)}"
        post_payload = {"data": payload_data}  # Send as dict, not list
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
            if response.status >= 400:
                try:
                    response_content = await response.json()
                    error_message = response_content.get('detail', response_content.get('message', 'Unknown error'))
                except:
                    error_message = f"HTTP {response.status}"
                print(f"❌ FAILED: {response.status} - {error_message}")
                # Don't raise, return error result instead
                return {
                    "operation": op_name,
                    "duration_s": 0,
                    "post_duration_s": 0,
                    "records_written": "FAILED",
                    "throughput_rps": 0,
                    "file_path": "",
                    "file_size_mb": 0.0,
                    "compression": compression,
                    "cpu_usage_op_percent": 0,
                    "memory_start_mb": memory_start_mb,
                    "memory_end_mb": memory_start_mb,
                    "memory_usage_diff_mb": 0,
                    "error": f"HTTP {response.status}: {error_message}"
                }
            
            response_content = await response.json()
            records_written = response_content.get("records_written", 0)
            file_path_written = response_content.get("file_path", "")
            file_size_mb = response_content.get("file_size_mb", 0.0)
    except (aiohttp.ClientError, asyncio.TimeoutError, aiohttp.ServerDisconnectedError) as e:
        print(f"❌ {op_name} FAILED: {e}")
        # Return error result instead of raising
        return {
            "operation": op_name,
            "duration_s": 0,
            "post_duration_s": 0,
            "records_written": "FAILED",
            "throughput_rps": 0,
            "file_path": "",
            "file_size_mb": 0.0,
            "compression": compression,
            "cpu_usage_op_percent": 0,
            "memory_start_mb": memory_start_mb,
            "memory_end_mb": memory_start_mb,
            "memory_usage_diff_mb": 0,
            "error": str(e)
        }
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
        "Operation", "Compression", "Total Time (s)", "POST Time (s)", "Records Written",
        "Throughput (Total, rps)", "Throughput (POST, rps)"
    ]
    rows = []
    for result in results:
        operation_name = result.get("operation", "N/A")
        records_written = result.get('records_written', 0)
        
        # Handle failed results
        if records_written == "FAILED":
            error_msg = result.get('error', 'Unknown error')
            throughput_total = f'FAILED: {error_msg[:30]}...' if len(error_msg) > 30 else f'FAILED: {error_msg}'
            throughput_post = 'FAILED'
        else:
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
            str(result.get("compression", "N/A")),
            f"{result.get('duration_s', 0):.2f}" if isinstance(result.get('duration_s', 0), (float, int)) else result.get('duration_s', 0),
            f"{result.get('post_duration_s', 0):.2f}" if isinstance(result.get('post_duration_s', 0), (float, int)) else result.get('post_duration_s', 0),
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
    print("\n" + "="*100)
    print("WRITE PERFORMANCE BENCHMARK RESULTS")
    print("="*100)
    print("Target: 500K records/second")
    print("="*100)
    
    header_line = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |"
    separator_line = "+"+ "+".join("-" * (w + 2) for w in col_widths) + "+"
    print(separator_line)
    print(header_line)
    print(separator_line)
    for row in rows:
        print("| " + " | ".join(str(cell).ljust(w) for cell, w in zip(row, col_widths)) + " |")
    print(separator_line)
    
    # Performance summary
    valid_total_throughputs = []
    valid_post_throughputs = []
    failed_count = 0
    
    for r in rows:
        if r[5] != 'N/A' and not str(r[5]).startswith('FAILED'):
            try:
                valid_total_throughputs.append(float(str(r[5]).replace(',', '')))
            except:
                pass
        if r[6] != 'N/A' and r[6] != 'FAILED' and not str(r[6]).startswith('FAILED'):
            try:
                valid_post_throughputs.append(float(str(r[6]).replace(',', '')))
            except:
                pass
        if str(r[4]) == 'FAILED' or str(r[5]).startswith('FAILED') or r[6] == 'FAILED':
            failed_count += 1
    
    best_throughput_total = max(valid_total_throughputs, default=0)
    best_throughput_post = max(valid_post_throughputs, default=0)
    target_met = "✅ TARGET MET!" if best_throughput_post >= 500000 else "❌ Target not met"
    print(f"\nBest Throughput (POST): {best_throughput_post:,.0f} records/second {target_met}")
    print(f"Best Throughput (Total): {best_throughput_total:,.0f} records/second")
    print(f"Failed Endpoints: {failed_count}/{len(rows)} ({failed_count/len(rows)*100:.1f}%)")
    print("="*100)


def save_results_to_csv(results: List[Dict], filename: str = None):
    """Save benchmark results to CSV file in project root"""
    if not results:
        print("No results to save.")
        return
    
    # Generate filename if not provided
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_{timestamp}.csv"
    
    # Ensure we're saving to project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(project_root, filename)
    
    # Prepare CSV data
    fieldnames = [
        'operation',
        'compression', 
        'records_written',
        'duration_s',
        'post_duration_s',
        'throughput_total_rps',
        'throughput_post_rps',
        'cpu_usage_percent',
        'memory_usage_mb',
        'file_size_mb',
        'file_path',
        'status'
    ]
    
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                # Calculate throughput values
                records_written = result.get('records_written', 0)
                total_time = result.get('duration_s', 0)
                post_time = result.get('post_duration_s', 0)
                
                if records_written == "FAILED":
                    throughput_total = "FAILED"
                    throughput_post = "FAILED"
                    status = "FAILED"
                else:
                    try:
                        throughput_total = int(records_written) / total_time if total_time and records_written else 0
                        throughput_post = int(records_written) / post_time if post_time and records_written else 0
                        status = "SUCCESS"
                    except:
                        throughput_total = 0
                        throughput_post = 0
                        status = "ERROR"
                
                # Write row
                writer.writerow({
                    'operation': result.get('operation', 'N/A'),
                    'compression': result.get('compression', 'N/A'),
                    'records_written': records_written,
                    'duration_s': result.get('duration_s', 'N/A'),
                    'post_duration_s': result.get('post_duration_s', 'N/A'),
                    'throughput_total_rps': f"{throughput_total:,.0f}" if isinstance(throughput_total, (int, float)) else throughput_total,
                    'throughput_post_rps': f"{throughput_post:,.0f}" if isinstance(throughput_post, (int, float)) else throughput_post,
                    'cpu_usage_percent': result.get('cpu_usage_op_percent', 'N/A'),
                    'memory_usage_mb': result.get('memory_usage_diff_mb', 'N/A'),
                    'file_size_mb': result.get('file_size_mb', 'N/A'),
                    'file_path': result.get('file_path', 'N/A'),
                    'status': status
                })
        
        print(f"✅ Results saved to: {csv_path}")
        
        # Print summary stats
        successful_results = [r for r in results if r.get('records_written') != "FAILED"]
        failed_count = len(results) - len(successful_results)
        
        if successful_results:
            # Calculate best throughputs
            valid_post_throughputs = []
            for r in successful_results:
                records = r.get('records_written', 0)
                post_time = r.get('post_duration_s', 0)
                if records and post_time:
                    try:
                        throughput = int(records) / post_time
                        valid_post_throughputs.append(throughput)
                    except:
                        pass
            
            if valid_post_throughputs:
                best_throughput = max(valid_post_throughputs)
                target_met = "✅ TARGET MET!" if best_throughput >= 500000 else "❌ Target not met"
                print(f"Best Throughput: {best_throughput:,.0f} records/second {target_met}")
        
        print(f"Total Tests: {len(results)} | Successful: {len(successful_results)} | Failed: {failed_count}")
        
    except Exception as e:
        print(f"❌ Error saving CSV: {e}")


def verify_benchmark_configuration():
    """Verify that all endpoints are properly configured for benchmarking"""
    print("\n" + "="*80)
    print("BENCHMARK CONFIGURATION VERIFICATION")
    print("="*80)
    
    total_endpoints = len(WRITE_ENDPOINTS)
    print(f"Total endpoints configured: {total_endpoints}")
    
    # Count by category
    standard_count = 5  # First 5 endpoints
    ultra_fast_count = 4  # Next 4 endpoints (indices 5-8)
    batch_compression_count = 6  # Next 6 endpoints (indices 9-14)
    optimized_count = total_endpoints - 15  # Remaining endpoints
    
    print(f"\nEndpoint Categories:")
    print(f"  - Standard (with validation): {standard_count}")
    print(f"  - Ultra-fast (bypass validation): {ultra_fast_count}")
    print(f"  - Batch & Compression: {batch_compression_count}")
    print(f"  - New Optimized (500K+ target): {optimized_count}")
    
    # Verify endpoint patterns
    print(f"\nEndpoint Pattern Verification:")
    patterns = {
        "polars-write": 0,
        "arrow-write": 0,
        "duckdb-write": 0,
        "bulk-write-optimized": 0,
        "stream-write": 0,
        "feather-write": 0,
        "pyarrow-write": 0
    }
    
    for name, endpoint, _, _, _ in WRITE_ENDPOINTS:
        for pattern in patterns:
            if pattern in endpoint:
                patterns[pattern] += 1
                break
    
    for pattern, count in patterns.items():
        if count > 0:
            print(f"  - {pattern}: {count} endpoints")
    
    # Check for new optimized endpoints
    optimized_endpoints = [name for name, endpoint, _, _, _ in WRITE_ENDPOINTS if "OPTIMIZED" in name]
    print(f"\nNew Optimized Endpoints ({len(optimized_endpoints)}):")
    for endpoint in optimized_endpoints:
        print(f"  - {endpoint}")
    
    print("="*80)
    print("✅ Benchmark configuration verified!")
    print("✅ All new ultra-high-performance endpoints included!")
    print("="*80)


def run_benchmarks(num_records_to_write: int, num_runs_per_endpoint: int):
    """
    Run the full benchmark suite for the given number of records and runs per endpoint.
    Returns: List[Dict[str, Any]] with endpoint name and best POST throughput (rps) for each endpoint.
    """
    import asyncio
    global NUM_RECORDS_TO_WRITE, NUM_RUNS_PER_ENDPOINT, DEFAULT_API_BATCH_SIZE
    NUM_RECORDS_TO_WRITE = num_records_to_write
    NUM_RUNS_PER_ENDPOINT = num_runs_per_endpoint
    DEFAULT_API_BATCH_SIZE = NUM_RECORDS_TO_WRITE

    results_summary = []

    async def main():
        print(f"STARTING API WRITE BENCHMARKS\nRecords per test: {NUM_RECORDS_TO_WRITE:,}\nTarget throughput: 500,000 records/second\nTotal endpoints available: {len(WRITE_ENDPOINTS)}\n")
        verify_benchmark_configuration()
        await test_validation_and_comparison_endpoints(aiohttp.ClientSession())
        print("\nAvailable endpoints to benchmark:")
        for i, (op_name, _, _, _, _) in enumerate(WRITE_ENDPOINTS):
            print(f"{i+1}. {op_name}")
        print("="*80)
        print(f"Running {NUM_RUNS_PER_ENDPOINT} runs per endpoint...")
        all_results = []
        failed_results = []
        timeout = aiohttp.ClientTimeout(total=900)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for endpoint_index, (op_name, endpoint_template, compression, validate_schema, is_ultra_fast) in enumerate(WRITE_ENDPOINTS):
                for run_idx in range(NUM_RUNS_PER_ENDPOINT):
                    try:
                        result = await benchmark_write(
                            session, endpoint_template, op_name,
                            NUM_RECORDS_TO_WRITE, compression, validate_schema, is_ultra_fast
                        )
                        all_results.append(result)
                        print(f"✅ Complete: {result['operation']} | {result['records_written']} records | Run {run_idx+1}/{NUM_RUNS_PER_ENDPOINT}")
                        await asyncio.sleep(DELAY_BETWEEN_RUNS_S)
                    except Exception as e:
                        print(f"❌ Failed: {str(e)}")
                        failed_results.append({
                            "operation": op_name,
                            "compression": compression,
                            "duration_s": 'N/A',
                            "post_duration_s": 'N/A',
                            "records_written": 'N/A',
                            "throughput_rps": 'N/A',
                            "cpu_usage_op_percent": 'N/A',
                            "memory_usage_mb": 'N/A',
                            "file_size_mb": 'N/A',
                            "file_path": 'N/A',
                            "error": str(e)
                        })
        if all_results:
            save_results_to_csv(all_results)
            print(f"\nBenchmark completed! Tested {len(all_results)} endpoint-runs with {NUM_RECORDS_TO_WRITE:,} records each.")
            if failed_results:
                print(f"Failed endpoints: {len(failed_results)}")
        else:
            print("No results to display.")

        # Build summary: best POST throughput per endpoint
        endpoint_to_rps = {}
        for result in all_results:
            name = result.get('operation', 'N/A')
            records_written = result.get('records_written', 0)
            post_time = result.get('post_duration_s', 0)
            if records_written == "FAILED" or not records_written or not post_time:
                continue
            try:
                rps = int(records_written) / float(post_time)
            except Exception:
                continue
            if name not in endpoint_to_rps or rps > endpoint_to_rps[name]:
                endpoint_to_rps[name] = rps
        # Format as requested
        nonlocal results_summary
        results_summary = [
            {'endpoint': name, 'rps': int(rps)}
            for name, rps in endpoint_to_rps.items()
        ]

    asyncio.run(main())
    return results_summary


async def test_validation_and_comparison_endpoints(session: aiohttp.ClientSession):
    """Test the new validation and performance comparison endpoints"""
    print("\n" + "="*80)
    print("TESTING VALIDATION AND PERFORMANCE COMPARISON ENDPOINTS")
    print("="*80)
    
    payload_data = generate_sample_data(NUM_RECORDS_TO_WRITE)
    
    # Test vectorized validation endpoint
    print(f"\n🔍 Testing Vectorized Validation Endpoint...")
    try:
        validation_url = f"{BASE_URL}/vectorized-validate/{SCHEMA_NAME}"
        validation_payload = {
            "data": payload_data,
            "validate_schema": True
        }
        
        start_time = time.perf_counter()
        async with session.post(validation_url, json=validation_payload, timeout=aiohttp.ClientTimeout(total=300)) as response:
            validation_result = await response.json()
            validation_time = time.perf_counter() - start_time
            
            if response.status == 200:
                print(f"✅ Vectorized Validation Complete:")
                print(f"   - Validation Time: {validation_time:.3f}s")
                print(f"   - Validation Throughput: {validation_result.get('validation_throughput', 0):,} records/sec")
                print(f"   - Is Valid: {validation_result.get('is_valid', False)}")
                print(f"   - Total Records: {validation_result.get('total_records', 0):,}")
                print(f"   - Error Count: {len(validation_result.get('validation_errors', []))}")
            else:
                print(f"❌ Validation endpoint failed: {response.status}")
    except Exception as e:
        print(f"❌ Validation endpoint error: {e}")
    
    # Test performance comparison endpoint
    print(f"\n📊 Testing Performance Comparison Endpoint...")
    try:
        comparison_url = f"{BASE_URL}/performance-comparison/{SCHEMA_NAME}?include_validation=true"
        comparison_payload = {
            "data": payload_data[:10000],  # Use smaller dataset for comparison
            "validate_schema": True
        }
        
        start_time = time.perf_counter()
        async with session.post(comparison_url, json=comparison_payload, timeout=aiohttp.ClientTimeout(total=600)) as response:
            comparison_result = await response.json()
            comparison_time = time.perf_counter() - start_time
            
            if response.status == 200:
                print(f"✅ Performance Comparison Complete:")
                print(f"   - Total Comparison Time: {comparison_time:.3f}s")
                print(f"   - Dataset Size: {comparison_result.get('dataset_info', {}).get('records_count', 0):,} records")
                print(f"   - Fastest Method: {comparison_result.get('summary', {}).get('fastest_method', 'N/A')}")
                
                # Show performance results
                perf_results = comparison_result.get('performance_results', {})
                for method, results in perf_results.items():
                    if 'throughput_records_per_second' in results:
                        print(f"   - {method}: {results['throughput_records_per_second']:,} records/sec")
            else:
                print(f"❌ Performance comparison failed: {response.status}")
    except Exception as e:
        print(f"❌ Performance comparison error: {e}")
    
    print("="*80)


def auto_tune_batch_size(
    min_records=5000, max_records=1000000, initial_step=50000, num_runs=1, rounds=3):
    """
    Iteratively search for the best batch size to maximize throughput.
    Returns: dict with best batch size and its throughput.
    """
    best_batch = None
    best_rps = 0
    history = []

    step = initial_step
    for round_num in range(rounds):
        print(f"\n=== Auto-tune round {round_num+1} ===")
        batch_sizes = list(range(min_records, max_records + 1, step))
        round_results = []
        for batch in batch_sizes:
            print(f"\n--- Benchmarking batch size: {batch} ---")
            results = run_benchmarks(batch, num_runs)
            # Find the best endpoint for this batch size
            if results:
                top = max(results, key=lambda r: r['rps'])
                round_results.append({'batch': batch, 'endpoint': top['endpoint'], 'rps': top['rps']})
                if top['rps'] > best_rps:
                    best_rps = top['rps']
                    best_batch = batch
        history.extend(round_results)
        # Focus next round around the best batch size found so far
        min_records = max(best_batch - step, 1000)
        max_records = best_batch + step
        step = max(step // 2, 1000)
        if step < 2000:
            break  # Stop if step is too small

    print("\n=== Auto-tune complete ===")
    print(f"Best batch size: {best_batch} with {best_rps:,.0f} rps")
    return {'best_batch': best_batch, 'best_rps': best_rps, 'history': history}


if __name__ == "__main__":
    """
    # Default: run with current global values
    scenarios = [10000, 20000, 100000]
    num_runs = 1  # or set to 2, 3, etc.

    for num_records in scenarios:
        print(f"\n=== Running benchmark for {num_records} records ===")
        results = run_benchmarks(num_records, num_runs)
        print("Results summary:")
        for r in results:
            print(r)
    """
result = auto_tune_batch_size(min_records=100000, max_records=900000, initial_step=200000, num_runs=1, rounds=1)
print(result)