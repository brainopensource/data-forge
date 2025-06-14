# api_bench.py
import asyncio
import aiohttp
import time
import json
import psutil
import os
from typing import List, Dict, Any
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.ipc as ipc
import uuid

# --- CONFIGURABLE PARAMETERS ---
BASE_URL = "http://localhost:8080/api/v1"  # Your API base URL
# Number of test records to generate
TEST_DATA_SIZE = 1_000_000
SCHEMA_NAME = "well_production"  # Schema to test with

# --- Test Data Generation ---
def generate_test_data(size: int) -> List[Dict[str, Any]]:
    """Generate test data with unique composite primary keys."""
    data = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(size):
        prod_date = base_date + timedelta(seconds=i)
        record = {
            "id": str(uuid.uuid4()),
            "created_at": datetime.now(),
            "version": 1,
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
        data.append(record)
    return data

# --- Resource Monitoring ---
def get_process_metrics():
    """Get current process CPU and memory usage."""
    process = psutil.Process(os.getpid())
    return {
        "cpu_percent": process.cpu_percent(),
        "memory_mb": process.memory_info().rss / (1024 * 1024)
    }

# --- Benchmark Functions ---
async def benchmark_bulk_insert(session: aiohttp.ClientSession, data: pa.Table) -> Dict[str, Any]:
    """Benchmark a single bulk insert endpoint using Arrow IPC."""
    print("--- Starting Bulk Insert Benchmark ---")
    metrics_start = get_process_metrics()
    start_time = time.perf_counter()

    # Prepare data for sending
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, data.schema) as writer:
        writer.write_table(data)
    body = sink.getvalue().to_pybytes()

    headers = {'Content-Type': 'application/vnd.apache.arrow.stream'}
    try:
        async with session.post(
            f"{BASE_URL}/arrow/bulk-insert/{SCHEMA_NAME}",
            data=body,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=300)
        ) as response:
            response.raise_for_status()
            response_json = await response.json()
            print(f"Insert successful: {response_json.get('message')}")

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"Bulk insert failed: {e}")
        raise

    end_time = time.perf_counter()
    metrics_end = get_process_metrics()
    
    duration = end_time - start_time
    return {
        "operation": "Bulk Insert",
        "duration_s": duration,
        "records_processed": len(data),
        "throughput_rps": len(data) / duration if duration > 0 else 0,
        "cpu_usage": metrics_end["cpu_percent"] - metrics_start["cpu_percent"],
        "memory_usage_mb": metrics_end["memory_mb"] - metrics_start["memory_mb"],
        "batch_size": len(data),
    }


async def benchmark_bulk_read(session: aiohttp.ClientSession) -> Dict[str, Any]:
    """Benchmark a single bulk read endpoint using Arrow IPC."""
    print("\n--- Starting Bulk Read Benchmark ---")
    metrics_start = get_process_metrics()
    start_time = time.perf_counter()
    
    records_retrieved = 0
    try:
        async with session.get(
            f"{BASE_URL}/arrow/bulk-read/{SCHEMA_NAME}",
            timeout=aiohttp.ClientTimeout(total=300)
        ) as response:
            response.raise_for_status()
            body = await response.read()
            with ipc.open_stream(body) as reader:
                arrow_table = reader.read_all()
            records_retrieved = len(arrow_table)
            print(f"Read successful: Retrieved {records_retrieved} records.")

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"Bulk read failed: {e}")
        raise
    
    end_time = time.perf_counter()
    metrics_end = get_process_metrics()
    
    duration = end_time - start_time
    return {
        "operation": "Bulk Read",
        "duration_s": end_time - start_time,
        "records_retrieved": records_retrieved,
        "throughput_rps": records_retrieved / duration if duration > 0 else 0,
        "cpu_usage": metrics_end["cpu_percent"] - metrics_start["cpu_percent"],
        "memory_usage_mb": metrics_end["memory_mb"] - metrics_start["memory_mb"],
        "batch_size": "all"
    }

# --- Results Processing ---
def print_results_table(results: List[Dict[str, Any]]):
    """Print benchmark results in a formatted table."""
    headers = ["Operation", "Batch Size", "Duration (s)", "Records", "Throughput (rps)", "CPU %", "Memory (MB)"]
    rows = []
    
    for result in results:
        rows.append([
            result.get("operation", "N/A"),
            result.get("batch_size", "N/A"),
            f"{result['duration_s']:.2f}",
            result.get("records_processed", result.get("records_retrieved", 0)),
            f"{result['throughput_rps']:.2f}",
            f"{result['cpu_usage']:.1f}",
            f"{result['memory_usage_mb']:.1f}"
        ])
    
    # Calculate column widths
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(headers, *rows)]
    
    # Print header
    print("\nBenchmark Results:")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    print("| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    
    # Print rows
    for row in rows:
        print("| " + " | ".join(str(cell).ljust(w) for cell, w in zip(row, col_widths)) + " |")
    
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")

# --- Main Benchmark Runner ---
async def run_benchmarks():
    """Run insert and read benchmarks sequentially."""
    all_results = []
    
    # Generate test data
    print(f"Generating {TEST_DATA_SIZE} test records...")
    test_data_list = generate_test_data(TEST_DATA_SIZE)
    test_data_arrow = pa.Table.from_pylist(test_data_list)

    # Configure client session with increased timeouts
    timeout = aiohttp.ClientTimeout(total=600)  # 10 minute timeout
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Test bulk insert
        try:
            insert_result = await benchmark_bulk_insert(session, test_data_arrow)
            all_results.append(insert_result)
        except Exception as e:
            print(f"Benchmark for bulk insert failed: {e}")

        # Add a small delay to ensure data is committed before reading
        await asyncio.sleep(2)

        # Test bulk read
        try:
            read_result = await benchmark_bulk_read(session)
            all_results.append(read_result)
        except Exception as e:
            print(f"Benchmark for bulk read failed: {e}")

    # Print results
    print_results_table(all_results)

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting API Performance Benchmark...")
    print(f"Base URL: {BASE_URL}")
    print(f"Test Data Size: records")
    
    asyncio.run(run_benchmarks())