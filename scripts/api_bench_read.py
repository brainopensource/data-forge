# api_read_bench.py
import asyncio
import aiohttp
import time
import pyarrow as pa
import pyarrow.ipc as ipc
import psutil
import os
from typing import Dict

BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"  # Change as needed

# --- Resource Monitoring ---
def get_process_metrics():
    process = psutil.Process(os.getpid())
    return {
        "cpu_percent": process.cpu_percent(),
        "memory_mb": process.memory_info().rss / (1024 * 1024)
    }

ENDPOINTS = [
    ("Polars Read (JSON)", "/polars-read-1/{schema_name}", "polars"),
    ("Polars Read 2 (Arrow IPC)", "/polars-read-2/{schema_name}", "arrow"),
    ("DuckDB Read (Arrow IPC)", "/duckdb-read/{schema_name}", "arrow"),
    ("PyArrow Read (Arrow IPC)", "/pyarrow-read/{schema_name}", "arrow"),
]

async def benchmark_read(session: aiohttp.ClientSession, endpoint: str, mode: str, op_name: str) -> dict:
    url = f"{BASE_URL}{endpoint.format(schema_name=SCHEMA_NAME)}"
    print(f"\n--- Starting Benchmark: {op_name} ({url}) ---")
    metrics_start = get_process_metrics()
    start_time = time.perf_counter()
    records_retrieved = 0
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=300)) as response:
            response.raise_for_status()
            if mode == "polars":
                data = await response.json()
                records_retrieved = len(data)
            elif mode == "arrow":
                body = await response.read()
                with ipc.open_stream(body) as reader:
                    arrow_table = reader.read_all()
                records_retrieved = len(arrow_table)
            print(f"Read successful: Retrieved {records_retrieved} records.")
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"Read failed: {e}")
        raise
    end_time = time.perf_counter()
    metrics_end = get_process_metrics()
    duration = end_time - start_time
    return {
        "operation": op_name,
        "duration_s": duration,
        "records_retrieved": records_retrieved,
        "throughput_rps": records_retrieved / duration if duration > 0 else 0,
        "cpu_usage": metrics_end["cpu_percent"] - metrics_start["cpu_percent"],
        "memory_usage_mb": metrics_end["memory_mb"] - metrics_start["memory_mb"],
    }

def print_results_table(results):
    headers = ["Operation", "Duration (s)", "Records", "Throughput (rps)", "CPU %", "Memory (MB)"]
    rows = []
    for result in results:
        rows.append([
            result.get("operation", "N/A"),
            f"{result['duration_s']:.2f}",
            result.get("records_retrieved", 0),
            f"{result['throughput_rps']:.2f}",
            f"{result['cpu_usage']:.1f}",
            f"{result['memory_usage_mb']:.1f}"
        ])
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(headers, *rows)]
    print("\nBenchmark Results:")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    print("| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    for row in rows:
        print("| " + " | ".join(str(cell).ljust(w) for cell, w in zip(row, col_widths)) + " |")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")

async def run_benchmark():
    timeout = aiohttp.ClientTimeout(total=600)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        results = []
        for op_name, endpoint, mode in ENDPOINTS:
            try:
                result = await benchmark_read(session, endpoint, mode, op_name)
                results.append(result)
            except Exception as e:
                print(f"Benchmark for {op_name} failed: {e}")
        print_results_table(results)

if __name__ == "__main__":
    print("Starting API Bulk Read Benchmarks...")
    asyncio.run(run_benchmark())
