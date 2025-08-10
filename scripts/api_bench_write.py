"""
API Write Benchmarks using actual /write endpoints, styled like api_bench_full.
Generates sample payloads, measures POST duration, throughput, CPU and memory; prints a table and saves CSV.
"""
import os
import csv
import json
import uuid
import random
import psutil
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"  # Change as needed
NUM_RECORDS_TO_WRITE = 1_000_000
NUM_RUNS_PER_ENDPOINT = 2
REQUEST_TIMEOUT_S = 900

# Actual write endpoints implemented in app/api/routes/writes.py
WRITE_ENDPOINTS: List[Tuple[str, str]] = [
    ("Polars Write Ultra-Fast", f"/write/polars/{SCHEMA_NAME}"),
    ("DuckDB Write Ultra-Fast", f"/write/duckdb/{SCHEMA_NAME}"),
]


# --- Sample Data Generation ---
def generate_sample_data(num_records: int) -> List[Dict[str, Any]]:
    data: List[Dict[str, Any]] = []
    base_prod_date = datetime(2020, 1, 1)
    for i in range(num_records):
        created_at_dt = datetime.now() - timedelta(days=random.randint(0, 365))
        prod_date_dt = base_prod_date + timedelta(days=(i % 3650), hours=random.randint(0, 23))
        record = {
            "id": str(uuid.uuid4()),
            "created_at": created_at_dt.isoformat() + "Z",
            "version": 1,
            "field_code": random.randint(1, 1000),
            "field_name": f"Field_{random.randint(1, 1000)}",
            "well_code": random.randint(1, 100),
            "well_reference": f"WELL_REF_{random.randint(1, 100):03d}",
            "well_name": f"Well_{random.randint(1, 100)}",
            "production_period": prod_date_dt.isoformat() + "Z",
            "days_on_production": random.randint(15, 30),
            "oil_production_kbd": round(random.uniform(10.0, 500.0) + (i * 0.01), 2),
            "gas_production_mmcfd": round(random.uniform(5.0, 200.0) + (i * 0.005), 2),
            "liquids_production_kbd": round(random.uniform(2.0, 100.0) + (i * 0.0025), 2),
            "water_production_kbd": round(random.uniform(20.0, 1000.0) + (i * 0.0075), 2),
            "data_source": "performance_test_v2",
            "source_data": json.dumps({"test_run_id": str(uuid.uuid4()), "iteration": i}),
            "partition_0": f"partition_{random.randint(0, 9)}",
        }
        data.append(record)
    return data


def _proc():
    return psutil.Process(os.getpid())


def benchmark_write(endpoint_path: str, op_name: str, num_records: int) -> Dict[str, Any]:
    url = f"{BASE_URL}{endpoint_path}"
    payload = {"data": generate_sample_data(num_records), "compression": "zstd"}

    p = _proc()
    mem_start = p.memory_info().rss / (1024 * 1024)
    cpu_start = p.cpu_percent(interval=None)
    start = time.perf_counter()
    resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_S)
    end = time.perf_counter()
    mem_end = p.memory_info().rss / (1024 * 1024)
    cpu_end = p.cpu_percent(interval=None)

    duration = end - start
    status = "SUCCESS" if resp.ok else f"FAILED: {resp.status_code}"
    records_written = 0
    file_path = ""
    file_size_mb = 0.0
    if resp.ok:
        try:
            result = resp.json()
            records_written = int(result.get("records_written", 0))
            file_path = result.get("file_path", "")
            file_size_mb = float(result.get("file_size_mb", 0.0))
        except Exception:
            status = "FAILED: invalid JSON response"

    throughput = int(records_written / duration) if duration > 0 and records_written > 0 else 0
    return {
        "operation": op_name,
        "duration_s": duration,
        "records": records_written,
        "throughput_rps": throughput,
        "cpu_usage": cpu_end - cpu_start,
        "memory_usage_mb": mem_end - mem_start,
        "status": status,
        "file_path": file_path,
        "file_size_mb": file_size_mb,
    }


def print_results_table(results: List[Dict[str, Any]]):
    headers = [
        "Operation",
        "Avg Duration (s)",
        "Avg Records",
        "Avg Throughput (rps)",
        "Avg CPU %",
        "Avg Memory (MB)",
        "Success Runs",
    ]
    rows = []
    for res in results:
        rows.append([
            res.get("operation", "N/A"),
            f"{res['duration_s']:.2f}",
            f"{res['records']:,}",
            f"{res['throughput_rps']:,}",
            f"{res['cpu_usage']:.1f}",
            f"{res['memory_usage_mb']:.1f}",
            f"{res.get('success_runs', 0)}/{res.get('total_runs', 0)}",
        ])

    if not rows:
        print("No results to display.")
        return

    col_widths = [max(len(str(cell)) for cell in col) for col in zip(headers, *rows)]
    print("\n" + "=" * 120)
    print("WRITE BENCHMARK RESULTS")
    print("=" * 120)
    print("| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    for row in rows:
        print("| " + " | ".join(str(cell).ljust(w) for cell, w in zip(row, col_widths)) + " |")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    print("=" * 120)


def save_results_to_csv(results: List[Dict[str, Any]], filename: str | None = None):
    if not results:
        return
    if not filename:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_write_results_{ts}.csv"
    bench_dir = os.path.join(os.getcwd(), "benchmarkings")
    os.makedirs(bench_dir, exist_ok=True)
    path = os.path.join(bench_dir, filename)
    fields = [
        "operation",
        "duration_s",
        "records",
        "throughput_rps",
        "cpu_usage",
        "memory_usage_mb",
        "status",
        "file_path",
        "file_size_mb",
        "success_runs",
        "total_runs",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in results:
            writer.writerow(r)
    print(f"✅ Results saved to: {path}")


def run_benchmark():
    aggregated: List[Dict[str, Any]] = []
    for op_name, endpoint in WRITE_ENDPOINTS:
        per_runs: List[Dict[str, Any]] = []
        success = 0
        for _ in range(NUM_RUNS_PER_ENDPOINT):
            res = benchmark_write(endpoint, op_name, NUM_RECORDS_TO_WRITE)
            if res.get("status") == "SUCCESS":
                success += 1
            per_runs.append(res)

        if per_runs:
            avg_duration = sum(r["duration_s"] for r in per_runs) / len(per_runs)
            avg_records = int(sum(r.get("records", 0) for r in per_runs) / len(per_runs))
            avg_throughput = int(sum(r.get("throughput_rps", 0) for r in per_runs) / len(per_runs))
            avg_cpu = sum(r.get("cpu_usage", 0.0) for r in per_runs) / len(per_runs)
            avg_mem = sum(r.get("memory_usage_mb", 0.0) for r in per_runs) / len(per_runs)
            # Keep last known file info
            last = next((r for r in reversed(per_runs) if r.get("file_path")), per_runs[-1])

            aggregated.append({
                "operation": op_name,
                "duration_s": avg_duration,
                "records": avg_records,
                "throughput_rps": avg_throughput,
                "cpu_usage": avg_cpu,
                "memory_usage_mb": avg_mem,
                "status": "SUCCESS" if success == len(per_runs) else ("PARTIAL" if success > 0 else "FAILED"),
                "file_path": last.get("file_path", ""),
                "file_size_mb": last.get("file_size_mb", 0.0),
                "success_runs": success,
                "total_runs": len(per_runs),
            })

    print_results_table(aggregated)
    save_results_to_csv(aggregated)


if __name__ == "__main__":
    print("STARTING API WRITE BENCHMARKS")
    print(f"Schema: {SCHEMA_NAME}")
    print(f"Records per test: {NUM_RECORDS_TO_WRITE:,}")
    print(f"Runs per endpoint: {NUM_RUNS_PER_ENDPOINT}")
    print(f"API Base URL: {BASE_URL}")
    run_benchmark()
