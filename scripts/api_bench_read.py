"""
API Read Benchmarks using actual /read endpoints, styled like api_bench_full.
Measures duration, throughput, CPU and memory; prints a table and saves CSV.
"""
import os
import time
import csv
import psutil
import requests
import pyarrow.ipc as ipc
from datetime import datetime
from typing import List, Dict, Any, Tuple

BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"  # Change as needed
NUM_RUNS_PER_ENDPOINT = 3
REQUEST_TIMEOUT_S = 600

# Actual read endpoints implemented in app/api/routes/reads.py
READ_ENDPOINTS: List[Tuple[str, str]] = [
    ("Polars Read (Arrow IPC)", f"/read/polars/{SCHEMA_NAME}"),
    ("DuckDB Read (Arrow IPC)", f"/read/duckdb/{SCHEMA_NAME}"),
    ("Arrow Read (Arrow IPC)", f"/read/arrow/{SCHEMA_NAME}"),
]


def _proc():
    return psutil.Process(os.getpid())


def benchmark_read(endpoint_path: str, op_name: str) -> Dict[str, Any]:
    url = f"{BASE_URL}{endpoint_path}"
    p = _proc()
    mem_start = p.memory_info().rss / (1024 * 1024)
    cpu_start = p.cpu_percent(interval=None)

    start = time.perf_counter()
    resp = requests.get(url, timeout=REQUEST_TIMEOUT_S)
    end = time.perf_counter()

    mem_end = p.memory_info().rss / (1024 * 1024)
    cpu_end = p.cpu_percent(interval=None)

    duration = end - start
    status = "SUCCESS" if resp.ok else f"FAILED: {resp.status_code}"
    records = 0
    if resp.ok:
        body = resp.content
        with ipc.open_stream(body) as reader:
            table = reader.read_all()
        records = len(table)

    throughput = int(records / duration) if duration > 0 and records > 0 else 0
    return {
        "operation": op_name,
        "duration_s": duration,
        "records": records,
        "throughput_rps": throughput,
        "cpu_usage": cpu_end - cpu_start,
        "memory_usage_mb": mem_end - mem_start,
        "status": status,
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
    print("READ BENCHMARK RESULTS")
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
        filename = f"benchmark_read_results_{ts}.csv"
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
    for op_name, endpoint in READ_ENDPOINTS:
        per_runs: List[Dict[str, Any]] = []
        success = 0
        for _ in range(NUM_RUNS_PER_ENDPOINT):
            res = benchmark_read(endpoint, op_name)
            if res.get("status") == "SUCCESS":
                success += 1
                per_runs.append(res)
            else:
                per_runs.append(res)

        if per_runs:
            avg_duration = sum(r["duration_s"] for r in per_runs) / len(per_runs)
            avg_records = int(sum(r.get("records", 0) for r in per_runs) / len(per_runs))
            avg_throughput = int(sum(r.get("throughput_rps", 0) for r in per_runs) / len(per_runs))
            avg_cpu = sum(r.get("cpu_usage", 0.0) for r in per_runs) / len(per_runs)
            avg_mem = sum(r.get("memory_usage_mb", 0.0) for r in per_runs) / len(per_runs)

            aggregated.append({
                "operation": op_name,
                "duration_s": avg_duration,
                "records": avg_records,
                "throughput_rps": avg_throughput,
                "cpu_usage": avg_cpu,
                "memory_usage_mb": avg_mem,
                "success_runs": success,
                "total_runs": len(per_runs),
            })

    print_results_table(aggregated)
    save_results_to_csv(aggregated)


if __name__ == "__main__":
    print("STARTING API READ BENCHMARKS")
    print(f"Schema: {SCHEMA_NAME}")
    print(f"Runs per endpoint: {NUM_RUNS_PER_ENDPOINT}")
    print(f"API Base URL: {BASE_URL}")
    run_benchmark()
