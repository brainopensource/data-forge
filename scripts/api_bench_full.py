import time
import requests
import psutil
import os
import random
import uuid
import json
from datetime import datetime, timedelta
import csv
import pyarrow as pa
import pyarrow.ipc as ipc
from typing import List, Dict, Any


BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"  # Change as needed
DATASET_SIZES = [10_000, 100_000, 700_000, 1_000_000]  # You can adjust as needed
NUM_RUNS_PER_SIZE = 1  # Number of runs per dataset size


# --- Sample Data Generation ---
def generate_sample_data(num_records: int) -> List[Dict[str, Any]]:
    data = []
    base_prod_date = datetime(2020, 1, 1)
    for i in range(num_records):
        created_at_dt = datetime.now() - timedelta(days=random.randint(0, 365))
        prod_date_dt = base_prod_date + timedelta(days=i, hours=random.randint(0,23))
        record = {
            "id": str(uuid.uuid4()),
            "created_at": created_at_dt.isoformat() + "Z",
            "version": 1,
            "field_code": random.randint(1, 1000),
            "field_name": f"Field_{random.randint(1, 1000)}",
            "well_code": random.randint(1, 100),
            "well_reference": f"WELL_REF_{random.randint(1,100):03d}",
            "well_name": f"Well_{random.randint(1,100)}",
            "production_period": prod_date_dt.isoformat() + "Z",
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


# --- Benchmark Functions ---
def write_data(num_records: int) -> Dict[str, Any]:
    url = f"{BASE_URL}/polars-write/{SCHEMA_NAME}"
    payload = {"data": generate_sample_data(num_records)}
    process = psutil.Process(os.getpid())
    mem_start = process.memory_info().rss / (1024 * 1024)
    cpu_start = process.cpu_percent(interval=None)
    start = time.perf_counter()
    response = requests.post(url, json=payload, timeout=600)
    end = time.perf_counter()
    mem_end = process.memory_info().rss / (1024 * 1024)
    cpu_end = process.cpu_percent(interval=None)
    duration = end - start
    if response.status_code == 200:
        result = response.json()
        records_written = result.get("records_written", 0)
        file_path = result.get("file_path", "")
        file_size_mb = result.get("file_size_mb", 0.0)
        return {
            "operation": "WRITE",
            "duration_s": duration,
            "records": records_written,
            "file_path": file_path,
            "file_size_mb": file_size_mb,
            "cpu_usage": cpu_end - cpu_start,
            "memory_usage_mb": mem_end - mem_start,
            "status": "SUCCESS"
        }
    else:
        return {
            "operation": "WRITE",
            "duration_s": duration,
            "records": 0,
            "file_path": "",
            "file_size_mb": 0.0,
            "cpu_usage": cpu_end - cpu_start,
            "memory_usage_mb": mem_end - mem_start,
            "status": f"FAILED: {response.status_code}"
        }


def read_data() -> Dict[str, Any]:
    url = f"{BASE_URL}/polars-read-latest/{SCHEMA_NAME}"
    process = psutil.Process(os.getpid())
    mem_start = process.memory_info().rss / (1024 * 1024)
    cpu_start = process.cpu_percent(interval=None)
    start = time.perf_counter()
    response = requests.get(url, timeout=600)
    end = time.perf_counter()
    mem_end = process.memory_info().rss / (1024 * 1024)
    cpu_end = process.cpu_percent(interval=None)
    duration = end - start
    if response.status_code == 200:
        body = response.content
        with ipc.open_stream(body) as reader:
            arrow_table = reader.read_all()
        records_read = len(arrow_table)
        return {
            "operation": "READ",
            "duration_s": duration,
            "records": records_read,
            "cpu_usage": cpu_end - cpu_start,
            "memory_usage_mb": mem_end - mem_start,
            "status": "SUCCESS"
        }
    else:
        return {
            "operation": "READ",
            "duration_s": duration,
            "records": 0,
            "cpu_usage": cpu_end - cpu_start,
            "memory_usage_mb": mem_end - mem_start,
            "status": f"FAILED: {response.status_code}"
        }


def print_results_table(results: List[Dict[str, Any]]):
    headers = ["Dataset Size", "Operation", "Duration (s)", "Records", "Throughput (rps)", "CPU %", "Memory (MB)", "Status"]
    rows = []
    for result in results:
        # Calculate throughput
        records = result.get("records", 0)
        duration = result.get("duration_s", 0)
        throughput = int(records / duration) if duration > 0 and records > 0 else 0
        
        rows.append([
            result.get("dataset_size", "N/A"),
            result.get("operation", "N/A"),
            f"{result['duration_s']:.2f}",
            f"{records:,}" if isinstance(records, int) else records,
            f"{throughput:,}" if throughput > 0 else "N/A",
            f"{result['cpu_usage']:.1f}",
            f"{result['memory_usage_mb']:.1f}",
            result.get("status", "N/A")
        ])
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(headers, *rows)]
    print("\n" + "="*120)
    print("END-TO-END BENCHMARK RESULTS (WRITE + READ)")
    print("="*120)
    print("| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    for row in rows:
        print("| " + " | ".join(str(cell).ljust(w) for cell, w in zip(row, col_widths)) + " |")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    print("="*120)


def save_results_to_csv(results: List[Dict[str, Any]], filename: str = None):
    if not results:
        print("No results to save.")
        return
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_full_results_{timestamp}.csv"
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(project_root, filename)
    fieldnames = ["dataset_size", "operation", "duration_s", "records", "throughput_rps", "cpu_usage", "memory_usage_mb", "status"]
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                # Calculate throughput
                records = result.get('records', 0)
                duration = result.get('duration_s', 0)
                throughput = int(records / duration) if duration and records and isinstance(records, int) else 0
                
                writer.writerow({
                    'dataset_size': result.get('dataset_size', 'N/A'),
                    'operation': result.get('operation', 'N/A'),
                    'duration_s': result.get('duration_s', 'N/A'),
                    'records': result.get('records', 'N/A'),
                    'throughput_rps': throughput if throughput > 0 else 'N/A',
                    'cpu_usage': result.get('cpu_usage', 'N/A'),
                    'memory_usage_mb': result.get('memory_usage_mb', 'N/A'),
                    'status': result.get('status', 'N/A')
                })
        print(f"✅ Results saved to: {csv_path}")
    except Exception as e:
        print(f"❌ Error saving CSV: {e}")


def main():
    print("="*120)
    print("STARTING END-TO-END BENCHMARK (WRITE + READ)")
    print("="*120)
    print(f"Schema: {SCHEMA_NAME}")
    print(f"Dataset sizes: {DATASET_SIZES}")
    print(f"Runs per size: {NUM_RUNS_PER_SIZE}")
    print(f"API Base URL: {BASE_URL}")
    print("="*120)
    
    all_results = []
    for size in DATASET_SIZES:
        for run in range(NUM_RUNS_PER_SIZE):
            print(f"\n=== Dataset Size: {size:,} | Run {run+1}/{NUM_RUNS_PER_SIZE} ===")
            
            # Write data
            write_result = write_data(size)
            write_result["dataset_size"] = size
            print(f"✅ Write: {write_result['records']:,} records in {write_result['duration_s']:.2f}s ({int(write_result['records']/write_result['duration_s']):,} rps)")
            all_results.append(write_result)
            
            # Small delay to ensure file system consistency
            time.sleep(0.1)
            
            # Read data
            read_result = read_data()
            read_result["dataset_size"] = size
            if read_result["status"] == "SUCCESS":
                print(f"✅ Read: {read_result['records']:,} records in {read_result['duration_s']:.2f}s ({int(read_result['records']/read_result['duration_s']):,} rps)")
            else:
                print(f"❌ Read failed: {read_result['status']}")
            all_results.append(read_result)
            
    print_results_table(all_results)
    save_results_to_csv(all_results)
    
    # Print summary
    successful_writes = [r for r in all_results if r['operation'] == 'WRITE' and r['status'] == 'SUCCESS']
    successful_reads = [r for r in all_results if r['operation'] == 'READ' and r['status'] == 'SUCCESS']
    
    print(f"\n📊 SUMMARY:")
    print(f"   Successful writes: {len(successful_writes)}/{len([r for r in all_results if r['operation'] == 'WRITE'])}")
    print(f"   Successful reads: {len(successful_reads)}/{len([r for r in all_results if r['operation'] == 'READ'])}")
    
    if successful_writes:
        write_throughputs = [int(r['records']/r['duration_s']) for r in successful_writes if r['duration_s'] > 0]
        if write_throughputs:
            print(f"   Best write throughput: {max(write_throughputs):,} records/second")
    
    if successful_reads:
        read_throughputs = [int(r['records']/r['duration_s']) for r in successful_reads if r['duration_s'] > 0]
        if read_throughputs:
            print(f"   Best read throughput: {max(read_throughputs):,} records/second")


if __name__ == "__main__":
    main()

