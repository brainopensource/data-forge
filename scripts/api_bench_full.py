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
from typing import List, Dict, Any, Optional


BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"  # Change as needed
DATASET_SIZES = [10_000_000]  # You can adjust as needed
NUM_RUNS_PER_SIZE = 1  # Number of runs per dataset size


# --- Sample Data Generation ---
def generate_sample_data(num_records: int) -> List[Dict[str, Any]]:
    data = []
    base_prod_date = datetime(2020, 1, 1)
    for i in range(num_records):
        created_at_dt = datetime.now() - timedelta(days=random.randint(0, 365))
        prod_date_dt = base_prod_date + timedelta(days=(i % 3650), hours=random.randint(0,23))  # 10 years range
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
    url = f"{BASE_URL}/write/polars/{SCHEMA_NAME}"
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


def read_data_polars() -> Dict[str, Any]:
    """Test Polars read endpoint performance."""
    url = f"{BASE_URL}/read/polars/{SCHEMA_NAME}"
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
            "operation": "READ_POLARS",
            "duration_s": duration,
            "records": records_read,
            "cpu_usage": cpu_end - cpu_start,
            "memory_usage_mb": mem_end - mem_start,
            "status": "SUCCESS"
        }
    else:
        return {
            "operation": "READ_POLARS",
            "duration_s": duration,
            "records": 0,
            "cpu_usage": cpu_end - cpu_start,
            "memory_usage_mb": mem_end - mem_start,
            "status": f"FAILED: {response.status_code}"
        }


def read_data_arrow() -> Dict[str, Any]:
    """Test Arrow read endpoint performance."""
    url = f"{BASE_URL}/read/arrow/{SCHEMA_NAME}"
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
            "operation": "READ_ARROW",
            "duration_s": duration,
            "records": records_read,
            "cpu_usage": cpu_end - cpu_start,
            "memory_usage_mb": mem_end - mem_start,
            "status": "SUCCESS"
        }
    else:
        return {
            "operation": "READ_ARROW",
            "duration_s": duration,
            "records": 0,
            "cpu_usage": cpu_end - cpu_start,
            "memory_usage_mb": mem_end - mem_start,
            "status": f"FAILED: {response.status_code}"
        }


def print_results_table(results: List[Dict[str, Any]]):
    headers = ["Dataset Size", "Operation", "Endpoint", "Duration (s)", "Records", "Throughput (rps)", "CPU %", "Memory (MB)", "Status"]
    rows = []
    for result in results:
        # Calculate throughput
        records = result.get("records", 0)
        duration = result.get("duration_s", 0)
        throughput = int(records / duration) if duration > 0 and records > 0 else 0
        
        # Extract endpoint from operation
        operation = result.get("operation", "N/A")
        if operation == "READ_POLARS":
            endpoint = "Polars"
            operation_type = "READ"
        elif operation == "READ_ARROW":
            endpoint = "Arrow"
            operation_type = "READ"
        else:
            endpoint = "N/A"
            operation_type = operation
        
        rows.append([
            result.get("dataset_size", "N/A"),
            operation_type,
            endpoint,
            f"{result['duration_s']:.2f}",
            f"{records:,}" if isinstance(records, int) else records,
            f"{throughput:,}" if throughput > 0 else "N/A",
            f"{result['cpu_usage']:.1f}",
            f"{result['memory_usage_mb']:.1f}",
            result.get("status", "N/A")
        ])
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(headers, *rows)]
    print("\n" + "="*140)
    print("END-TO-END BENCHMARK RESULTS (WRITE + READ: POLARS vs ARROW)")
    print("="*140)
    print("| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    for row in rows:
        print("| " + " | ".join(str(cell).ljust(w) for cell, w in zip(row, col_widths)) + " |")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    print("="*140)


def save_results_to_csv(results: List[Dict[str, Any]], filename: Optional[str] = None):
    if not results:
        print("No results to save.")
        return
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_full_polars_vs_arrow_{timestamp}.csv"
    # Save to cwd/benchmarkings/
    cwd = os.getcwd()
    bench_dir = os.path.join(cwd, "benchmarkings")
    os.makedirs(bench_dir, exist_ok=True)
    csv_path = os.path.join(bench_dir, filename)
    fieldnames = ["dataset_size", "operation", "endpoint", "duration_s", "records", "throughput_rps", "cpu_usage", "memory_usage_mb", "status"]
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                # Calculate throughput
                records = result.get('records', 0)
                duration = result.get('duration_s', 0)
                throughput = int(records / duration) if duration and records and isinstance(records, int) else 0
                
                # Extract endpoint from operation
                operation = result.get('operation', 'N/A')
                if operation == "READ_POLARS":
                    endpoint = "Polars"
                    operation_type = "READ"
                elif operation == "READ_ARROW":
                    endpoint = "Arrow"
                    operation_type = "READ"
                else:
                    endpoint = "N/A"
                    operation_type = operation
                
                writer.writerow({
                    'dataset_size': result.get('dataset_size', 'N/A'),
                    'operation': operation_type,
                    'endpoint': endpoint,
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


def create_schema():
    """Create the schema needed for the benchmark."""
    schema_definition = {
        "description": "Schema for well production data for benchmarking.",
        "table_name": "well_production",
        "primary_key": ["field_code", "well_code", "production_period"],
        "properties": [
            {"name": "field_code", "type": "integer", "db_type": "BIGINT", "required": True, "primary_key": True},
            {"name": "field_name", "type": "string", "db_type": "VARCHAR"},
            {"name": "well_code", "type": "integer", "db_type": "BIGINT", "required": True, "primary_key": True},
            {"name": "well_reference", "type": "string", "db_type": "VARCHAR"},
            {"name": "well_name", "type": "string", "db_type": "VARCHAR"},
            {"name": "production_period", "type": "string", "db_type": "TIMESTAMP", "required": True, "primary_key": True},
            {"name": "days_on_production", "type": "integer", "db_type": "BIGINT"},
            {"name": "oil_production_kbd", "type": "number", "db_type": "DOUBLE"},
            {"name": "gas_production_mmcfd", "type": "number", "db_type": "DOUBLE"},
            {"name": "liquids_production_kbd", "type": "number", "db_type": "DOUBLE"},
            {"name": "water_production_kbd", "type": "number", "db_type": "DOUBLE"},
            {"name": "data_source", "type": "string", "db_type": "VARCHAR"},
            {"name": "source_data", "type": "string", "db_type": "VARCHAR"},
            {"name": "partition_0", "type": "string", "db_type": "VARCHAR"},
        ],
    }
    
    try:
        response = requests.post(f"{BASE_URL}/schemas/{SCHEMA_NAME}", json=schema_definition, timeout=30)
        # The API now returns 201 for successful creation
        if response.status_code in [200, 201]:
            print(f"✅ Schema '{SCHEMA_NAME}' registered successfully (or already existed).")
            return True
        else:
            print(f"❌ Schema registration failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"❌ Schema registration error: {e}")
        return False

def main():
    print("="*120)
    print("STARTING END-TO-END BENCHMARK (WRITE + READ: POLARS vs ARROW)")
    print("="*120)
    print(f"Schema: {SCHEMA_NAME}")
    print(f"Dataset sizes: {DATASET_SIZES}")
    print(f"Runs per size: {NUM_RUNS_PER_SIZE}")
    print(f"API Base URL: {BASE_URL}")
    print("Read endpoints to test: Polars, Arrow")
    print("="*120)
    
    # Create schema first
    #if not create_schema():
    #    print("❌ Cannot proceed without schema. Exiting.")
    #    return
    
    all_results = []
    for size in DATASET_SIZES:
        for run in range(NUM_RUNS_PER_SIZE):
            print(f"\n=== Dataset Size: {size:,} | Run {run+1}/{NUM_RUNS_PER_SIZE} ===")
            
            # Write data once
            write_result = write_data(size)
            write_result["dataset_size"] = size
            print(f"✅ Write: {write_result['records']:,} records in {write_result['duration_s']:.2f}s ({int(write_result['records']/write_result['duration_s']):,} rps)")
            all_results.append(write_result)
            
            # Small delay to ensure file system consistency
            time.sleep(0.1)
            
            # Test Polars read endpoint
            print(f"🔄 Testing Polars read endpoint...")
            polars_result = read_data_polars()
            polars_result["dataset_size"] = size
            if polars_result["status"] == "SUCCESS":
                print(f"✅ Polars Read: {polars_result['records']:,} records in {polars_result['duration_s']:.2f}s ({int(polars_result['records']/polars_result['duration_s']):,} rps)")
            else:
                print(f"❌ Polars Read failed: {polars_result['status']}")
            all_results.append(polars_result)
            
            # Small delay between read tests
            time.sleep(0.1)
            
            # Test Arrow read endpoint
            print(f"🔄 Testing Arrow read endpoint...")
            arrow_result = read_data_arrow()
            arrow_result["dataset_size"] = size
            if arrow_result["status"] == "SUCCESS":
                print(f"✅ Arrow Read: {arrow_result['records']:,} records in {arrow_result['duration_s']:.2f}s ({int(arrow_result['records']/arrow_result['duration_s']):,} rps)")
            else:
                print(f"❌ Arrow Read failed: {arrow_result['status']}")
            all_results.append(arrow_result)
            
    print_results_table(all_results)
    save_results_to_csv(all_results)
    
    # Print detailed summary
    successful_writes = [r for r in all_results if r['operation'] == 'WRITE' and r['status'] == 'SUCCESS']
    successful_polars_reads = [r for r in all_results if r['operation'] == 'READ_POLARS' and r['status'] == 'SUCCESS']
    successful_arrow_reads = [r for r in all_results if r['operation'] == 'READ_ARROW' and r['status'] == 'SUCCESS']
    
    print(f"\n📊 DETAILED SUMMARY:")
    print(f"   Successful writes: {len(successful_writes)}/{len([r for r in all_results if r['operation'] == 'WRITE'])}")
    print(f"   Successful Polars reads: {len(successful_polars_reads)}/{len([r for r in all_results if r['operation'] == 'READ_POLARS'])}")
    print(f"   Successful Arrow reads: {len(successful_arrow_reads)}/{len([r for r in all_results if r['operation'] == 'READ_ARROW'])}")
    
    if successful_writes:
        write_throughputs = [int(r['records']/r['duration_s']) for r in successful_writes if r['duration_s'] > 0]
        if write_throughputs:
            print(f"   Best write throughput: {max(write_throughputs):,} records/second")
    
    if successful_polars_reads:
        polars_throughputs = [int(r['records']/r['duration_s']) for r in successful_polars_reads if r['duration_s'] > 0]
        if polars_throughputs:
            avg_polars = sum(polars_throughputs) / len(polars_throughputs)
            print(f"   Polars read - Best: {max(polars_throughputs):,} rps, Average: {int(avg_polars):,} rps")
    
    if successful_arrow_reads:
        arrow_throughputs = [int(r['records']/r['duration_s']) for r in successful_arrow_reads if r['duration_s'] > 0]
        if arrow_throughputs:
            avg_arrow = sum(arrow_throughputs) / len(arrow_throughputs)
            print(f"   Arrow read - Best: {max(arrow_throughputs):,} rps, Average: {int(avg_arrow):,} rps")
    
    # Performance comparison
    if successful_polars_reads and successful_arrow_reads:
        polars_avg_time = sum(r['duration_s'] for r in successful_polars_reads) / len(successful_polars_reads)
        arrow_avg_time = sum(r['duration_s'] for r in successful_arrow_reads) / len(successful_arrow_reads)
        
        print(f"\n🏆 PERFORMANCE COMPARISON:")
        print(f"   Average Polars read time: {polars_avg_time:.2f}s")
        print(f"   Average Arrow read time: {arrow_avg_time:.2f}s")
        
        if polars_avg_time < arrow_avg_time:
            improvement = ((arrow_avg_time - polars_avg_time) / arrow_avg_time) * 100
            print(f"   🥇 Polars is {improvement:.1f}% faster than Arrow")
        elif arrow_avg_time < polars_avg_time:
            improvement = ((polars_avg_time - arrow_avg_time) / polars_avg_time) * 100
            print(f"   🥇 Arrow is {improvement:.1f}% faster than Polars")
        else:
            print(f"   🤝 Both endpoints have similar performance")


if __name__ == "__main__":
    main()

