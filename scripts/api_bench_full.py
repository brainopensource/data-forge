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
import re
from typing import List, Dict, Any, Optional, Tuple


BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"  # Change as needed
DATASET_SIZES = [1_000_000]  # You can adjust as needed
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


# --- Data Integrity Validation ---
def validate_data_integrity(original_data: List[Dict[str, Any]], arrow_table: pa.Table) -> Dict[str, Any]:
    """
    Validate that the first and last rows match between original data and read data.
    Returns validation results with detailed comparison.
    """
    try:
        # Convert Arrow table to list of dictionaries for comparison
        read_data = arrow_table.to_pylist()
        
        if len(original_data) != len(read_data):
            return {
                "status": "FAILED",
                "error": f"Record count mismatch: original={len(original_data)}, read={len(read_data)}"
            }
        
        if len(original_data) == 0:
            return {"status": "SUCCESS", "message": "Empty dataset - no validation needed"}
        
        # Compare first row
        first_original = original_data[0]
        first_read = read_data[0]
        
        # Compare last row
        last_original = original_data[-1]
        last_read = read_data[-1]
        
        # Fields to compare (excluding potentially modified fields like timestamps)
        compare_fields = [
            "field_code", "field_name", "well_code", "well_reference", "well_name",
            "days_on_production", "oil_production_kbd", "gas_production_mmcfd",
            "liquids_production_kbd", "water_production_kbd", "data_source", "partition_0"
        ]
        
        first_row_issues = []
        last_row_issues = []
        
        # Compare first row
        for field in compare_fields:
            if field in first_original and field in first_read:
                orig_val = first_original[field]
                read_val = first_read[field]
                if orig_val != read_val:
                    first_row_issues.append(f"{field}: {orig_val} != {read_val}")
        
        # Compare last row
        for field in compare_fields:
            if field in last_original and field in last_read:
                orig_val = last_original[field]
                read_val = last_read[field]
                if orig_val != read_val:
                    last_row_issues.append(f"{field}: {orig_val} != {read_val}")
        
        if first_row_issues or last_row_issues:
            error_msg = ""
            if first_row_issues:
                error_msg += f"First row issues: {'; '.join(first_row_issues)}"
            if last_row_issues:
                if error_msg:
                    error_msg += " | "
                error_msg += f"Last row issues: {'; '.join(last_row_issues)}"
            
            return {
                "status": "FAILED",
                "error": error_msg,
                "first_row_issues": first_row_issues,
                "last_row_issues": last_row_issues
            }
        
        return {
            "status": "SUCCESS", 
            "message": f"Data integrity validated: {len(original_data)} records, first & last rows match",
            "validated_fields": len(compare_fields),
            "first_row_sample": {k: first_original[k] for k in compare_fields[:3] if k in first_original},
            "last_row_sample": {k: last_original[k] for k in compare_fields[:3] if k in last_original}
        }
        
    except Exception as e:
        return {
            "status": "ERROR",
            "error": f"Validation error: {str(e)}"
        }


# --- Benchmark Functions ---
def write_data(num_records: int, data: List[Dict[str, Any]]) -> Dict[str, Any]:
    url = f"{BASE_URL}/write/polars/{SCHEMA_NAME}"
    payload = {"data": data}
    print(f"🔗 Calling write URL: {url}")
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
        print(f"📝 Write result: {records_written} records written to {file_path} ({file_size_mb} MB)")
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
        print(f"❌ Write response: {response.status_code} - {response.text}")
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


def read_data_polars() -> Tuple[Dict[str, Any], Optional[pa.Table]]:
    """Test Polars read endpoint performance."""
    url = f"{BASE_URL}/read/polars/{SCHEMA_NAME}"
    print(f"🔗 Calling Polars read URL: {url}")
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
        }, arrow_table
    else:
        print(f"❌ Polars read response: {response.status_code} - {response.text}")
        return {
            "operation": "READ_POLARS",
            "duration_s": duration,
            "records": 0,
            "cpu_usage": cpu_end - cpu_start,
            "memory_usage_mb": mem_end - mem_start,
            "status": f"FAILED: {response.status_code}"
        }, None


def read_data_arrow() -> Tuple[Dict[str, Any], Optional[pa.Table]]:
    """Test Arrow read endpoint performance."""
    url = f"{BASE_URL}/read/arrow/{SCHEMA_NAME}"
    print(f"🔗 Calling Arrow read URL: {url}")
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
        }, arrow_table
    else:
        print(f"❌ Arrow read response: {response.status_code} - {response.text}")
        return {
            "operation": "READ_ARROW",
            "duration_s": duration,
            "records": 0,
            "cpu_usage": cpu_end - cpu_start,
            "memory_usage_mb": mem_end - mem_start,
            "status": f"FAILED: {response.status_code}"
        }, None


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
        elif operation == "WRITE":
            endpoint = "Polars"
            operation_type = "WRITE"
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
    
    if not rows:
        print("\n❌ No results to display")
        return
        
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


def save_results_to_csv(results: List[Dict[str, Any]], filename: Optional[str] = None) -> Optional[str]:
    if not results:
        print("No results to save.")
        return None
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
                elif operation == "WRITE":
                    endpoint = "Polars"
                    operation_type = "WRITE"
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
        return csv_path
    except Exception as e:
        print(f"❌ Error saving CSV: {e}")
        return None


def check_files_exist() -> None:
    """Check if files exist for the schema to debug file not found issues."""
    try:
        # Try to list files by calling the read endpoint first to see what it finds
        url = f"{BASE_URL}/read/polars/{SCHEMA_NAME}"
        response = requests.get(url, timeout=30)
        if response.status_code == 404:
            print(f"📁 Read endpoint reports: No files found for schema '{SCHEMA_NAME}'")
        else:
            print(f"📁 Read endpoint status: {response.status_code}")
            
        # Also try to get schema info
        schema_url = f"{BASE_URL}/schemas/{SCHEMA_NAME}"
        schema_response = requests.get(schema_url, timeout=30)
        if schema_response.status_code == 200:
            schema_info = schema_response.json()
            print(f"📋 Schema info: {schema_info}")
        else:
            print(f"❌ Failed to get schema info: {schema_response.status_code}")
            
    except Exception as e:
        print(f"❌ Error checking files: {e}")


def parse_app_log_for_internal_throughput(log_file_path: str = "logs/app.log") -> List[Dict[str, Any]]:
    """
    Parse the app.log file to extract internal throughput values.
    Returns a list of log entries with operation details and internal throughput.
    """
    internal_operations = []
    
    if not os.path.exists(log_file_path):
        print(f"⚠️  Log file not found: {log_file_path}")
        return internal_operations
    
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Pattern to match log entries like: "write|success|1000000|2.814|355341"
        operation_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?(write|read)\|(\w+)\|(\d+)\|([\d.]+)\|(\d+)'
        
        for line in lines:
            match = re.search(operation_pattern, line)
            if match:
                timestamp_str = match.group(1)
                operation = match.group(2)
                status = match.group(3)
                record_count = int(match.group(4))
                duration = float(match.group(5))
                internal_throughput = int(match.group(6))
                
                # Parse timestamp
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
                
                internal_operations.append({
                    'timestamp': timestamp,
                    'operation': operation,
                    'status': status,
                    'records': record_count,
                    'duration_s': duration,
                    'internal_rps': internal_throughput
                })
        
        print(f"📊 Parsed {len(internal_operations)} internal operations from log file")
        return internal_operations
    
    except Exception as e:
        print(f"❌ Error parsing log file: {e}")
        return internal_operations


def match_internal_throughput_to_results(benchmark_results: List[Dict[str, Any]], 
                                       internal_operations: List[Dict[str, Any]], 
                                       tolerance_seconds: float = 10.0) -> List[Dict[str, Any]]:
    """
    Match internal throughput values to benchmark results based on operation type, 
    record count, and timestamp proximity.
    """
    enhanced_results = []
    
    for result in benchmark_results:
        enhanced_result = result.copy()
        enhanced_result['internal_rps'] = 'N/A'  # Default value
        
        operation_type = result.get('operation', '')
        records = result.get('records', 0)
        
        # Convert operation type for matching
        internal_op_type = None
        if operation_type == 'WRITE':
            internal_op_type = 'write'
        elif operation_type in ['READ_POLARS', 'READ_ARROW']:
            internal_op_type = 'read'
        
        if internal_op_type and records > 0:
            # Find matching internal operation
            best_match = None
            min_time_diff = float('inf')
            
            for internal_op in internal_operations:
                if (internal_op['operation'] == internal_op_type and 
                    internal_op['records'] == records and
                    internal_op['status'] == 'success'):
                    
                    # For now, we'll match by operation type and record count
                    # In a more sophisticated approach, we could match by timestamp
                    best_match = internal_op
                    break
            
            if best_match:
                enhanced_result['internal_rps'] = best_match['internal_rps']
                enhanced_result['internal_duration_s'] = best_match['duration_s']
        
        enhanced_results.append(enhanced_result)
    
    return enhanced_results


def update_csv_with_internal_throughput(csv_file_path: str, enhanced_results: List[Dict[str, Any]]):
    """
    Update the existing CSV file to include internal throughput data.
    """
    try:
        # Read existing CSV
        existing_data = []
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            existing_data = list(reader)
        
        # Update with internal throughput data
        if len(existing_data) == len(enhanced_results):
            for i, row in enumerate(existing_data):
                enhanced_result = enhanced_results[i]
                row['internal_rps'] = enhanced_result.get('internal_rps', 'N/A')
                row['internal_duration_s'] = enhanced_result.get('internal_duration_s', 'N/A')
                
                # Calculate speed improvement
                external_rps = row.get('throughput_rps', 'N/A')
                internal_rps = row.get('internal_rps', 'N/A')
                
                if (external_rps != 'N/A' and internal_rps != 'N/A' and 
                    external_rps != '' and internal_rps != '' and
                    str(external_rps).isdigit() and str(internal_rps).isdigit()):
                    ext_val = int(external_rps)
                    int_val = int(internal_rps)
                    if ext_val > 0:
                        speed_improvement = ((int_val - ext_val) / ext_val) * 100
                        row['internal_vs_external_improvement_pct'] = f"{speed_improvement:.1f}"
                    else:
                        row['internal_vs_external_improvement_pct'] = 'N/A'
                else:
                    row['internal_vs_external_improvement_pct'] = 'N/A'
        
        # Write updated CSV
        fieldnames = list(existing_data[0].keys()) if existing_data else []
        if 'internal_rps' not in fieldnames:
            fieldnames.extend(['internal_rps', 'internal_duration_s', 'internal_vs_external_improvement_pct'])
        
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(existing_data)
        
        print(f"✅ Updated CSV file with internal throughput data: {csv_file_path}")
        
        # Print comparison summary
        print(f"\n📈 INTERNAL vs EXTERNAL THROUGHPUT COMPARISON:")
        for row in existing_data:
            operation = row.get('operation', 'Unknown')
            endpoint = row.get('endpoint', 'Unknown')
            external_rps = row.get('throughput_rps', 'N/A')
            internal_rps = row.get('internal_rps', 'N/A')
            improvement = row.get('internal_vs_external_improvement_pct', 'N/A')
            
            if external_rps != 'N/A' and internal_rps != 'N/A':
                print(f"   {operation} ({endpoint}): External={external_rps:>10} rps | Internal={internal_rps:>10} rps | Improvement={improvement:>6}%")
            else:
                print(f"   {operation} ({endpoint}): External={external_rps:>10} rps | Internal={internal_rps:>10} rps | Improvement=N/A")
    
    except Exception as e:
        print(f"❌ Error updating CSV with internal throughput: {e}")


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
    validation_data = []  # Store data for validation after all benchmarks complete
    
    for size in DATASET_SIZES:
        for run in range(NUM_RUNS_PER_SIZE):
            print(f"\n=== Dataset Size: {size:,} | Run {run+1}/{NUM_RUNS_PER_SIZE} ===")
            
            # Generate data once for consistency across operations
            print("🎲 Generating sample data...")
            original_data = generate_sample_data(size)
            
            # Write data once
            write_result = write_data(size, original_data)
            write_result["dataset_size"] = size
            print(f"✅ Write: {write_result['records']:,} records in {write_result['duration_s']:.2f}s ({int(write_result['records']/write_result['duration_s']):,} rps)")
            all_results.append(write_result)
            
            # Check what files exist after writing
            check_files_exist()
            
            # Longer delay to ensure file system consistency and file availability
            print("⏳ Waiting for file system to sync...")
            time.sleep(2.0)
            
            # Test Polars read endpoint (performance measurement only)
            print(f"🔄 Testing Polars read endpoint...")
            polars_result, polars_table = read_data_polars()
            polars_result["dataset_size"] = size
            if polars_result["status"] == "SUCCESS":
                print(f"✅ Polars Read: {polars_result['records']:,} records in {polars_result['duration_s']:.2f}s ({int(polars_result['records']/polars_result['duration_s']):,} rps)")
                
                # Store data for later validation (not timed)
                validation_data.append({
                    "original_data": original_data,
                    "read_table": polars_table,
                    "endpoint": "Polars",
                    "dataset_size": size,
                    "run": run + 1
                })
            else:
                print(f"❌ Polars Read failed: {polars_result['status']}")
            all_results.append(polars_result)
            
            # Small delay between read tests
            time.sleep(0.5)
            
            # Test Arrow read endpoint (performance measurement only)
            print(f"🔄 Testing Arrow read endpoint...")
            arrow_result, arrow_table = read_data_arrow()
            arrow_result["dataset_size"] = size
            if arrow_result["status"] == "SUCCESS":
                print(f"✅ Arrow Read: {arrow_result['records']:,} records in {arrow_result['duration_s']:.2f}s ({int(arrow_result['records']/arrow_result['duration_s']):,} rps)")
                
                # Store data for later validation (not timed)
                validation_data.append({
                    "original_data": original_data,
                    "read_table": arrow_table,
                    "endpoint": "Arrow",
                    "dataset_size": size,
                    "run": run + 1
                })
            else:
                print(f"❌ Arrow Read failed: {arrow_result['status']}")
            all_results.append(arrow_result)
    
    # === INDEPENDENT DATA INTEGRITY VALIDATION (NOT TIMED) ===
    print(f"\n" + "="*80)
    print("🔍 PERFORMING END-TO-END DATA INTEGRITY VALIDATION")
    print("="*80)
    print("ℹ️  This validation step is independent and not included in performance measurements")
    
    validation_results = []
    if validation_data:
        for i, validation_item in enumerate(validation_data, 1):
            endpoint = validation_item["endpoint"]
            size = validation_item["dataset_size"]
            run = validation_item["run"]
            
            print(f"\n🔍 Validating {endpoint} (Dataset: {size:,}, Run: {run})...")
            
            validation_result = validate_data_integrity(
                validation_item["original_data"], 
                validation_item["read_table"]
            )
            validation_result["endpoint"] = endpoint
            validation_result["dataset_size"] = size
            validation_result["run"] = run
            validation_results.append(validation_result)
            
            if validation_result["status"] == "SUCCESS":
                print(f"   ✅ {endpoint}: {validation_result['message']}")
            else:
                print(f"   ❌ {endpoint}: {validation_result.get('error', 'Unknown error')}")
    else:
        print("⚠️  No validation data available (no successful reads)")
    
    print(f"\n" + "="*80)

    print_results_table(all_results)
    csv_file_path = save_results_to_csv(all_results)
    
    # === ENHANCE CSV WITH INTERNAL THROUGHPUT DATA ===
    print(f"\n" + "="*80)
    print("📊 ENHANCING RESULTS WITH INTERNAL THROUGHPUT DATA")
    print("="*80)
    print("ℹ️  Reading app.log to extract true internal processing speeds")
    
    # Parse log file for internal throughput data
    internal_operations = parse_app_log_for_internal_throughput()
    
    if internal_operations:
        # Match internal data to benchmark results
        enhanced_results = match_internal_throughput_to_results(all_results, internal_operations)
        
        # Update CSV file with internal throughput
        if csv_file_path:
            update_csv_with_internal_throughput(csv_file_path, enhanced_results)
    else:
        print("⚠️  No internal operation data found in logs")
    
    print(f"\n" + "="*80)
    
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
    
    # Data integrity validation summary
    if validation_results:
        print(f"\n🔍 DATA INTEGRITY VALIDATION:")
        successful_validations = [v for v in validation_results if v['status'] == 'SUCCESS']
        failed_validations = [v for v in validation_results if v['status'] in ['FAILED', 'ERROR']]
        
        print(f"   Total validations performed: {len(validation_results)}")
        print(f"   ✅ Successful validations: {len(successful_validations)}/{len(validation_results)}")
        
        if failed_validations:
            print(f"   ❌ Failed validations: {len(failed_validations)}/{len(validation_results)}")
            for validation in failed_validations:
                endpoint = validation.get('endpoint', 'Unknown')
                error = validation.get('error', 'Unknown error')
                print(f"      - {endpoint}: {error}")
        
        if successful_validations:
            print(f"   📋 Validation details:")
            for validation in successful_validations:
                endpoint = validation.get('endpoint', 'Unknown')
                size = validation.get('dataset_size', 'Unknown')
                fields = validation.get('validated_fields', 'Unknown')
                print(f"      - {endpoint} ({size:,} records): {fields} fields validated")
                
                # Show sample data for verification
                if 'first_row_sample' in validation:
                    first_sample = validation['first_row_sample']
                    print(f"        First row sample: {first_sample}")
        
        print(f"\n🎯 END-TO-END VALIDATION RESULT:")
        if len(successful_validations) == len(validation_results):
            print(f"   ✅ ALL DATA INTEGRITY CHECKS PASSED")
            print(f"   🎉 Complete write-read cycle validated successfully!")
        else:
            print(f"   ❌ SOME DATA INTEGRITY CHECKS FAILED")
            print(f"   ⚠️  Please review the validation errors above")


if __name__ == "__main__":
    main()
