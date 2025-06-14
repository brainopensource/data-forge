import time
import json
from typing import List, Dict, Any
import requests


def load_json_test_data(file_path: str = "external/mocked_response_100K-4.json") -> List[Dict[str, Any]]:
    """Load test data from JSON file and normalize field names"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract records from the 'value' array
        records = data.get('value', [])
        
        # Normalize field names to match our schema
        normalized_records = []
        for record in records:
            normalized_record = {
                "field_code": record.get("field_code"),
                "field_name": record.get("_field_name", record.get("field_name", "")),
                "well_code": record.get("well_code"),
                "well_reference": record.get("_well_reference", record.get("well_reference", "")),
                "well_name": record.get("well_name", ""),
                "production_period": record.get("production_period", ""),
                "days_on_production": record.get("days_on_production", 0),
                "oil_production_kbd": record.get("oil_production_kbd", 0.0),
                "gas_production_mmcfd": record.get("gas_production_mmcfd", 0.0),
                "liquids_production_kbd": record.get("liquids_production_kbd", 0.0),
                "water_production_kbd": record.get("water_production_kbd", 0.0),
                "data_source": record.get("data_source", ""),
                "source_data": record.get("source_data", ""),
                "partition_0": record.get("partition_0", "latest")
            }
            normalized_records.append(normalized_record)
        
        print(f"✅ Loaded {len(normalized_records):,} records from {file_path}")
        return normalized_records
        
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return []
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
        return []
    except Exception as e:
        print(f"❌ Error loading JSON data: {e}")
        return []

def run_load_data_fast():
        
    # Load the JSON data
    json_data = load_json_test_data()

    if not json_data:
        return {
            "success": False,
            "error": "Failed to load JSON test data"
        }

    print(f"📊 Loaded {len(json_data):,} records from JSON file")

    results = {
        "data_source": "mocked_response_100K-4.json",
        "total_records": len(json_data),
        "tests": {}
    }

    # Test 2: High-performance bulk insert
    print(f"\n🚀 Testing high-performance bulk insert with {len(json_data):,} real records...")
    start_time = time.perf_counter()

    hp_response = requests.post(
        f"{BASE_URL}/api/v1/high-performance/ultra-fast-bulk/{SCHEMA_NAME}",
        json=json_data,
        headers={"Content-Type": "application/json"}
    )

    hp_duration = (time.perf_counter() - start_time) * 1000

    if hp_response.status_code == 200:
        hp_result = hp_response.json()
        results["tests"]["high_performance"] = {
            "success": True,
            "duration_ms": hp_duration,
            "throughput_rps": hp_result.get("performance_metrics", {}).get("throughput_rps", 0),
            "records_processed": len(json_data),
            "optimization": hp_result.get("optimization", "unknown")
        }
        print(f"✅ High-performance insert completed: {hp_duration:.2f}ms ({hp_result.get('performance_metrics', {}).get('throughput_rps', 0):,} records/sec)")
    else:
        results["tests"]["high_performance"] = {
            "success": False,
            "error": hp_response.text,
            "status_code": hp_response.status_code
        }
        print(f"❌ High-performance insert failed: {hp_response.status_code}")


def run_load_data_slow():
        
    # Load the JSON data
    json_data = load_json_test_data()

    if not json_data:
        return {
            "success": False,
            "error": "Failed to load JSON test data"
        }

    print(f"📊 Loaded {len(json_data):,} records from JSON file")

    results = {
        "data_source": "mocked_response_100K-4.json",
        "total_records": len(json_data),
        "tests": {}
    }


    # Test 1: Traditional bulk insert
    print(f"\n🔄 Testing traditional bulk insert with {len(json_data):,} real records...")
    start_time = time.perf_counter()

    traditional_response = requests.post(
        f"{BASE_URL}/api/v1/records/bulk",
        json={
            "schema_name": SCHEMA_NAME,
            "data": json_data
        },
        headers={"Content-Type": "application/json"}
    )

    traditional_duration = (time.perf_counter() - start_time) * 1000

    if traditional_response.status_code == 201:
        traditional_throughput = len(json_data) / (traditional_duration / 1000) if traditional_duration > 0 else 0
        results["tests"]["traditional"] = {
            "success": True,
            "duration_ms": traditional_duration,
            "throughput_rps": int(traditional_throughput),
            "records_processed": len(json_data)
        }
        print(f"✅ Traditional insert completed: {traditional_duration:.2f}ms ({int(traditional_throughput):,} records/sec)")
    else:
        results["tests"]["traditional"] = {
            "success": False,
            "error": traditional_response.text,
            "status_code": traditional_response.status_code
        }
        print(f"❌ Traditional insert failed: {traditional_response.status_code}")



if __name__ == "__main__":
    print('Startin')
    BASE_URL = "http://localhost:8080"
    SCHEMA_NAME = "well_production"
    run_load_data_fast()
    run_load_data_slow()
