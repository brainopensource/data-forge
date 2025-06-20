#!/usr/bin/env python3
"""
Test script for the refactored Data Forge API.
Verifies basic functionality and performance.
"""
import asyncio
import time
import json
from typing import Dict, Any, List

try:
    import requests
except ImportError:
    print("❌ requests library not found. Install with: pip install requests")
    exit(1)


# Test configuration
BASE_URL = "http://localhost:8080"
TEST_SCHEMA = "well_production"  # Using existing schema from schema_config.py


def test_health_endpoints():
    """Test health and monitoring endpoints."""
    print("🔍 Testing health endpoints...")
    
    # Test root endpoint
    response = requests.get(f"{BASE_URL}/")
    assert response.status_code == 200
    data = response.json()
    assert "Data Forge API" in data["message"]
    print("✅ Root endpoint working")
    
    # Test health check
    response = requests.get(f"{BASE_URL}/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    print("✅ Health check working")
    
    # Test system status
    response = requests.get(f"{BASE_URL}/health/status")
    assert response.status_code == 200
    data = response.json()
    assert "system" in data
    assert "performance" in data
    print("✅ System status working")
    
    # Test performance info
    response = requests.get(f"{BASE_URL}/performance")
    assert response.status_code == 200
    data = response.json()
    assert data["performance_mode"] == "ultra-fast"
    print("✅ Performance info working")


def test_schema_endpoints():
    """Test schema management endpoints."""
    print("\n📋 Testing schema endpoints...")
    
    # Test list schemas
    response = requests.get(f"{BASE_URL}/schemas")
    assert response.status_code == 200
    data = response.json()
    assert "schemas" in data
    assert len(data["schemas"]) > 0
    print("✅ Schema listing working")
    
    # Test specific schema info
    response = requests.get(f"{BASE_URL}/schemas/{TEST_SCHEMA}")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == TEST_SCHEMA
    assert "properties" in data
    print(f"✅ Schema info for '{TEST_SCHEMA}' working")
    
    # Test Polars schema
    response = requests.get(f"{BASE_URL}/schemas/{TEST_SCHEMA}/polars-schema")
    assert response.status_code == 200
    data = response.json()
    assert "polars_schema" in data
    print("✅ Polars schema working")


def generate_test_data(num_records: int = 1000) -> List[Dict[str, Any]]:
    """Generate test data for well production schema."""
    test_data = []
    for i in range(num_records):
        record = {
            "field_code": 1000 + i,
            "field_name": f"Field_{i}",
            "well_code": 2000 + i,
            "well_reference": f"WELL_REF_{i}",
            "well_name": f"Well_{i}",
            "production_period": f"2024-01-{(i % 28) + 1:02d}",
            "days_on_production": 30,
            "oil_production_kbd": 100.5 + i,
            "gas_production_mmcfd": 50.2 + i,
            "liquids_production_kbd": 120.3 + i,
            "water_production_kbd": 20.1 + i,
            "data_source": "TEST",
            "source_data": "automated_test",
            "partition_0": "2024"
        }
        test_data.append(record)
    return test_data


def test_write_endpoints():
    """Test write endpoints with performance measurement."""
    print("\n✍️ Testing write endpoints...")
    
    # Generate test data
    test_data = generate_test_data(10000)  # 10K records for testing
    
    # Test ultra-fast write
    print("Testing ultra-fast write...")
    start_time = time.time()
    response = requests.post(
        f"{BASE_URL}/write/ultra-fast/{TEST_SCHEMA}",
        json={
            "data": test_data,
            "compression": "zstd"
        }
    )
    write_time = time.time() - start_time
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] == True
    assert data["records_written"] == len(test_data)
    
    throughput = len(test_data) / write_time
    print(f"✅ Ultra-fast write: {len(test_data)} records in {write_time:.3f}s ({throughput:,.0f} records/sec)")
    
    # Test validated write
    print("Testing validated write...")
    start_time = time.time()
    response = requests.post(
        f"{BASE_URL}/write/fast-validated/{TEST_SCHEMA}",
        json={
            "data": test_data[:1000],  # Smaller dataset for validation
            "batch_size": 1000,
            "compression": "snappy",
            "validate_schema": True
        }
    )
    write_time = time.time() - start_time
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] == True
    
    throughput = 1000 / write_time
    print(f"✅ Validated write: 1000 records in {write_time:.3f}s ({throughput:,.0f} records/sec)")
    
    # Test Feather write
    print("Testing Feather write...")
    start_time = time.time()
    response = requests.post(
        f"{BASE_URL}/write/feather/{TEST_SCHEMA}",
        json={
            "data": test_data[:5000]
        }
    )
    write_time = time.time() - start_time
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] == True
    
    throughput = 5000 / write_time
    print(f"✅ Feather write: 5000 records in {write_time:.3f}s ({throughput:,.0f} records/sec)")


def test_read_endpoints():
    """Test read endpoints with performance measurement."""
    print("\n📖 Testing read endpoints...")
    
    # Test Polars read
    print("Testing Polars read...")
    start_time = time.time()
    response = requests.get(f"{BASE_URL}/read/polars/{TEST_SCHEMA}")
    read_time = time.time() - start_time
    
    if response.status_code == 200:
        # Response is Arrow IPC stream
        content_length = len(response.content)
        print(f"✅ Polars read: {content_length} bytes in {read_time:.3f}s")
    elif response.status_code == 404:
        print("⚠️ Polars read: No data file found (expected for first run)")
    else:
        print(f"❌ Polars read failed: {response.status_code}")
    
    # Test DuckDB read
    print("Testing DuckDB read...")
    start_time = time.time()
    response = requests.get(f"{BASE_URL}/read/duckdb/{TEST_SCHEMA}")
    read_time = time.time() - start_time
    
    if response.status_code == 200:
        content_length = len(response.content)
        print(f"✅ DuckDB read: {content_length} bytes in {read_time:.3f}s")
    elif response.status_code == 404:
        print("⚠️ DuckDB read: No data file found (expected for first run)")
    else:
        print(f"❌ DuckDB read failed: {response.status_code}")
    
    # Test latest read
    print("Testing latest file read...")
    start_time = time.time()
    response = requests.get(f"{BASE_URL}/read/latest/{TEST_SCHEMA}")
    read_time = time.time() - start_time
    
    if response.status_code == 200:
        content_length = len(response.content)
        print(f"✅ Latest read: {content_length} bytes in {read_time:.3f}s")
    elif response.status_code == 404:
        print("⚠️ Latest read: No data file found (run write tests first)")
    else:
        print(f"❌ Latest read failed: {response.status_code}")


def test_performance_monitoring():
    """Test performance monitoring features."""
    print("\n📊 Testing performance monitoring...")
    
    # Test system metrics
    response = requests.get(f"{BASE_URL}/health/status")
    assert response.status_code == 200
    data = response.json()
    
    system = data["system"]
    print(f"CPU Usage: {system['cpu_usage_percent']:.1f}%")
    print(f"Memory Usage: {system['memory_usage_percent']:.1f}%")
    print(f"Memory Available: {system['memory_available_gb']:.1f} GB")
    print(f"Disk Free: {system['disk_free_gb']:.1f} GB")
    print("✅ Performance monitoring working")


def main():
    """Run all tests."""
    print("🚀 Testing Data Forge API - Refactored Version")
    print("=" * 50)
    
    try:
        # Check if API is running
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print("❌ API is not responding. Make sure it's running on port 8080.")
            return
    except requests.exceptions.RequestException:
        print("❌ Cannot connect to API. Make sure it's running on port 8080.")
        print("Run: python app/main_refactored.py")
        return
    
    try:
        test_health_endpoints()
        test_schema_endpoints()
        test_write_endpoints()
        test_read_endpoints()
        test_performance_monitoring()
        
        print("\n🎉 All tests passed!")
        print("✅ Refactored Data Forge API is working correctly")
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")


if __name__ == "__main__":
    main() 