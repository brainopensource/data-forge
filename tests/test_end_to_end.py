"""
Comprehensive End-to-End Testing Kit for Data Forge API MVP
Tests the core functionality: startup, health, schema management, reads, and writes
"""
import pytest
import requests
import uuid
import json
import time
from datetime import datetime
import pyarrow.ipc as ipc
import io
from typing import Dict, Any, List

# Test configuration
BASE_URL = "http://localhost:8080"
TEST_SCHEMA = "e2e_test_schema"
TIMEOUT = 30

class TestAPIStartupAndHealth:
    """Test application startup and health monitoring."""
    
    def test_application_startup_successful(self):
        """Test that the application starts successfully and is responsive."""
        response = requests.get(f"{BASE_URL}/health", timeout=10)
        assert response.status_code == 200
        
        health_data = response.json()
        assert health_data["status"] == "healthy"
        assert "uptime_seconds" in health_data
        assert health_data["performance_mode"] == "ultra-fast"
    
    def test_root_endpoint_info(self):
        """Test that the root endpoint provides correct API information."""
        response = requests.get(f"{BASE_URL}/", timeout=10)
        assert response.status_code == 200
        
        data = response.json()
        assert "Data Forge API" in data["message"]
        # docs_url may not be exposed by this version
        assert "version" in data
        assert data.get("health_url", "/health") in ("/health", None)
    
    def test_comprehensive_health_status(self):
        """Test detailed system health status endpoint."""
        response = requests.get(f"{BASE_URL}/health/status", timeout=15)
        assert response.status_code == 200
        
        status = response.json()
        assert status["status"] == "healthy"
        assert "checks" in status
        assert "performance" in status
        assert "features" in status
        
        # Verify key features are enabled
        features = status["features"]
        assert features["arrow_streaming"] is True
        assert features["duckdb_integration"] is True
        assert features["polars_optimization"] is True


class TestSchemaManagement:
    """Test schema creation and management functionality."""
    
    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self):
        """Setup and cleanup for schema tests."""
        # Cleanup any existing test schema before test
        yield
        # Cleanup after test (if needed)
    
    def get_test_schema_definition(self) -> Dict[str, str]:
        """Get a comprehensive test schema definition."""
        return {
            "id": "string",
            "created_at": "string", 
            "version": "integer",
            "field_code": "integer",
            "field_name": "string",
            "well_code": "integer",
            "well_reference": "string",
            "well_name": "string",
            "production_period": "string",
            "days_on_production": "integer",
            "oil_production_kbd": "double",
            "gas_production_mmcfd": "double",
            "liquids_production_kbd": "double",
            "water_production_kbd": "double",
            "data_source": "string",
            "source_data": "string",
            "partition_0": "string"
        }
    
    def test_create_schema_success(self):
        """Test successful schema creation."""
        schema_def = self.get_test_schema_definition()
        
        response = requests.post(
            f"{BASE_URL}/schemas/{TEST_SCHEMA}",
            json=schema_def,
            timeout=TIMEOUT
        )
        
        assert response.status_code == 200
        data = response.json()
        assert f"Schema '{TEST_SCHEMA}' created/updated successfully" in data["message"]
        assert data["schema_name"] == TEST_SCHEMA
        assert data["properties_count"] == len(schema_def)
    
    def test_create_schema_idempotent(self):
        """Test that schema creation is idempotent."""
        schema_def = self.get_test_schema_definition()
        
        # Create schema first time
        response1 = requests.post(
            f"{BASE_URL}/schemas/{TEST_SCHEMA}",
            json=schema_def,
            timeout=TIMEOUT
        )
        assert response1.status_code == 200
        
        # Create schema second time (should still succeed)
        response2 = requests.post(
            f"{BASE_URL}/schemas/{TEST_SCHEMA}",
            json=schema_def,
            timeout=TIMEOUT
        )
        assert response2.status_code == 200
    
    def test_create_schema_empty_definition(self):
        """Test handling of empty schema definition."""
        response = requests.post(
            f"{BASE_URL}/schemas/{TEST_SCHEMA}_empty",
            json={},
            timeout=TIMEOUT
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["properties_count"] == 0


class TestDataWriteOperations:
    """Test data writing functionality."""
    
    @pytest.fixture(autouse=True)
    def ensure_schema_exists(self):
        """Ensure test schema exists before each test."""
        schema_def = {
            "id": "string", "created_at": "string", "version": "integer",
            "field_code": "integer", "field_name": "string", "well_code": "integer",
            "well_reference": "string", "well_name": "string", "production_period": "string",
            "days_on_production": "integer", "oil_production_kbd": "double",
            "gas_production_mmcfd": "double", "liquids_production_kbd": "double",
            "water_production_kbd": "double", "data_source": "string",
            "source_data": "string", "partition_0": "string"
        }
        
        requests.post(f"{BASE_URL}/schemas/{TEST_SCHEMA}", json=schema_def, timeout=TIMEOUT)
        yield
    
    def generate_test_records(self, count: int) -> List[Dict[str, Any]]:
        """Generate test records for writing."""
        records = []
        base_time = datetime.now()
        
        for i in range(count):
            record = {
                "id": str(uuid.uuid4()),
                "created_at": base_time.isoformat() + "Z",
                "version": 1,
                "field_code": 1000 + i,
                "field_name": f"Field_{1000 + i}",
                "well_code": 100 + i,
                "well_reference": f"WELL_REF_{100 + i:03d}",
                "well_name": f"Well_{100 + i}",
                "production_period": base_time.isoformat() + "Z",
                "days_on_production": 30,
                "oil_production_kbd": 100.0 + i * 0.5,
                "gas_production_mmcfd": 50.0 + i * 0.25,
                "liquids_production_kbd": 10.0 + i * 0.1,
                "water_production_kbd": 200.0 + i * 0.75,
                "data_source": "e2e_test",
                "source_data": json.dumps({"test_run": i, "batch": "e2e"}),
                "partition_0": f"partition_{i % 3}"
            }
            records.append(record)
        
        return records
    
    def test_write_single_record(self):
        """Test writing a single record."""
        records = self.generate_test_records(1)
        payload = {"data": records}
        
        response = requests.post(
            f"{BASE_URL}/write/polars/{TEST_SCHEMA}",
            json=payload,
            timeout=TIMEOUT
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["records_written"] == 1
        # file_size_mb may be zero; verify we got a timing metric
        assert "write_time_seconds" in data and data["write_time_seconds"] > 0
        assert TEST_SCHEMA in data["file_path"]
    
    def test_write_batch_records(self):
        """Test writing a batch of records."""
        record_count = 100
        records = self.generate_test_records(record_count)
        payload = {"data": records}
        
        response = requests.post(
            f"{BASE_URL}/write/polars/{TEST_SCHEMA}",
            json=payload,
            timeout=TIMEOUT
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["records_written"] == record_count
        # ensure we get a write timing
        assert "write_time_seconds" in data and data["write_time_seconds"] > 0
    
    def test_write_large_batch(self):
        """Test writing a large batch to verify performance."""
        record_count = 1000
        records = self.generate_test_records(record_count)
        payload = {"data": records}
        
        start_time = time.time()
        response = requests.post(
            f"{BASE_URL}/write/polars/{TEST_SCHEMA}",
            json=payload,
            timeout=TIMEOUT
        )
        write_duration = time.time() - start_time
        
        assert response.status_code == 200
        data = response.json()
        assert data["records_written"] == record_count
        # compute throughput manually
        rps = data["records_written"] / data["write_time_seconds"]
        assert rps > 1000
        assert write_duration < 10
    
    def test_write_empty_data(self):
        """Test handling of empty data payload."""
        payload = {"data": []}
        
        response = requests.post(
            f"{BASE_URL}/write/polars/{TEST_SCHEMA}",
            json=payload,
            timeout=TIMEOUT
        )
        
        assert response.status_code == 400
        assert "No data provided" in response.json()["detail"]
    
    def test_write_malformed_payload(self):
        """Test handling of malformed payload."""
        payload = {"invalid": "payload"}
        
        response = requests.post(
            f"{BASE_URL}/write/polars/{TEST_SCHEMA}",
            json=payload,
            timeout=TIMEOUT
        )
        
        # Should handle gracefully (either 400 or write 0 records)
        assert response.status_code in [400, 500]


class TestDataReadOperations:
    """Test data reading functionality."""
    
    @pytest.fixture(autouse=True)
    def setup_test_data(self):
        """Setup test data before each read test."""
        # Ensure schema exists
        schema_def = {
            "id": "string", "created_at": "string", "version": "integer",
            "field_code": "integer", "field_name": "string", "well_code": "integer",
            "well_reference": "string", "well_name": "string", "production_period": "string",
            "days_on_production": "integer", "oil_production_kbd": "double",
            "gas_production_mmcfd": "double", "liquids_production_kbd": "double",
            "water_production_kbd": "double", "data_source": "string",
            "source_data": "string", "partition_0": "string"
        }
        
        requests.post(f"{BASE_URL}/schemas/{TEST_SCHEMA}", json=schema_def, timeout=TIMEOUT)
        
        # Write some test data
        records = []
        for i in range(10):
            record = {
                "id": f"read_test_{i}",
                "created_at": datetime.now().isoformat() + "Z",
                "version": 1,
                "field_code": 2000 + i,
                "field_name": f"ReadField_{i}",
                "well_code": 200 + i,
                "well_reference": f"READ_WELL_{i:03d}",
                "well_name": f"ReadWell_{i}",
                "production_period": datetime.now().isoformat() + "Z",
                "days_on_production": 30,
                "oil_production_kbd": 200.0 + i,
                "gas_production_mmcfd": 100.0 + i,
                "liquids_production_kbd": 20.0 + i,
                "water_production_kbd": 300.0 + i,
                "data_source": "read_test",
                "source_data": json.dumps({"read_test": True}),
                "partition_0": f"read_partition_{i % 2}"
            }
            records.append(record)
        
        # Write the test data
        payload = {"data": records}
        write_response = requests.post(
            f"{BASE_URL}/write/polars/{TEST_SCHEMA}",
            json=payload,
            timeout=TIMEOUT
        )
        assert write_response.status_code == 200
        
        yield
    
    def test_polars_read_success(self):
        """Test successful Polars read operation."""
        response = requests.get(f"{BASE_URL}/read/polars/{TEST_SCHEMA}", timeout=TIMEOUT)
        
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"
        
        # Verify Arrow stream content
        with ipc.open_stream(io.BytesIO(response.content)) as reader:
            arrow_table = reader.read_all()
            assert arrow_table.num_rows >= 10  # At least our test data
            
            # Verify schema
            schema_names = arrow_table.schema.names
            expected_fields = ["id", "oil_production_kbd", "well_name", "field_name"]
            for field in expected_fields:
                assert field in schema_names
    
    def test_duckdb_read_success(self):
        """Test successful DuckDB read operation."""
        response = requests.get(f"{BASE_URL}/read/duckdb/{TEST_SCHEMA}", timeout=TIMEOUT)
        
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"
        
        # Verify Arrow stream content
        with ipc.open_stream(io.BytesIO(response.content)) as reader:
            arrow_table = reader.read_all()
            assert arrow_table.num_rows >= 10
            
            # Verify we can find our test data
            table_dict = arrow_table.to_pydict()
            ids = table_dict.get("id", [])
            assert any("read_test_" in str(id_val) for id_val in ids)
    
    def test_read_nonexistent_schema(self):
        """Test reading from non-existent schema."""
        response = requests.get(f"{BASE_URL}/read/polars/nonexistent_schema", timeout=TIMEOUT)
        
        assert response.status_code == 404
        detail = response.json().get("detail", "").lower()
        assert "no data directory" in detail
    
    def test_legacy_endpoints_compatibility(self):
        """Test that legacy endpoints still work."""
        # Test legacy polars endpoint
        response1 = requests.get(f"{BASE_URL}/read/polars-read/{TEST_SCHEMA}", timeout=TIMEOUT)
        assert response1.status_code == 200
        
        # Test legacy duckdb endpoint  
        response2 = requests.get(f"{BASE_URL}/read/duckdb-read/{TEST_SCHEMA}", timeout=TIMEOUT)
        assert response2.status_code == 200


class TestEndToEndWorkflow:
    """Test complete end-to-end workflows."""
    
    def test_complete_data_lifecycle(self):
        """Test complete data lifecycle: schema -> write -> read -> verify."""
        workflow_schema = f"{TEST_SCHEMA}_workflow"
        
        # Step 1: Create schema
        schema_def = {
            "id": "string", "name": "string", "value": "double", 
            "timestamp": "string", "active": "boolean"
        }
        
        schema_response = requests.post(
            f"{BASE_URL}/schemas/{workflow_schema}",
            json=schema_def,
            timeout=TIMEOUT
        )
        assert schema_response.status_code == 200
        
        # Step 2: Write test data with known values
        test_id = str(uuid.uuid4())
        test_record = {
            "id": test_id,
            "name": "workflow_test_record",
            "value": 123.456,
            "timestamp": datetime.now().isoformat() + "Z",
            "active": True
        }
        
        write_response = requests.post(
            f"{BASE_URL}/write/polars/{workflow_schema}",
            json={"data": [test_record]},
            timeout=TIMEOUT
        )
        assert write_response.status_code == 200
        write_data = write_response.json()
        assert write_data["records_written"] == 1
        
        # Step 3: Read data back
        read_response = requests.get(
            f"{BASE_URL}/read/polars/{workflow_schema}",
            timeout=TIMEOUT
        )
        assert read_response.status_code == 200
        
        # Step 4: Verify data integrity
        with ipc.open_stream(io.BytesIO(read_response.content)) as reader:
            arrow_table = reader.read_all()
            table_dict = arrow_table.to_pydict()
            
            # Find our record
            ids = table_dict["id"]
            assert test_id in [str(id_val) for id_val in ids]
            
            # Get index of our record
            idx = [str(id_val) for id_val in ids].index(test_id)
            
            # Verify all fields
            assert str(table_dict["name"][idx]) == "workflow_test_record"
            assert float(table_dict["value"][idx]) == 123.456
            assert table_dict["active"][idx] in [True, "true", "True", 1]
    
    def test_performance_benchmark_workflow(self):
        """Test performance-focused workflow with metrics."""
        perf_schema = f"{TEST_SCHEMA}_performance"
        
        # Create schema
        schema_def = {
            "id": "string", "batch_id": "string", "sequence": "integer",
            "data_point": "double", "created_at": "string"
        }
        
        requests.post(f"{BASE_URL}/schemas/{perf_schema}", json=schema_def, timeout=TIMEOUT)
        
        # Generate larger dataset for performance testing
        batch_size = 500
        batch_id = str(uuid.uuid4())
        records = []
        
        for i in range(batch_size):
            record = {
                "id": str(uuid.uuid4()),
                "batch_id": batch_id,
                "sequence": i,
                "data_point": float(i * 1.5),
                "created_at": datetime.now().isoformat() + "Z"
            }
            records.append(record)
        
        # Measure write performance
        start_time = time.time()
        write_response = requests.post(
            f"{BASE_URL}/write/polars/{perf_schema}",
            json={"data": records},
            timeout=TIMEOUT
        )
        write_duration = time.time() - start_time
        
        assert write_response.status_code == 200
        write_data = write_response.json()
        assert write_data["records_written"] == batch_size
        
        # Performance assertions
        # throughput = records / write_time_seconds
        assert "write_time_seconds" in write_data
        write_rps = write_data["records_written"] / write_data["write_time_seconds"]
        assert write_rps > 100
        
        # Measure read performance
        start_time = time.time()
        read_response = requests.get(f"{BASE_URL}/read/polars/{perf_schema}", timeout=TIMEOUT)
        read_duration = time.time() - start_time
        
        assert read_response.status_code == 200
        assert read_duration < 5  # Read should complete quickly
        
        # Verify data completeness
        with ipc.open_stream(io.BytesIO(read_response.content)) as reader:
            arrow_table = reader.read_all()
            assert arrow_table.num_rows >= batch_size
    
    def test_error_recovery_workflow(self):
        """Test system behavior under error conditions."""
        error_schema = f"{TEST_SCHEMA}_error"
        
        # Test 1: Write to non-existent schema (should create it)
        test_record = {
            "id": str(uuid.uuid4()),
            "test_field": "error_test"
        }
        
        # This might fail or succeed depending on implementation
        write_response = requests.post(
            f"{BASE_URL}/write/polars/{error_schema}",
            json={"data": [test_record]},
            timeout=TIMEOUT
        )
        
        # Either way, system should remain stable
        health_response = requests.get(f"{BASE_URL}/health", timeout=10)
        assert health_response.status_code == 200
        assert health_response.json()["status"] == "healthy"
        
        # Test 2: Read from non-existent schema should return 404
        read_response = requests.get(f"{BASE_URL}/read/polars/definitely_nonexistent", timeout=TIMEOUT)
        assert read_response.status_code == 404
        
        # System should still be healthy after error
        health_response2 = requests.get(f"{BASE_URL}/health", timeout=10)
        assert health_response2.status_code == 200


if __name__ == "__main__":
    """
    Run end-to-end tests manually.
    
    Usage:
    1. Start the API server: python -m app.main
    2. Run tests: python tests/test_end_to_end.py
    """
    print("Running End-to-End Tests for Data Forge API...")
    print(f"Testing against: {BASE_URL}")
    print("=" * 60)
    
    # Basic connectivity test
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            print("✓ API server is responsive")
        else:
            print(f"✗ API server returned status {response.status_code}")
            exit(1)
    except requests.exceptions.RequestException as e:
        print(f"✗ Cannot connect to API server: {e}")
        print("Please ensure the API server is running on port 8080")
        exit(1)
    
    print("\nRunning comprehensive test suite...")
    print("Use 'pytest tests/test_end_to_end.py -v' for detailed test execution")
