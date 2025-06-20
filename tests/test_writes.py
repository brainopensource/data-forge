import pytest
from fastapi.testclient import TestClient
import uuid
from datetime import datetime
import random
import json

SCHEMA_NAME = "test_write_schema"

@pytest.fixture(scope="module")
def sample_schema():
    """
    Provides a sample schema definition for testing.
    """
    return {
        "id": "string", "created_at": "string", "version": "integer",
        "field_code": "integer", "field_name": "string", "well_code": "integer",
        "well_reference": "string", "well_name": "string", "production_period": "string",
        "days_on_production": "integer", "oil_production_kbd": "double",
        "gas_production_mmcfd": "double", "liquids_production_kbd": "double",
        "water_production_kbd": "double", "data_source": "string",
        "source_data": "string", "partition_0": "string"
    }

def generate_sample_data(num_records: int) -> list:
    data = []
    base_prod_date = datetime(2023, 1, 1)
    for i in range(num_records):
        created_at_dt = datetime.now()
        prod_date_dt = base_prod_date
        record = {
            "id": str(uuid.uuid4()), "created_at": created_at_dt.isoformat() + "Z", "version": 1,
            "field_code": 100 + i, "field_name": f"Field_{100 + i}", "well_code": 10 + i,
            "well_reference": f"WELL_REF_{10+i}", "well_name": f"Well_{10+i}",
            "production_period": prod_date_dt.isoformat() + "Z", "days_on_production": 30,
            "oil_production_kbd": 150.0 + i, "gas_production_mmcfd": 50.0 + i,
            "liquids_production_kbd": 20.0 + i, "water_production_kbd": 200.0 + i,
            "data_source": "test", "source_data": json.dumps({"run": "test"}), "partition_0": "p0"
        }
        data.append(record)
    return data

def test_create_schema(client: TestClient, sample_schema):
    """
    Test creating a new schema. This is a prerequisite for writing data.
    """
    response = client.post(f"/schemas/{SCHEMA_NAME}", json=sample_schema)
    assert response.status_code == 200
    assert response.json()["message"] == f"Schema '{SCHEMA_NAME}' created/updated successfully."

def test_polars_write(client: TestClient):
    """
    Test the polars-write endpoint. This writes a batch of data and verifies the response.
    """
    sample_data = generate_sample_data(10)
    payload = {"data": sample_data}
    response = client.post(f"/write/polars-write/{SCHEMA_NAME}", json=payload)
    
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["records_written"] == 10
    assert json_response["file_size_mb"] > 0
    assert SCHEMA_NAME in json_response["file_path"] 