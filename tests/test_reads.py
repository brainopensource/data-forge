import pytest
from fastapi.testclient import TestClient
import pyarrow.ipc as ipc
import io

SCHEMA_NAME = "test_write_schema"

def test_polars_read(client: TestClient):
    """
    Test reading data for a schema using the polars-read endpoint.
    This test depends on data being written previously.
    """
    response = client.get(f"/read/polars/{SCHEMA_NAME}")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"

    # Verify the Arrow stream content
    with ipc.open_stream(io.BytesIO(response.content)) as reader:
        arrow_table = reader.read_all()
        assert arrow_table.num_rows > 0
        assert "oil_production_kbd" in arrow_table.schema.names
        assert "well_name" in arrow_table.schema.names

def test_duckdb_read(client: TestClient):
    """
    Test reading data using the duckdb-read endpoint.
    """
    response = client.get(f"/read/duckdb/{SCHEMA_NAME}")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"

    with ipc.open_stream(io.BytesIO(response.content)) as reader:
        arrow_table = reader.read_all()
        assert arrow_table.num_rows > 0
        assert "gas_production_mmcfd" in arrow_table.schema.names 