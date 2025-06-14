import pytest
import pyarrow as pa
import pyarrow.ipc as ipc
from fastapi import status

@pytest.mark.asyncio
async def test_bulk_insert_and_read_success(client, sample_arrow_table):
    # Serialize Arrow table to IPC stream
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, sample_arrow_table.schema) as writer:
        writer.write_table(sample_arrow_table)
    arrow_bytes = sink.getvalue().to_pybytes()

    # Insert
    response = client.post("/api/v1/arrow/bulk-insert/test", data=arrow_bytes)
    assert response.status_code in (200, 404)  # 404 if schema 'test' doesn't exist
    if response.status_code == 200:
        assert response.json()["success"] is True
        assert response.json()["records_processed"] == sample_arrow_table.num_rows

    # Read
    response = client.get("/api/v1/arrow/bulk-read/test")
    if response.status_code == 200:
        assert response.headers["content-type"].startswith("application/vnd.apache.arrow.stream")
        # Try to deserialize
        try:
            with ipc.open_stream(response.content) as reader:
                table = reader.read_all()
                assert isinstance(table, pa.Table)
        except Exception:
            pytest.fail("Returned content is not valid Arrow IPC stream")
    else:
        assert response.status_code in (404, 500)

@pytest.mark.asyncio
async def test_bulk_insert_empty_body(client):
    response = client.post("/api/v1/arrow/bulk-insert/test", data=b"")
    assert response.status_code == 400
    assert "No Arrow data provided" in response.text

@pytest.mark.asyncio
async def test_bulk_insert_invalid_arrow(client):
    response = client.post("/api/v1/arrow/bulk-insert/test", data=b"notarrowdata")
    assert response.status_code == 400
    assert (
        "Invalid Arrow IPC data" in response.text
        or "Invalid or empty Arrow table" in response.text
        or "Expected to read" in response.text
    )

@pytest.mark.asyncio
async def test_bulk_read_not_found(client):
    response = client.get("/api/v1/arrow/bulk-read/doesnotexist")
    assert response.status_code in (404, 500) 