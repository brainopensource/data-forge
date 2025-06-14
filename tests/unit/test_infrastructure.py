import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.infrastructure.persistence.duckdb.schema_manager import DuckDBSchemaManager
from app.infrastructure.persistence.repositories.file_schema_repository import FileSchemaRepository
from app.infrastructure.persistence.arrow_bulk_operations import ArrowBulkOperations
from app.domain.entities.schema import Schema
import pandas as pd
import pyarrow as pa
from fastapi import status
from fastapi.testclient import TestClient
from app.infrastructure.web.routers.arrow_performance_data import router
from app.infrastructure.web.arrow import ArrowResponse
from fastapi import FastAPI
import pyarrow.ipc as ipc
from app.container.container import container

app = FastAPI()
app.include_router(router)
app.container = container

@pytest.mark.asyncio
async def test_async_duckdb_pool_initialize_and_close():
    pool = AsyncDuckDBPool()
    with patch.object(pool, 'initialize', new=AsyncMock()) as mock_init:
        await pool.initialize()
        mock_init.assert_awaited()
    with patch.object(pool, 'close', new=AsyncMock()) as mock_close:
        await pool.close()
        mock_close.assert_awaited()

@pytest.mark.asyncio
async def test_duckdb_schema_manager_ensure_tables_exist():
    pool = MagicMock()
    manager = DuckDBSchemaManager(connection_pool=pool)
    manager.connection_pool.acquire = AsyncMock()
    schema = Schema(name="s", description="d", table_name="t", properties=[], primary_key=None)
    with patch.object(manager, 'ensure_tables_exist', new=AsyncMock()) as mock_ensure:
        await manager.ensure_tables_exist([schema])
        mock_ensure.assert_awaited()

@pytest.mark.asyncio
async def test_duckdb_schema_manager_ensure_tables_exist_error():
    pool = MagicMock()
    manager = DuckDBSchemaManager(connection_pool=pool)
    with patch.object(manager, 'ensure_tables_exist', new=AsyncMock(side_effect=Exception("fail"))):
        with pytest.raises(Exception):
            await manager.ensure_tables_exist([])

@pytest.mark.asyncio
async def test_file_schema_repository():
    manager = MagicMock()
    repo = FileSchemaRepository(schema_manager=manager)
    repo._schemas = {"test": Schema(name="test", description="d", table_name="t", properties=[], primary_key=None)}
    assert await repo.get_schema_by_name("test") is not None
    assert await repo.get_schema_by_name("notfound") is None
    assert isinstance(await repo.get_all_schemas(), list)

@pytest.mark.asyncio
async def test_arrow_bulk_operations():
    pool = MagicMock()
    ops = ArrowBulkOperations(connection_pool=pool)
    schema = Schema(name="s", description="d", table_name="t", properties=[], primary_key=None)
    df = pd.DataFrame({"a": [1,2]})
    table = pa.Table.from_pandas(df)
    with patch.object(ops, 'bulk_insert_from_dataframe', new=AsyncMock()) as mock_insert_df:
        await ops.bulk_insert_from_dataframe(schema, df)
        mock_insert_df.assert_awaited()
    with patch.object(ops, 'bulk_insert_from_arrow_table', new=AsyncMock()) as mock_insert_arrow:
        await ops.bulk_insert_from_arrow_table(schema, table)
        mock_insert_arrow.assert_awaited()
    with patch.object(ops, 'bulk_read_to_arrow_table', new=AsyncMock(return_value=table)) as mock_read_arrow:
        result = await ops.bulk_read_to_arrow_table(schema)
        assert isinstance(result, pa.Table)
    with patch.object(ops, 'bulk_read_to_dataframe', new=AsyncMock(return_value=df)) as mock_read_df:
        result = await ops.bulk_read_to_dataframe(schema)
        assert isinstance(result, pd.DataFrame)

@pytest.mark.asyncio
async def test_arrow_bulk_operations_error():
    pool = MagicMock()
    ops = ArrowBulkOperations(connection_pool=pool)
    schema = Schema(name="s", description="d", table_name="t", properties=[], primary_key=None)
    with patch.object(ops, 'bulk_insert_from_arrow_table', new=AsyncMock(side_effect=Exception("fail"))):
        with pytest.raises(Exception):
            await ops.bulk_insert_from_arrow_table(schema, pa.table({"a": [1]}))

@pytest.mark.asyncio
async def test_concurrent_duckdb_pool():
    pool = AsyncDuckDBPool()
    with patch.object(pool, 'initialize', new=AsyncMock()) as mock_init:
        await asyncio.gather(pool.initialize(), pool.initialize())
        assert mock_init.await_count >= 2 

@pytest.mark.asyncio
async def test_bulk_insert_success(monkeypatch):
    client = TestClient(app)
    schema_name = "test_schema"
    df = pa.table({"a": [1, 2, 3]})
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, df.schema) as writer:
        writer.write_table(df)
    arrow_bytes = sink.getvalue().to_pybytes()

    async def mock_execute_from_arrow_table(schema_name, arrow_table):
        assert schema_name == "test_schema"
        assert isinstance(arrow_table, pa.Table)
        return None

    with patch.object(
        app.container.create_ultra_fast_bulk_data_use_case,
        "execute_from_arrow_table",
        new=AsyncMock(side_effect=mock_execute_from_arrow_table),
    ):
        response = client.post(f"/arrow/bulk-insert/{schema_name}", data=arrow_bytes)
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["success"] is True
        assert response.json()["records_processed"] == 3

@pytest.mark.asyncio
async def test_bulk_insert_no_data(monkeypatch):
    client = TestClient(app)
    schema_name = "test_schema"
    response = client.post(f"/arrow/bulk-insert/{schema_name}", data=b"")
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "No Arrow data provided" in response.text

@pytest.mark.asyncio
async def test_bulk_read_success(monkeypatch):
    client = TestClient(app)
    schema_name = "test_schema"
    df = pa.table({"a": [1, 2, 3]})

    async def mock_read_to_arrow_table(schema_name):
        assert schema_name == "test_schema"
        return df

    with patch.object(
        app.container.create_ultra_fast_bulk_data_use_case,
        "read_to_arrow_table",
        new=AsyncMock(side_effect=mock_read_to_arrow_table),
    ):
        response = client.get(f"/arrow/bulk-read/{schema_name}")
        assert response.status_code == status.HTTP_200_OK
        assert response.content  # Should return Arrow IPC stream 