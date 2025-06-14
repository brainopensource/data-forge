import pytest
import pyarrow as pa
from unittest.mock import AsyncMock, MagicMock
from app.application.command_handlers.bulk_data_command_handlers import BulkDataCommandHandler
from app.application.use_cases.create_ultra_fast_bulk_data import CreateUltraFastBulkDataUseCase
from app.application.commands.bulk_data_commands import BulkInsertFromArrowTableCommand, BulkReadToArrowCommand
from app.domain.exceptions import SchemaNotFoundException
from hypothesis import given, strategies as st
import pandas as pd

@pytest.mark.asyncio
async def test_bulk_data_command_handler():
    schema_repo = MagicMock()
    arrow_ops = MagicMock()
    handler = BulkDataCommandHandler(schema_repository=schema_repo, arrow_operations=arrow_ops)
    schema_repo.get_schema_by_name = AsyncMock(return_value=MagicMock())
    arrow_ops.bulk_insert_from_arrow_table = AsyncMock()
    arrow_ops.bulk_read_to_arrow_table = AsyncMock(return_value=pa.table({"a": [1]}))
    cmd_insert = BulkInsertFromArrowTableCommand(schema_name="s", arrow_table=pa.table({"a": [1]}))
    await handler.handle_bulk_insert_from_arrow_table(cmd_insert)
    arrow_ops.bulk_insert_from_arrow_table.assert_awaited()
    cmd_read = BulkReadToArrowCommand(schema_name="s")
    result = await handler.handle_bulk_read_to_arrow(cmd_read)
    arrow_ops.bulk_read_to_arrow_table.assert_awaited()
    assert isinstance(result, pa.Table)

@pytest.mark.asyncio
async def test_bulk_data_command_handler_schema_not_found():
    schema_repo = MagicMock()
    arrow_ops = MagicMock()
    handler = BulkDataCommandHandler(schema_repository=schema_repo, arrow_operations=arrow_ops)
    schema_repo.get_schema_by_name = AsyncMock(return_value=None)
    cmd_insert = BulkInsertFromArrowTableCommand(schema_name="notfound", arrow_table=pa.table({"a": [1]}))
    with pytest.raises(SchemaNotFoundException):
        await handler.handle_bulk_insert_from_arrow_table(cmd_insert)

@pytest.mark.asyncio
async def test_create_ultra_fast_bulk_data_use_case():
    handler = MagicMock()
    use_case = CreateUltraFastBulkDataUseCase(command_handler=handler)
    handler.handle_bulk_insert_from_arrow_table = AsyncMock()
    handler.handle_bulk_read_to_arrow = AsyncMock(return_value=pa.table({"a": [1]}))
    await use_case.execute_from_arrow_table("s", pa.table({"a": [1]}))
    handler.handle_bulk_insert_from_arrow_table.assert_awaited()
    result = await use_case.read_to_arrow_table("s")
    handler.handle_bulk_read_to_arrow.assert_awaited()
    assert isinstance(result, pa.Table)

@given(st.lists(st.integers(min_value=-(2**63), max_value=2**63-1), min_size=1, max_size=10))
def test_bulk_insert_from_arrow_table_property(data):
    df = pd.DataFrame({"a": data})
    table = pa.Table.from_pandas(df)
    assert isinstance(table, pa.Table) 