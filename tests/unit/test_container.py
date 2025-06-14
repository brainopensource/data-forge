import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from app.container.container import Container

@pytest.mark.asyncio
async def test_container_startup_and_shutdown():
    container = Container()
    with patch.object(container.connection_pool, 'initialize', new=AsyncMock()) as mock_init, \
         patch.object(container.connection_pool, 'close', new=AsyncMock()) as mock_close, \
         patch.object(container.schema_repository, 'initialize', new=AsyncMock()) as mock_repo_init:
        await container.startup()
        mock_init.assert_awaited()
        mock_repo_init.assert_awaited()
        await container.shutdown()
        mock_close.assert_awaited()

def test_container_dependencies():
    container = Container()
    # Check that all main dependencies are wired
    assert container.connection_pool is not None
    assert container.schema_manager is not None
    assert container.schema_repository is not None
    assert container.arrow_bulk_operations is not None
    assert container.bulk_data_command_handler is not None
    assert container.create_ultra_fast_bulk_data_use_case is not None

@pytest.mark.asyncio
async def test_container_startup_non_file_schema_repo():
    container = Container()
    container.schema_repository = MagicMock()  # Not FileSchemaRepository
    with patch.object(container.connection_pool, 'initialize', new=AsyncMock()) as mock_init:
        await container.startup()
        mock_init.assert_awaited()

@pytest.mark.asyncio
async def test_container_double_startup_shutdown():
    container = Container()
    with patch.object(container.connection_pool, 'initialize', new=AsyncMock()) as mock_init, \
         patch.object(container.connection_pool, 'close', new=AsyncMock()) as mock_close, \
         patch.object(container.schema_repository, 'initialize', new=AsyncMock()) as mock_repo_init:
        await container.startup()
        await container.startup()  # Should not error
        await container.shutdown()
        await container.shutdown()  # Should not error 