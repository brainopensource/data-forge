import pytest
from fastapi.testclient import TestClient
from app.main_windows import app
import os
import shutil

# Fixture to create a TestClient instance for the application
@pytest.fixture(scope="session")
def client():
    """
    Yield a TestClient for the FastAPI app.
    This allows sending requests to the app in tests.
    """
    with TestClient(app) as c:
        yield c

# Fixture to set up a clean test environment before tests run
@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """
    Create necessary directories for testing and clean up after.
    This fixture runs automatically for the entire test session.
    """
    # Create test directories
    os.makedirs("data_test/schemas", exist_ok=True)
    os.makedirs("data_test/test_data", exist_ok=True)

    # Yield control to the test session
    yield

    # Teardown: remove test directories after session is complete
    shutil.rmtree("data_test", ignore_errors=True) 