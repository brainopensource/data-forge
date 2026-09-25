import os
import tempfile

import pytest

# Must be set before app import: config paths are resolved at import time.
os.environ["DATAFORGE_DATA_DIR"] = tempfile.mkdtemp(prefix="dataforge_test_")

from fastapi.testclient import TestClient  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c
