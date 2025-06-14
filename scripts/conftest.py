import pytest
from fastapi.testclient import TestClient
from app.main import app
import pyarrow as pa
import pandas as pd
import asyncio
import factory
from app.domain.entities.schema import Schema, SchemaProperty
import os
import json

@pytest.fixture(scope="session")
def client():
    return TestClient(app)

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def sample_arrow_table():
    df = pd.DataFrame({"id": ["a", "b"], "value": [1, 2]})
    return pa.Table.from_pandas(df)

class SchemaPropertyFactory(factory.Factory):
    class Meta:
        model = SchemaProperty
    name = factory.Sequence(lambda n: f"field{n}")
    type = "string"
    db_type = "VARCHAR"
    required = False
    default = None
    primary_key = False

class SchemaFactory(factory.Factory):
    class Meta:
        model = Schema
    name = factory.Sequence(lambda n: f"Schema{n}")
    description = "A test schema"
    table_name = factory.Sequence(lambda n: f"table{n}")
    properties = factory.List([SchemaPropertyFactory()])
    primary_key = None

@pytest.fixture
def schema_factory():
    return SchemaFactory

@pytest.fixture
def test_data():
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    if os.path.exists(data_dir):
        for fname in os.listdir(data_dir):
            if fname.endswith(".json"):
                with open(os.path.join(data_dir, fname)) as f:
                    yield json.load(f)

@pytest.mark.slow
def test_slow_example():
    import time
    time.sleep(0.1)
    assert True 