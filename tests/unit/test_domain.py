import pytest
from app.domain.entities.schema import Schema, SchemaProperty
from app.domain.exceptions import InvalidDataException, SchemaNotFoundException, RecordNotFoundException, SchemaValidationException
from app.domain.repositories.schema_repository import ISchemaRepository

class DummySchemaRepository(ISchemaRepository):
    async def get_schema_by_name(self, name: str):
        return None
    async def get_all_schemas(self):
        return []

def test_schema_property_and_schema():
    prop = SchemaProperty(name="id", type="string", db_type="VARCHAR", required=True, primary_key=True)
    schema = Schema(
        name="TestSchema",
        description="A test schema",
        table_name="test_table",
        properties=[prop],
        primary_key=["id"]
    )
    assert schema.name == "TestSchema"
    assert schema.primary_key == ["id"]
    # Valid data
    schema.validate_data({"id": "abc"})
    # Missing required
    with pytest.raises(InvalidDataException):
        schema.validate_data({})
    # Wrong type
    with pytest.raises(InvalidDataException):
        schema.validate_data({"id": 123})
    # Composite key extraction
    assert schema.get_composite_key_from_data({"id": "abc"}) == {"id": "abc"}
    with pytest.raises(InvalidDataException):
        schema.get_composite_key_from_data({})

def test_domain_exceptions():
    with pytest.raises(SchemaNotFoundException):
        raise SchemaNotFoundException()
    with pytest.raises(RecordNotFoundException):
        raise RecordNotFoundException()
    with pytest.raises(InvalidDataException):
        raise InvalidDataException()
    with pytest.raises(SchemaValidationException):
        raise SchemaValidationException()

def test_schema_repository_interface():
    repo = DummySchemaRepository()
    import asyncio
    assert asyncio.run(repo.get_schema_by_name("any")) is None
    assert asyncio.run(repo.get_all_schemas()) == []

@pytest.mark.parametrize("type_,value,should_raise", [
    ("string", "abc", False),
    ("string", 123, True),
    ("integer", 123, False),
    ("integer", "abc", True),
    ("number", 1.5, False),
    ("number", 2, False),
    ("number", "abc", True),
    ("boolean", True, False),
    ("boolean", "true", True),
    ("array", [1,2], False),
    ("array", "notarray", True),
    ("object", {"a":1}, False),
    ("object", "notobject", True),
])
def test_schema_validate_data_types(type_, value, should_raise):
    prop = SchemaProperty(name="field", type=type_, db_type="DUMMY", required=True)
    schema = Schema(
        name="S", description="", table_name="T", properties=[prop], primary_key=None
    )
    if should_raise:
        with pytest.raises(InvalidDataException):
            schema.validate_data({"field": value})
    else:
        schema.validate_data({"field": value})

def test_schema_optional_and_required():
    prop_req = SchemaProperty(name="req", type="string", db_type="VARCHAR", required=True)
    prop_opt = SchemaProperty(name="opt", type="string", db_type="VARCHAR", required=False)
    schema = Schema(
        name="S", description="", table_name="T", properties=[prop_req, prop_opt], primary_key=None
    )
    # Missing required
    with pytest.raises(InvalidDataException):
        schema.validate_data({"opt": "x"})
    # Optional can be missing
    schema.validate_data({"req": "x"})

def test_schema_composite_key_missing_pk():
    prop1 = SchemaProperty(name="id1", type="string", db_type="VARCHAR", required=True, primary_key=True)
    prop2 = SchemaProperty(name="id2", type="string", db_type="VARCHAR", required=True, primary_key=True)
    schema = Schema(
        name="S", description="", table_name="T", properties=[prop1, prop2], primary_key=["id1", "id2"]
    )
    # Both present
    data = {"id1": "a", "id2": "b"}
    assert schema.get_composite_key_from_data(data) == data
    # One missing
    with pytest.raises(InvalidDataException):
        schema.get_composite_key_from_data({"id1": "a"}) 