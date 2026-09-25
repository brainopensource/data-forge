import io
import uuid

import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

ARROW = "application/vnd.apache.arrow.stream"
ROWS = [{"id": i, "well": f"W{i}", "oil": 1.5 * i} for i in range(100)]


def schema_name() -> str:
    return f"t_{uuid.uuid4().hex[:8]}"


def arrow_body(rows) -> bytes:
    sink = io.BytesIO()
    pl.DataFrame(rows).write_ipc_stream(sink)
    return sink.getvalue()


def read(client, engine, name, **params) -> pa.Table:
    r = client.get(f"/read/{engine}/{name}", params=params)
    assert r.status_code == 200, r.text
    assert r.headers["content-type"] == ARROW
    return ipc.open_stream(r.content).read_all()


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_json_write_then_read_all_engines(client, engine):
    name = schema_name()
    r = client.post(f"/write/{engine}/{name}", json={"data": ROWS, "compression": "snappy"})
    assert r.status_code == 200, r.text
    assert r.json()["records_written"] == 100
    for reader in ("polars", "arrow", "duckdb"):
        table = read(client, reader, name)
        assert table.num_rows == 100
        assert table.column("oil").to_pylist()[3] == 4.5


def test_arrow_ipc_write(client):
    name = schema_name()
    r = client.post(f"/write/polars/{name}", content=arrow_body(ROWS), headers={"content-type": ARROW})
    assert r.status_code == 200, r.text
    assert read(client, "arrow", name).num_rows == 100


def test_writes_append_as_dataset(client):
    name = schema_name()
    for _ in range(3):
        assert client.post(f"/write/polars/{name}", json={"data": ROWS}).status_code == 200
    assert read(client, "polars", name).num_rows == 300


@pytest.mark.parametrize("engine", ["polars", "arrow", "duckdb"])
def test_projection_and_limit(client, engine):
    name = schema_name()
    client.post(f"/write/polars/{name}", json={"data": ROWS})
    table = read(client, engine, name, columns="id,oil", limit=10)
    assert table.column_names == ["id", "oil"]
    assert table.num_rows == 10


def test_missing_schema_is_404(client):
    assert client.get(f"/read/polars/{schema_name()}").status_code == 404


@pytest.mark.parametrize("payload", [{"data": []}, {"nope": 1}])
def test_empty_payload_is_400(client, payload):
    assert client.post(f"/write/polars/{schema_name()}", json=payload).status_code == 400


def test_bad_compression_is_400(client):
    r = client.post(f"/write/polars/{schema_name()}", json={"data": ROWS}, params={"compression": "x"})
    assert r.status_code == 400


@pytest.mark.parametrize("bad", ["..", "a.b", "a'b"])
def test_schema_name_is_validated(client, bad):
    assert client.get(f"/read/polars/{bad}").status_code in (404, 422)
    assert client.post(f"/write/polars/{bad}", json={"data": ROWS}).status_code in (404, 405, 422)


def test_unknown_engine_is_422(client):
    assert client.get(f"/read/pandas/{schema_name()}").status_code == 422


def test_invalid_schema_is_rejected_and_not_persisted(client):
    name = schema_name()
    assert client.post(f"/schemas/{name}", json={"properties": []}).status_code == 400
    assert client.get(f"/schemas/{name}").status_code == 404


def test_schema_register(client):
    name = schema_name()
    definition = {"description": "d", "table_name": name,
                  "properties": [{"name": "id", "type": "integer", "db_type": "BIGINT"}]}
    assert client.post(f"/schemas/{name}", json=definition).status_code == 201
    assert client.get(f"/schemas/{name}").json() == [1]


def test_health(client):
    r = client.get("/health/")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"
