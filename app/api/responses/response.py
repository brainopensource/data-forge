"""
Shared API response types and path parameters.
"""
from typing import Annotated

import orjson
import pyarrow as pa
import pyarrow.ipc as ipc
from fastapi import Path
from fastapi.responses import Response

SchemaName = Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]+$", description="Schema name")]


class ArrowResponse(Response):
    """Arrow IPC stream response; serialized once and sent without an extra bytes copy."""
    media_type = "application/vnd.apache.arrow.stream"

    def __init__(self, table: pa.Table, filename: str = "data.arrow", **kwargs):
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        headers = {"Content-Disposition": f"attachment; filename={filename}", "Cache-Control": "no-cache"}
        super().__init__(content=memoryview(sink.getvalue()), headers=headers, **kwargs)


class FastJSONResponse(Response):
    """orjson-serialized JSON response."""
    media_type = "application/json"

    def render(self, content) -> bytes:
        return b"" if content is None else orjson.dumps(content)
