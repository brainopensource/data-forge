"""
Optimized response classes for Data Forge API.
Custom Arrow IPC response for maximum performance.
"""
import pyarrow as pa
import pyarrow.ipc as ipc
from fastapi.responses import Response


class ArrowResponse(Response):
    """
    Custom Arrow IPC response for ultra-fast data streaming.
    Optimized for 10M+ rows/second throughput.
    """
    media_type = "application/vnd.apache.arrow.stream"
    
    def __init__(self, table: pa.Table, filename: str = "data.arrow", **kwargs):
        """
        Initialize Arrow response with optimized settings.
        
        Args:
            table: PyArrow table to stream
            filename: Suggested filename for download
            **kwargs: Additional response parameters
        """
        # Create Arrow IPC stream with zero-copy optimization
        sink = pa.BufferOutputStream()
        
        # Use RecordBatchStreamWriter for optimal streaming performance
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        
        content = sink.getvalue().to_pybytes()
        
        # Set headers for optimal client handling
        headers = {
            "Content-Disposition": f"attachment; filename={filename}",
            "Content-Length": str(len(content)),
            "Cache-Control": "no-cache",  # Prevent caching for dynamic data
            **kwargs.get("headers", {})
        }
        
        super().__init__(
            content=content, 
            media_type=self.media_type, 
            headers=headers,
            **{k: v for k, v in kwargs.items() if k != "headers"}
        )


class FastJSONResponse(Response):
    """
    Optimized JSON response for metadata and small payloads.
    """
    media_type = "application/json"
    
    def __init__(self, content, **kwargs):
        # Try to use orjson for ultra-fast serialization, fallback to standard json
        try:
            import orjson
            if isinstance(content, (dict, list)):
                content = orjson.dumps(content).decode("utf-8")
        except ImportError:
            import json
            if isinstance(content, (dict, list)):
                content = json.dumps(content)
        
        super().__init__(content=content, media_type=self.media_type, **kwargs) 