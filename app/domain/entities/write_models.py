"""
Generic models for dynamic write operations.
"""
from typing import List, Dict, Any, Optional
from pydantic import BaseModel


class WriteResponse(BaseModel):
    """Response model for write operations with performance metrics."""
    success: bool
    message: str
    records_written: int
    schema_name: str
    file_path: str
    write_time_seconds: float
    throughput_records_per_second: int
    file_size_mb: float
    validation_errors: Optional[List[str]] = None
