"""
Generic models for dynamic write operations.
"""
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, validator


class DynamicWriteRequest(BaseModel):
    """Generic request model for dynamic write operations with performance optimizations."""
    data: List[Dict[str, Any]]
    batch_size: int = Field(default=100000, ge=1000, le=1000000, description="Batch size for processing (1K-1M)")
    compression: Optional[str] = Field(default="snappy", description="Compression type: snappy, lz4, zstd, or None")
    append_mode: bool = Field(default=False, description="Whether to append to existing file or overwrite")
    validate_schema: bool = Field(default=True, description="Whether to validate data against schema")
    
    @validator('compression')
    def validate_compression(cls, v):
        """Validate compression parameter."""
        if v is None or v.lower() == 'none':
            return None
        allowed_compressions = ['snappy', 'lz4', 'zstd', 'gzip', 'brotli']
        if v.lower() not in allowed_compressions:
            raise ValueError(f"Compression must be one of: {allowed_compressions} or None")
        return v.lower()
    
    @validator('data')
    def validate_data_not_empty(cls, v):
        """Ensure data is not empty."""
        if not v:
            raise ValueError("Data cannot be empty")
        return v


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
