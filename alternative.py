"""
High-Performance Data Processing API with Clean Architecture

Architecture Features:
- Domain-Driven Design (DDD) with clear separation of concerns
- Hexagonal Architecture with ports and adapters
- SOLID principles for maintainability and extensibility  
- State-of-the-art performance optimizations
- Dependency injection for testability
- Strategic performance-focused modularization

"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Protocol, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import time
import os
from datetime import datetime
import asyncio
from functools import lru_cache, wraps
import logging

# High-performance imports
import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import duckdb

# FastAPI imports
from fastapi import FastAPI, HTTPException, Path, Body, Query, Depends
from fastapi.responses import Response
from pydantic import BaseModel, Field, validator

# ============================================================================
# DOMAIN LAYER - CORE BUSINESS LOGIC
# ============================================================================

class DataType(str, Enum):
    """Domain data types."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"


@dataclass(frozen=True)
class SchemaProperty:
    """Immutable schema property value object."""
    name: str
    data_type: DataType
    required: bool = False
    primary_key: bool = False
    
    def __post_init__(self):
        if not self.name or not self.name.strip():
            raise ValueError("Property name cannot be empty")


@dataclass(frozen=True)
class Schema:
    """Immutable schema aggregate root."""
    name: str
    properties: tuple[SchemaProperty, ...]
    version: str = "1.0"
    
    def __post_init__(self):
        if not self.name or not self.name.strip():
            raise ValueError("Schema name cannot be empty")
        if not self.properties:
            raise ValueError("Schema must have at least one property")
        
        # Ensure property names are unique
        names = [prop.name for prop in self.properties]
        if len(names) != len(set(names)):
            raise ValueError("Property names must be unique")
    
    @property
    def required_properties(self) -> tuple[SchemaProperty, ...]:
        """Get required properties."""
        return tuple(prop for prop in self.properties if prop.required)
    
    @property
    def primary_key_properties(self) -> tuple[SchemaProperty, ...]:
        """Get primary key properties."""
        return tuple(prop for prop in self.properties if prop.primary_key)


@dataclass
class WriteOperation:
    """Domain entity representing a write operation."""
    schema_name: str
    data: List[Dict[str, Any]]
    validation_mode: str = "vectorized"
    compression: str = "snappy"
    format: str = "parquet"
    
    def __post_init__(self):
        if not self.data:
            raise ValueError("Write operation must contain data")
        if self.validation_mode not in ["none", "vectorized", "full"]:
            raise ValueError("Invalid validation mode")


@dataclass
class WriteResult:
    """Domain entity representing write operation result."""
    success: bool
    records_written: int
    file_path: str
    write_time_seconds: float
    throughput_records_per_second: int
    file_size_mb: float
    validation_errors: Optional[List[str]] = None
    
    @property
    def message(self) -> str:
        return f"Processed {self.records_written:,} records at {self.throughput_records_per_second:,} records/sec"


# Domain Services
class ValidationService:
    """Domain service for data validation."""
    
    @staticmethod
    def validate_vectorized(schema: Schema, data: List[Dict[str, Any]]) -> List[str]:
        """Fast vectorized validation using Polars."""
        if not data:
            return ["No data provided"]
        
        errors = []
        
        # Convert to Polars DataFrame for fast validation
        try:
            df = pl.DataFrame(data, infer_schema_length=50)
            
            # Check required fields
            for prop in schema.required_properties:
                if prop.name not in df.columns:
                    errors.append(f"Required field '{prop.name}' is missing")
                    continue
                
                null_count = df.select(pl.col(prop.name).is_null().sum()).item()
                if null_count > 0:
                    errors.append(f"Required field '{prop.name}' has {null_count} null values")
            
            return errors
            
        except Exception as e:
            return [f"Validation error: {str(e)}"]
    
    @staticmethod
    def validate_full(schema: Schema, data: List[Dict[str, Any]]) -> List[str]:
        """Full validation with type checking."""
        errors = []
        
        for i, record in enumerate(data):
            # Check required fields
            for prop in schema.required_properties:
                if prop.name not in record or record[prop.name] is None:
                    errors.append(f"Record {i}: Required field '{prop.name}' is missing")
            
            # Type validation
            for prop in schema.properties:
                if prop.name in record and record[prop.name] is not None:
                    value = record[prop.name]
                    if not ValidationService._is_valid_type(value, prop.data_type):
                        errors.append(f"Record {i}: Field '{prop.name}' has invalid type")
            
            # Limit errors for performance
            if len(errors) > 100:
                errors.append(f"... and more validation errors (showing first 100)")
                break
        
        return errors
    
    @staticmethod
    def _is_valid_type(value: Any, data_type: DataType) -> bool:
        """Check if value matches expected data type."""
        if data_type == DataType.STRING:
            return isinstance(value, str)
        elif data_type == DataType.INTEGER:
            return isinstance(value, int)
        elif data_type == DataType.FLOAT:
            return isinstance(value, (int, float))
        elif data_type == DataType.BOOLEAN:
            return isinstance(value, bool)
        elif data_type == DataType.DATETIME:
            return isinstance(value, str) or isinstance(value, datetime)
        return True


# ============================================================================
# APPLICATION LAYER - USE CASES AND APPLICATION SERVICES
# ============================================================================

class SchemaRepository(Protocol):
    """Port for schema repository."""
    
    def get_schema(self, name: str) -> Optional[Schema]:
        ...
    
    def get_all_schemas(self) -> List[Schema]:
        ...


class DataWriter(Protocol):
    """Port for data writing."""
    
    async def write_parquet(self, operation: WriteOperation, file_path: str) -> WriteResult:
        ...
    
    async def write_feather(self, operation: WriteOperation, file_path: str) -> WriteResult:
        ...


class DataReader(Protocol):
    """Port for data reading."""
    
    async def read_as_arrow(self, file_path: str) -> pa.Table:
        ...


class FilePathGenerator(Protocol):
    """Port for file path generation."""
    
    def generate_write_path(self, schema_name: str, format: str, suffix: str = "") -> str:
        ...


# Application Services
class WriteUseCase:
    """Use case for writing data with performance optimization."""
    
    def __init__(
        self,
        schema_repo: SchemaRepository,
        data_writer: DataWriter,
        path_generator: FilePathGenerator
    ):
        self._schema_repo = schema_repo
        self._data_writer = data_writer
        self._path_generator = path_generator
    
    async def execute(self, operation: WriteOperation) -> WriteResult:
        """Execute write operation with optimal performance."""
        # Validate schema exists
        schema = self._schema_repo.get_schema(operation.schema_name)
        if not schema:
            raise ValueError(f"Schema '{operation.schema_name}' not found")
        
        # Validate data if requested
        validation_errors = []
        if operation.validation_mode == "vectorized":
            validation_errors = ValidationService.validate_vectorized(schema, operation.data)
        elif operation.validation_mode == "full":
            validation_errors = ValidationService.validate_full(schema, operation.data)
        
        if validation_errors and operation.validation_mode != "none":
            return WriteResult(
                success=False,
                records_written=0,
                file_path="",
                write_time_seconds=0.0,
                throughput_records_per_second=0,
                file_size_mb=0.0,
                validation_errors=validation_errors[:10]  # Limit for performance
            )
        
        # Generate file path
        file_path = self._path_generator.generate_write_path(
            operation.schema_name,
            operation.format,
            f"_{operation.validation_mode}"
        )
        
        # Execute write operation
        if operation.format == "parquet":
            result = await self._data_writer.write_parquet(operation, file_path)
        elif operation.format == "feather":
            result = await self._data_writer.write_feather(operation, file_path)
        else:
            raise ValueError(f"Unsupported format: {operation.format}")
        
        return result


class ReadUseCase:
    """Use case for reading data with performance optimization."""
    
    def __init__(
        self,
        schema_repo: SchemaRepository,
        data_reader: DataReader,
        path_generator: FilePathGenerator
    ):
        self._schema_repo = schema_repo
        self._data_reader = data_reader
        self._path_generator = path_generator
    
    async def execute(self, schema_name: str) -> pa.Table:
        """Execute read operation."""
        schema = self._schema_repo.get_schema(schema_name)
        if not schema:
            raise ValueError(f"Schema '{schema_name}' not found")
        
        # Generate file path for reading
        file_path = self._path_generator.generate_write_path(schema_name, "parquet")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Data file not found for schema '{schema_name}'")
        
        return await self._data_reader.read_as_arrow(file_path)


# ============================================================================
# INFRASTRUCTURE LAYER - ADAPTERS AND EXTERNAL CONCERNS
# ============================================================================

class InMemorySchemaRepository:
    """In-memory schema repository adapter."""
    
    def __init__(self):
        self._schemas: Dict[str, Schema] = {}
        self._load_default_schemas()
    
    def get_schema(self, name: str) -> Optional[Schema]:
        return self._schemas.get(name)
    
    def get_all_schemas(self) -> List[Schema]:
        return list(self._schemas.values())
    
    def add_schema(self, schema: Schema) -> None:
        self._schemas[schema.name] = schema
    
    def _load_default_schemas(self) -> None:
        """Load default schemas for demonstration."""
        # Well production schema
        well_production_schema = Schema(
            name="well_production",
            properties=(
                SchemaProperty("api", DataType.STRING, required=True, primary_key=True),
                SchemaProperty("date", DataType.DATETIME, required=True),
                SchemaProperty("oil", DataType.FLOAT, required=True),
                SchemaProperty("gas", DataType.FLOAT, required=True),
                SchemaProperty("water", DataType.FLOAT, required=True),
            )
        )
        self.add_schema(well_production_schema)


class HighPerformanceDataWriter:
    """High-performance data writer adapter."""
    
    # Performance constants
    OPTIMAL_ROW_GROUP_SIZE = 100000
    MINIMAL_INFER_LENGTH = 50
    
    async def write_parquet(self, operation: WriteOperation, file_path: str) -> WriteResult:
        """Ultra-fast Parquet write with Polars."""
        start_time = time.time()
        
        try:
            # Create directory if needed
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Convert to Polars DataFrame with minimal inference
            df = pl.DataFrame(operation.data, infer_schema_length=self.MINIMAL_INFER_LENGTH)
            
            # Optimal write settings for maximum performance
            write_options = {
                "compression": operation.compression,
                "row_group_size": min(self.OPTIMAL_ROW_GROUP_SIZE, len(operation.data)),
                "use_pyarrow": True,
                "statistics": False,  # Skip statistics for speed
            }
            
            # Write with optimizations
            df.write_parquet(file_path, **write_options)
            
            # Calculate metrics
            end_time = time.time()
            write_time = end_time - start_time
            records_written = len(operation.data)
            throughput = int(records_written / write_time) if write_time > 0 else 0
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
            
            return WriteResult(
                success=True,
                records_written=records_written,
                file_path=file_path,
                write_time_seconds=round(write_time, 3),
                throughput_records_per_second=throughput,
                file_size_mb=round(file_size, 2)
            )
            
        except Exception as e:
            logging.error(f"Parquet write error: {e}")
            raise RuntimeError(f"Failed to write Parquet file: {str(e)}")
    
    async def write_feather(self, operation: WriteOperation, file_path: str) -> WriteResult:
        """Ultra-fast Feather/Arrow write."""
        start_time = time.time()
        
        try:
            # Create directory if needed
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Convert to Arrow table for maximum performance
            arrow_table = pa.Table.from_pylist(operation.data)
            
            # Streaming write for memory efficiency
            batch_size = min(50000, len(operation.data))
            records_written = len(operation.data)
            
            with pa.OSFile(file_path, 'wb') as sink:
                with pa.ipc.new_file(sink, arrow_table.schema) as writer:
                    for i in range(0, records_written, batch_size):
                        batch_table = arrow_table.slice(i, min(batch_size, records_written - i))
                        writer.write_table(batch_table)
            
            # Calculate metrics
            end_time = time.time()
            write_time = end_time - start_time
            throughput = int(records_written / write_time) if write_time > 0 else 0
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
            
            return WriteResult(
                success=True,
                records_written=records_written,
                file_path=file_path,
                write_time_seconds=round(write_time, 3),
                throughput_records_per_second=throughput,
                file_size_mb=round(file_size, 2)
            )
            
        except Exception as e:
            logging.error(f"Feather write error: {e}")
            raise RuntimeError(f"Failed to write Feather file: {str(e)}")


class HighPerformanceDataReader:
    """High-performance data reader adapter."""
    
    async def read_as_arrow(self, file_path: str) -> pa.Table:
        """Ultra-fast read using DuckDB or PyArrow."""
        try:
            if file_path.endswith('.parquet'):
                # Use DuckDB for optimal Parquet reading
                conn = duckdb.connect()
                return conn.execute(f"SELECT * FROM read_parquet('{file_path}')").fetch_arrow_table()
            elif file_path.endswith('.feather'):
                # Use PyArrow for Feather files
                return pa.ipc.open_file(file_path).read_all()
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
                
        except Exception as e:
            logging.error(f"Read error: {e}")
            raise RuntimeError(f"Failed to read file: {str(e)}")


class OptimizedFilePathGenerator:
    """Optimized file path generator adapter."""
    
    def __init__(self, base_data_dir: str = "data"):
        self.base_data_dir = base_data_dir
    
    def generate_write_path(self, schema_name: str, format: str, suffix: str = "") -> str:
        """Generate optimized file path."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{schema_name}_write_{timestamp}{suffix}.{format}"
        
        # Create schema-specific directory
        schema_dir = os.path.join(self.base_data_dir, schema_name)
        os.makedirs(schema_dir, exist_ok=True)
        
        return os.path.join(schema_dir, filename)


# ============================================================================
# DEPENDENCY INJECTION CONTAINER
# ============================================================================

class DIContainer:
    """Dependency injection container following SOLID principles."""
    
    def __init__(self):
        self._schema_repo = InMemorySchemaRepository()
        self._data_writer = HighPerformanceDataWriter()
        self._data_reader = HighPerformanceDataReader()
        self._path_generator = OptimizedFilePathGenerator()
        
        # Configure performance optimizations
        self._configure_performance()
    
    def _configure_performance(self) -> None:
        """Configure global performance optimizations."""
        # Configure Arrow memory pool
        pa.set_memory_pool(pa.system_memory_pool())
        
        # Configure Polars for performance
        pl.Config.set_tbl_rows(100)
        pl.Config.set_fmt_str_lengths(50)
        
        # Configure DuckDB
        try:
            conn = duckdb.connect(":memory:")
            conn.execute("SET threads=8")
            conn.execute("SET memory_limit='8GB'")
            conn.close()
        except Exception as e:
            logging.warning(f"DuckDB optimization failed: {e}")
    
    @property
    def schema_repository(self) -> SchemaRepository:
        return self._schema_repo
    
    @property
    def data_writer(self) -> DataWriter:
        return self._data_writer
    
    @property
    def data_reader(self) -> DataReader:
        return self._data_reader
    
    @property
    def path_generator(self) -> FilePathGenerator:
        return self._path_generator
    
    def create_write_use_case(self) -> WriteUseCase:
        return WriteUseCase(
            self._schema_repo,
            self._data_writer,
            self._path_generator
        )
    
    def create_read_use_case(self) -> ReadUseCase:
        return ReadUseCase(
            self._schema_repo,
            self._data_reader,
            self._path_generator
        )


# ============================================================================
# PRESENTATION LAYER - API CONTROLLERS
# ============================================================================

# Request/Response models
class WriteRequest(BaseModel):
    """API request model for write operations."""
    data: List[Dict[str, Any]] = Field(..., description="Data to write")
    validation_mode: str = Field(default="vectorized", description="Validation mode: none, vectorized, full")
    compression: str = Field(default="snappy", description="Compression type")
    format: str = Field(default="parquet", description="Output format")
    
    @validator('data')
    def validate_data_not_empty(cls, v):
        if not v:
            raise ValueError("Data cannot be empty")
        return v
    
    @validator('validation_mode')
    def validate_validation_mode(cls, v):
        if v not in ["none", "vectorized", "full"]:
            raise ValueError("Validation mode must be: none, vectorized, or full")
        return v


class WriteResponse(BaseModel):
    """API response model for write operations."""
    success: bool
    message: str
    records_written: int
    file_path: str
    write_time_seconds: float
    throughput_records_per_second: int
    file_size_mb: float
    validation_errors: Optional[List[str]] = None


class ArrowResponse(Response):
    """Custom response for Arrow data."""
    media_type = "application/vnd.apache.arrow.stream"
    
    def __init__(self, table: pa.Table, **kwargs):
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        content = sink.getvalue().to_pybytes()
        super().__init__(content=content, media_type=self.media_type, **kwargs)


# Performance monitoring decorator
def monitor_performance(operation_name: str):
    """Decorator to monitor operation performance."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                logging.info(f"{operation_name} completed in {duration:.3f}s")
                return result
            except Exception as e:
                duration = time.time() - start_time
                logging.error(f"{operation_name} failed in {duration:.3f}s: {e}")
                raise
        return wrapper
    return decorator


# FastAPI Application
def create_app() -> FastAPI:
    """Create FastAPI application with dependency injection."""
    
    # Initialize DI container
    container = DIContainer()
    
    app = FastAPI(
        title="High-Performance Data Forge API",
        description="Clean Architecture API with state-of-the-art performance",
        version="2.0.0"
    )
    
    # Dependency providers
    def get_write_use_case() -> WriteUseCase:
        return container.create_write_use_case()
    
    def get_read_use_case() -> ReadUseCase:
        return container.create_read_use_case()
    
    def get_schema_repository() -> SchemaRepository:
        return container.schema_repository
    
    # API Endpoints
    @app.get("/")
    async def root():
        """Health check endpoint."""
        return {
            "message": "High-Performance Data Forge API",
            "architecture": "Clean Architecture with DDD & Hexagonal",
            "status": "operational"
        }
    
    @app.get("/schemas")
    async def list_schemas(
        schema_repo: SchemaRepository = Depends(get_schema_repository)
    ):
        """List all available schemas."""
        schemas = schema_repo.get_all_schemas()
        return {
            "schemas": [
                {
                    "name": schema.name,
                    "version": schema.version,
                    "properties_count": len(schema.properties),
                    "required_count": len(schema.required_properties),
                    "primary_key_count": len(schema.primary_key_properties)
                }
                for schema in schemas
            ]
        }
    
    @app.get("/schemas/{schema_name}")
    async def get_schema_details(
        schema_name: str = Path(..., description="Schema name"),
        schema_repo: SchemaRepository = Depends(get_schema_repository)
    ):
        """Get detailed schema information."""
        schema = schema_repo.get_schema(schema_name)
        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found")
        
        return {
            "name": schema.name,
            "version": schema.version,
            "properties": [
                {
                    "name": prop.name,
                    "type": prop.data_type.value,
                    "required": prop.required,
                    "primary_key": prop.primary_key
                }
                for prop in schema.properties
            ],
            "required_properties": [prop.name for prop in schema.required_properties],
            "primary_key_properties": [prop.name for prop in schema.primary_key_properties]
        }
    
    @app.post("/write/{schema_name}", response_model=WriteResponse)
    @monitor_performance("write_operation")
    async def write_data(
        schema_name: str = Path(..., description="Schema name"),
        request: WriteRequest = Body(...),
        write_use_case: WriteUseCase = Depends(get_write_use_case)
    ):
        """High-performance data write endpoint."""
        try:
            operation = WriteOperation(
                schema_name=schema_name,
                data=request.data,
                validation_mode=request.validation_mode,
                compression=request.compression,
                format=request.format
            )
            
            result = await write_use_case.execute(operation)
            
            return WriteResponse(
                success=result.success,
                message=result.message,
                records_written=result.records_written,
                file_path=result.file_path,
                write_time_seconds=result.write_time_seconds,
                throughput_records_per_second=result.throughput_records_per_second,
                file_size_mb=result.file_size_mb,
                validation_errors=result.validation_errors
            )
            
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logging.error(f"Write operation failed: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")
    
    @app.get("/read/{schema_name}")
    @monitor_performance("read_operation")
    async def read_data(
        schema_name: str = Path(..., description="Schema name"),
        read_use_case: ReadUseCase = Depends(get_read_use_case)
    ):
        """High-performance data read endpoint."""
        try:
            table = await read_use_case.execute(schema_name)
            return ArrowResponse(
                table,
                headers={"Content-Disposition": f"attachment; filename={schema_name}.arrow"}
            )
            
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logging.error(f"Read operation failed: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")
    
    @app.post("/benchmark/{schema_name}")
    @monitor_performance("benchmark_operation")
    async def benchmark_write_modes(
        schema_name: str = Path(..., description="Schema name"),
        request: WriteRequest = Body(...),
        write_use_case: WriteUseCase = Depends(get_write_use_case)
    ):
        """Benchmark different validation modes."""
        results = {}
        
        for validation_mode in ["none", "vectorized", "full"]:
            try:
                operation = WriteOperation(
                    schema_name=schema_name,
                    data=request.data,
                    validation_mode=validation_mode,
                    compression=request.compression,
                    format=request.format
                )
                
                result = await write_use_case.execute(operation)
                
                results[validation_mode] = {
                    "success": result.success,
                    "records_written": result.records_written,
                    "write_time_seconds": result.write_time_seconds,
                    "throughput_records_per_second": result.throughput_records_per_second,
                    "file_size_mb": result.file_size_mb,
                    "validation_errors_count": len(result.validation_errors) if result.validation_errors else 0
                }
                
            except Exception as e:
                results[validation_mode] = {
                    "success": False,
                    "error": str(e)
                }
        
        return {
            "schema_name": schema_name,
            "dataset_size": len(request.data),
            "benchmark_results": results,
            "fastest_mode": max(
                results.keys(),
                key=lambda mode: results[mode].get("throughput_records_per_second", 0)
            )
        }
    
    return app


# ============================================================================
# APPLICATION ENTRY POINT
# ============================================================================

# Create the application instance
app = create_app()

if __name__ == "__main__":
    import uvicorn
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Run the application
    uvicorn.run(
        "alternative:app",
        host="0.0.0.0",
        port=8080,
        reload=False,  # Disable for production performance
        workers=1,     # Single worker for optimal memory usage
        loop="uvloop", # High-performance event loop
        log_level="info"
    )
