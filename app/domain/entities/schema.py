"""
Schema domain entities for dynamic data validation and processing.
"""
from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, Field, validator
from enum import Enum
import polars as pl
import pyarrow as pa


class DataType(str, Enum):
    """Supported data types for schema properties."""
    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"


class SchemaProperty(BaseModel):
    """Schema property definition with validation rules."""
    name: str
    type: DataType
    db_type: str
    required: bool = False
    primary_key: bool = False
    default: Optional[Union[str, int, float, bool]] = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    description: Optional[str] = None


class Schema(BaseModel):
    """Complete schema definition for data validation and processing."""
    name: str
    description: str
    table_name: str
    primary_key: List[str] = Field(default_factory=list)
    properties: List[SchemaProperty]
    version: str = "1.0"
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @validator('properties')
    def validate_properties(cls, properties):
        """Ensure property names are unique and primary key fields exist."""
        names = [prop.name for prop in properties]
        if len(names) != len(set(names)):
            raise ValueError("Property names must be unique")
        return properties

    def get_property(self, name: str) -> Optional[SchemaProperty]:
        """Get a property by name."""
        for prop in self.properties:
            if prop.name == name:
                return prop
        return None

    def get_primary_key_properties(self) -> List[SchemaProperty]:
        """Get all properties that are part of the primary key."""
        return [prop for prop in self.properties if prop.primary_key]

    def get_required_properties(self) -> List[SchemaProperty]:
        """Get all required properties."""
        return [prop for prop in self.properties if prop.required]

    def to_polars_schema(self) -> Dict[str, Any]:
        """Convert schema to Polars schema format."""
        polars_schema = {}
        for prop in self.properties:
            if prop.type == DataType.STRING:
                polars_schema[prop.name] = pl.Utf8
            elif prop.type == DataType.INTEGER:
                polars_schema[prop.name] = pl.Int64
            elif prop.type == DataType.NUMBER:
                polars_schema[prop.name] = pl.Float64
            elif prop.type == DataType.BOOLEAN:
                polars_schema[prop.name] = pl.Boolean
            elif prop.type in [DataType.DATETIME, DataType.TIMESTAMP]:
                polars_schema[prop.name] = pl.Datetime
            else:
                polars_schema[prop.name] = pl.Utf8  # Default fallback
        return polars_schema

    def to_pyarrow_schema(self) -> pa.Schema:
        """Convert schema to PyArrow schema format."""
        fields = []
        for prop in self.properties:
            if prop.type == DataType.STRING:
                arrow_type = pa.string()
            elif prop.type == DataType.INTEGER:
                arrow_type = pa.int64()
            elif prop.type == DataType.NUMBER:
                arrow_type = pa.float64()
            elif prop.type == DataType.BOOLEAN:
                arrow_type = pa.bool_()
            elif prop.type in [DataType.DATETIME, DataType.TIMESTAMP]:
                arrow_type = pa.timestamp('us')
            else:
                arrow_type = pa.string()  # Default fallback
            
            fields.append(pa.field(prop.name, arrow_type, nullable=not prop.required))
        
        return pa.schema(fields)


class DynamicRecord(BaseModel):
    """Dynamic record model that validates against a schema."""
    data: Dict[str, Any]
    
    class Config:
        extra = "forbid"  # Prevent extra fields
