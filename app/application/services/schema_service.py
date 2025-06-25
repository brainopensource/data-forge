"""
Schema service for managing and loading schema configurations.
"""
from typing import Dict, List, Optional, Any
from app.domain.entities.schema import Schema, SchemaProperty, DataType
from app.infrastructure.persistence.metadata.schema_config import SCHEMAS_METADATA
import polars as pl
import pyarrow as pa
from pydantic import BaseModel, create_model, Field
from datetime import datetime


class SchemaService:
    """High-performance schema service with caching and dynamic model creation."""
    
    def __init__(self):
        self._schema_cache: Dict[str, Schema] = {}
        self._pydantic_models_cache: Dict[str, type] = {}
        self._load_schemas()
    
    def _load_schemas(self) -> None:
        """Load all schemas from configuration and cache them."""
        for schema_config in SCHEMAS_METADATA:
            schema_properties = []
            
            for prop_config in schema_config["properties"]:
                # Map string types to enum types
                data_type = self._map_type(prop_config["type"])
                
                schema_prop = SchemaProperty(
                    name=prop_config["name"],
                    type=data_type,
                    db_type=prop_config["db_type"],
                    required=prop_config.get("required", False),
                    primary_key=prop_config.get("primary_key", False),
                    default=prop_config.get("default"),
                    description=prop_config.get("description")
                )
                schema_properties.append(schema_prop)
            
            schema = Schema(
                name=schema_config["name"],
                description=schema_config["description"],
                table_name=schema_config["table_name"],
                primary_key=schema_config.get("primary_key", []),
                properties=schema_properties
            )
            
            self._schema_cache[schema.name] = schema
    
    def _map_type(self, type_str: str) -> DataType:
        """Map string type to DataType enum."""
        type_mapping = {
            "string": DataType.STRING,
            "integer": DataType.INTEGER,
            "number": DataType.NUMBER,
            "boolean": DataType.BOOLEAN,
            "datetime": DataType.DATETIME,
            "timestamp": DataType.TIMESTAMP
        }
        return type_mapping.get(type_str, DataType.STRING)
    
    def get_schema(self, schema_name: str) -> Optional[Schema]:
        """Get schema by name with caching."""
        return self._schema_cache.get(schema_name)
    
    def get_all_schemas(self) -> List[Schema]:
        """Get all available schemas."""
        return list(self._schema_cache.values())
    
    def get_schema_names(self) -> List[str]:
        """Get all available schema names."""
        return list(self._schema_cache.keys())
    
    def create_pydantic_model(self, schema_name: str) -> Optional[type]:
        """Create a dynamic Pydantic model from schema with caching."""
        if schema_name in self._pydantic_models_cache:
            return self._pydantic_models_cache[schema_name]
        
        schema = self.get_schema(schema_name)
        if not schema:
            return None
        
        # Build field definitions for Pydantic model
        field_definitions = {}
        for prop in schema.properties:
            python_type = self._get_python_type(prop)
            
            # Handle optional fields
            if not prop.required:
                python_type = Optional[python_type]
                default_value = prop.default if prop.default is not None else None
                field_definitions[prop.name] = (python_type, Field(default=default_value))
            else:
                field_definitions[prop.name] = (python_type, Field(...))
        
        # Create dynamic model
        model_class = create_model(
            f"{schema.name.title().replace('_', '')}Record",
            **field_definitions
        )
        
        # Cache the model
        self._pydantic_models_cache[schema_name] = model_class
        return model_class
    
    def _get_python_type(self, prop: SchemaProperty) -> type:
        """Get Python type from schema property."""
        if prop.type == DataType.STRING:
            return str
        elif prop.type == DataType.INTEGER:
            return int
        elif prop.type == DataType.NUMBER:
            return float
        elif prop.type == DataType.BOOLEAN:
            return bool
        elif prop.type in [DataType.DATETIME, DataType.TIMESTAMP]:
            return str  # We'll handle datetime parsing in the processing logic
        else:
            return str  # Default fallback
    
    def validate_data(self, schema_name: str, data: List[Dict[str, Any]]) -> tuple[bool, List[str]]:
        """Validate data against schema and return validation results."""
        schema = self.get_schema(schema_name)
        if not schema:
            return False, [f"Schema '{schema_name}' not found"]
        
        errors = []
        
        # Get dynamic model for validation
        model_class = self.create_pydantic_model(schema_name)
        if not model_class:
            return False, [f"Could not create validation model for schema '{schema_name}'"]
        
        # Validate each record
        for i, record in enumerate(data):
            try:
                model_class(**record)
            except Exception as e:
                errors.append(f"Record {i}: {str(e)}")
                if len(errors) > 100:  # Limit error messages for performance
                    errors.append(f"... and {len(data) - i - 1} more validation errors")
                    break
        
        return len(errors) == 0, errors
    
    def get_polars_schema(self, schema_name: str) -> Optional[Dict[str, Any]]:
        """Get Polars schema for the given schema name."""
        schema = self.get_schema(schema_name)
        return schema.to_polars_schema() if schema else None
    
    def get_arrow_schema(self, schema_name: str) -> Optional[pa.Schema]:
        """Get PyArrow schema for the given schema name."""
        schema = self.get_schema(schema_name)
        return schema.to_pyarrow_schema() if schema else None
    
    def preprocess_data_for_polars(self, schema_name: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Preprocess data for optimal Polars performance."""
        schema = self.get_schema(schema_name)
        if not schema:
            return data
        
        processed_data = []
        
        for record in data:
            processed_record = {}
            for prop in schema.properties:
                value = record.get(prop.name)
                
                # Handle None values
                if value is None:
                    if prop.required:
                        raise ValueError(f"Required field '{prop.name}' is missing")
                    processed_record[prop.name] = None
                    continue
                
                # Type-specific preprocessing
                if prop.type in [DataType.DATETIME, DataType.TIMESTAMP]:
                    # Keep as string - Polars will handle the conversion
                    processed_record[prop.name] = str(value)
                elif prop.type == DataType.INTEGER:
                    processed_record[prop.name] = int(value) if value is not None else None
                elif prop.type == DataType.NUMBER:
                    processed_record[prop.name] = float(value) if value is not None else None
                elif prop.type == DataType.BOOLEAN:
                    processed_record[prop.name] = bool(value) if value is not None else None
                else:
                    processed_record[prop.name] = str(value) if value is not None else None
            
            processed_data.append(processed_record)
        
        return processed_data
    
    def get_datetime_columns(self, schema_name: str) -> List[str]:
        """Get list of datetime/timestamp column names for a schema."""
        schema = self.get_schema(schema_name)
        if not schema:
            return []
        
        return [
            prop.name for prop in schema.properties 
            if prop.type in [DataType.DATETIME, DataType.TIMESTAMP]
        ]


# Global schema service instance
schema_service = SchemaService()
