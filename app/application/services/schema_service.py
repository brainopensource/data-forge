"""
Schema service for managing and loading schema configurations.
"""
from typing import Dict, List, Optional, Any
from app.domain.entities.schema import Schema, SchemaProperty, DataType
from app.infrastructure.persistence.repositories.schema_repository import FileSchemaRepository
from app.domain.exceptions.exceptions import SchemaNotFoundException
import polars as pl
import pyarrow as pa
from pydantic import BaseModel, create_model, Field
from datetime import datetime


class SchemaService:
    """High-performance schema service with versioning, caching, and dynamic model creation."""
    
    def __init__(self, repository: Optional[FileSchemaRepository] = None):
        self._repository = repository or FileSchemaRepository()
        self._schema_cache: Dict[str, Dict[int, Schema]] = {}
        self._pydantic_models_cache: Dict[str, Dict[int, type]] = {}
        self._load_schemas()
    
    def _load_schemas(self) -> None:
        """Load all schemas from the repository and cache them."""
        schema_names = self._repository.get_all_schema_names()
        for schema_name in schema_names:
            self._schema_cache[schema_name] = {}
            self._pydantic_models_cache[schema_name] = {}
            versions = self._repository.get_versions_for_schema(schema_name)
            for version in versions:
                try:
                    schema_definition = self._repository.load_definition(schema_name, version)
                    schema = self._build_schema_from_definition(schema_definition)
                    self._schema_cache[schema_name][version] = schema
                except SchemaNotFoundException:
                    logging.warning(
                        f"SchemaNotFoundException: Could not load schema '{schema_name}' version {version}."
                    )

    def _build_schema_from_definition(self, schema_definition: Dict[str, Any]) -> Schema:
        """Build a Schema object from its dictionary definition."""
        schema_properties = []
        for prop_config in schema_definition.get("properties", []):
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
        
        return Schema(
            name=schema_definition["name"],
            description=schema_definition.get("description"),
            table_name=schema_definition.get("table_name"),
            primary_key=schema_definition.get("primary_key", []),
            properties=schema_properties,
            version=schema_definition.get("version")
        )

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

    def list_schema_families(self) -> List[str]:
        """Get all available schema family names."""
        return list(self._schema_cache.keys())

    def list_schema_versions(self, schema_name: str) -> List[int]:
        """Lists all available versions for a schema, sorted."""
        if schema_name not in self._schema_cache:
            raise SchemaNotFoundException(f"Schema family '{schema_name}' not found.")
        versions = list(self._schema_cache[schema_name].keys())
        versions.sort()
        return versions

    def get_latest_schema_version(self, schema_name: str) -> int:
        """Gets the latest version number for a schema."""
        versions = self.list_schema_versions(schema_name)
        if not versions:
            raise SchemaNotFoundException(f"No versions found for schema family '{schema_name}'.")
        return max(versions)
    
    def get_schema(self, schema_name: str, version: Optional[int] = None) -> Schema:
        """Get schema by name and version. If version is None, gets the latest."""
        if schema_name not in self._schema_cache:
            raise SchemaNotFoundException(f"Schema family '{schema_name}' not found.")
        
        if version is None:
            version = self.get_latest_schema_version(schema_name)

        if version not in self._schema_cache[schema_name]:
            raise SchemaNotFoundException(f"Schema '{schema_name}' version {version} not found.")
            
        return self._schema_cache[schema_name][version]

    def register_new_schema_version(self, schema_name: str, schema_definition: Dict[str, Any]) -> Schema:
        """Registers a new version of a schema."""
        versions = self.list_schema_versions(schema_name) if schema_name in self._schema_cache else []
        next_version = max(versions) + 1 if versions else 1

        # Enrich and save the definition
        schema_definition['name'] = schema_name
        self._repository.save_definition(schema_name, next_version, schema_definition)
        
        # Build and cache the new schema object
        new_schema = self._build_schema_from_definition(schema_definition)
        if schema_name not in self._schema_cache:
            self._schema_cache[schema_name] = {}
        self._schema_cache[schema_name][next_version] = new_schema
        
        return new_schema

    def delete_schema_family(self, schema_name: str):
        """(Soft) Deletes an entire schema family by archiving it."""
        if schema_name not in self._schema_cache:
            raise SchemaNotFoundException(f"Schema family '{schema_name}' not found.")
        
        self._repository.archive_schema_family(schema_name)
        
        # Remove from caches
        if schema_name in self._schema_cache:
            del self._schema_cache[schema_name]
        if schema_name in self._pydantic_models_cache:
            del self._pydantic_models_cache[schema_name]
    
    def get_all_schemas(self) -> List[Schema]:
        """Get all available schemas (latest version of each)."""
        all_schemas = []
        for name in self.list_schema_families():
            try:
                latest_version = self.get_latest_schema_version(name)
                all_schemas.append(self.get_schema(name, latest_version))
            except SchemaNotFoundException:
                continue
        return all_schemas
    
    def create_pydantic_model(self, schema_name: str, version: Optional[int] = None) -> type:
        """Create a dynamic Pydantic model from a specific schema version."""
        schema = self.get_schema(schema_name, version) # Handles not found exceptions
        
        if version is None:
            version = schema.version

        if self._pydantic_models_cache.get(schema_name, {}).get(version):
            return self._pydantic_models_cache[schema_name][version]
        
        # Build field definitions for Pydantic model
        field_definitions = {}
        for prop in schema.properties:
            python_type = self._get_python_type(prop)
            
            if not prop.required:
                python_type = Optional[python_type]
                field_definitions[prop.name] = (python_type, Field(default=prop.default))
            else:
                field_definitions[prop.name] = (python_type, Field(...))
        
        model_class = create_model(
            f"{schema.name.title().replace('_', '')}V{schema.version}Record",
            **field_definitions
        )
        
        # Cache the model
        if schema_name not in self._pydantic_models_cache:
            self._pydantic_models_cache[schema_name] = {}
        self._pydantic_models_cache[schema_name][version] = model_class
        
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
            return str
        else:
            return str

    def get_arrow_schema(self, schema_name: str, version: Optional[int] = None) -> pa.Schema:
        """Get PyArrow schema for a given schema name and version."""
        schema = self.get_schema(schema_name, version)
        return schema.to_pyarrow_schema()

# Global schema service instance
schema_service = SchemaService()
