"""
Data-related queries for retrieving information from APIs and local storage.
"""
from dataclasses import dataclass
from typing import Any, Dict, Optional, List
from frontend.application.queries.base import BaseQuery


@dataclass
class GetSchemaListQuery(BaseQuery):
    """Query to retrieve list of available schemas."""
    include_versions: bool = False
    filter_pattern: Optional[str] = None
    
    def validate(self) -> bool:
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'include_versions': self.include_versions,
            'filter_pattern': self.filter_pattern
        })
        return base


@dataclass
class GetSchemaDetailsQuery(BaseQuery):
    """Query to retrieve details of a specific schema."""
    schema_name: str
    version: Optional[str] = None
    
    def validate(self) -> bool:
        return bool(self.schema_name)
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'schema_name': self.schema_name,
            'version': self.version
        })
        return base


@dataclass
class GetDataQuery(BaseQuery):
    """Query to retrieve data from a schema."""
    schema_name: str
    limit: Optional[int] = None
    offset: Optional[int] = None
    filters: Optional[Dict[str, Any]] = None
    columns: Optional[List[str]] = None
    
    def validate(self) -> bool:
        return bool(self.schema_name)
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'schema_name': self.schema_name,
            'limit': self.limit,
            'offset': self.offset,
            'filters': self.filters,
            'columns': self.columns
        })
        return base


@dataclass
class GetExplorationDataQuery(BaseQuery):
    """Query to retrieve data specifically for exploration components."""
    source_type: str  # 'current_schema', 'sample', 'csv_file'
    source_path: Optional[str] = None
    sample_size: Optional[int] = None
    
    def validate(self) -> bool:
        valid_sources = ['current_schema', 'sample', 'csv_file']
        if self.source_type not in valid_sources:
            return False
        if self.source_type == 'csv_file' and not self.source_path:
            return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'source_type': self.source_type,
            'source_path': self.source_path,
            'sample_size': self.sample_size
        })
        return base


@dataclass
class GetApplicationStateQuery(BaseQuery):
    """Query to retrieve current application state."""
    component_id: Optional[str] = None
    state_keys: Optional[List[str]] = None
    
    def validate(self) -> bool:
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'component_id': self.component_id,
            'state_keys': self.state_keys
        })
        return base


@dataclass
class GetLogHistoryQuery(BaseQuery):
    """Query to retrieve application log history."""
    level_filter: Optional[str] = None
    category_filter: Optional[str] = None
    limit: int = 100
    since: Optional[str] = None  # ISO timestamp
    
    def validate(self) -> bool:
        return self.limit > 0
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'level_filter': self.level_filter,
            'category_filter': self.category_filter,
            'limit': self.limit,
            'since': self.since
        })
        return base
