"""Data record domain entity."""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime
import uuid


@dataclass
class DataRecord:
    """Domain entity representing a data record."""
    
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    data: Dict[str, Any] = field(default_factory=dict)
    schema_name: Optional[str] = None
    created_at: Optional[datetime] = field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    version: int = 1
    
    def get_value(self, column: str, default: Any = None) -> Any:
        """
        Get value for specified column.
        
        Args:
            column: Column name
            default: Default value if column not found
            
        Returns:
            Any: Column value or default
        """
        return self.data.get(column, default)
    
    def set_value(self, column: str, value: Any) -> None:
        """
        Set value for specified column.
        
        Args:
            column: Column name
            value: Value to set
        """
        self.data[column] = value
        self.updated_at = datetime.now()
        self.version += 1
    
    def has_column(self, column: str) -> bool:
        """
        Check if record has specified column.
        
        Args:
            column: Column name to check
            
        Returns:
            bool: True if column exists, False otherwise
        """
        return column in self.data
    
    def get_columns(self) -> List[str]:
        """
        Get list of all column names.
        
        Returns:
            List[str]: List of column names
        """
        return list(self.data.keys())
    
    def validate(self) -> bool:
        """
        Validate record data.
        
        Returns:
            bool: True if valid, False otherwise
        """
        return bool(self.id and isinstance(self.data, dict))
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert record to dictionary.
        
        Returns:
            Dict[str, Any]: Record as dictionary
        """
        return {
            'id': self.id,
            'data': self.data,
            'schema_name': self.schema_name,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'metadata': self.metadata,
            'version': self.version
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataRecord':
        """
        Create record from dictionary.
        
        Args:
            data: Dictionary containing record data
            
        Returns:
            DataRecord: New record instance
        """
        record = cls()
        record.id = data.get('id', record.id)
        record.data = data.get('data', {})
        record.schema_name = data.get('schema_name')
        record.metadata = data.get('metadata', {})
        record.version = data.get('version', 1)
        
        # Parse datetime strings
        if data.get('created_at'):
            try:
                record.created_at = datetime.fromisoformat(data['created_at'])
            except (ValueError, TypeError):
                pass
                
        if data.get('updated_at'):
            try:
                record.updated_at = datetime.fromisoformat(data['updated_at'])
            except (ValueError, TypeError):
                pass
                
        return record
    
    def copy(self) -> 'DataRecord':
        """
        Create a copy of the record.
        
        Returns:
            DataRecord: New record instance with same data
        """
        return DataRecord.from_dict(self.to_dict())
    
    def __str__(self) -> str:
        """String representation of the record."""
        return f"DataRecord(id={self.id}, columns={len(self.data)}, schema={self.schema_name})"
    
    def __repr__(self) -> str:
        """Detailed string representation of the record."""
        return (f"DataRecord(id='{self.id}', data={self.data}, "
                f"schema_name='{self.schema_name}', version={self.version})")
