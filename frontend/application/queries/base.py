"""
Base query classes and interfaces for CQRS implementation.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, TypeVar, Generic
from dataclasses import dataclass, field
from datetime import datetime

T = TypeVar('T')


@dataclass
class QueryResult(Generic[T]):
    """Result object for query execution."""
    success: bool
    data: Optional[T] = None
    error: Optional[Exception] = None
    message: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Optional[Dict[str, Any]] = None


class IQuery(ABC):
    """Base interface for all queries."""
    
    @abstractmethod
    def validate(self) -> bool:
        """Validate query parameters."""
        pass
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert query to dictionary for logging/caching."""
        pass


class IQueryHandler(Generic[T], ABC):
    """Base interface for query handlers."""
    
    @abstractmethod
    async def handle(self, query: IQuery) -> QueryResult[T]:
        """Handle the query execution."""
        pass


@dataclass
class BaseQuery(IQuery):
    """Base implementation for queries with common functionality."""
    
    def __post_init__(self):
        """Post-initialization validation."""
        if not self.validate():
            raise ValueError(f"Invalid query parameters: {self.to_dict()}")
    
    def validate(self) -> bool:
        """Default validation - can be overridden."""
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Default dictionary conversion."""
        return {
            'query_type': self.__class__.__name__,
            'timestamp': datetime.now().isoformat()
        }
