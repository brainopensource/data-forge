"""Repository interface definition."""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict


class IRepository(ABC):
    """Base interface for data repositories."""
    
    @abstractmethod
    async def get_by_id(self, id: str) -> Optional[Any]:
        """
        Get entity by ID.
        
        Args:
            id: Entity identifier
            
        Returns:
            Optional[Any]: Entity if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def save(self, entity: Any) -> bool:
        """
        Save entity.
        
        Args:
            entity: Entity to save
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        pass
    
    @abstractmethod
    async def delete(self, id: str) -> bool:
        """
        Delete entity by ID.
        
        Args:
            id: Entity identifier
            
        Returns:
            bool: True if deleted successfully, False otherwise
        """
        pass
    
    @abstractmethod
    async def get_all(self) -> List[Any]:
        """
        Get all entities.
        
        Returns:
            List[Any]: List of all entities
        """
        pass
    
    async def find_by_criteria(self, criteria: Dict[str, Any]) -> List[Any]:
        """
        Find entities by criteria.
        
        Args:
            criteria: Search criteria
            
        Returns:
            List[Any]: List of matching entities
        """
        return []
    
    async def count(self) -> int:
        """
        Get total count of entities.
        
        Returns:
            int: Total count
        """
        all_entities = await self.get_all()
        return len(all_entities)
