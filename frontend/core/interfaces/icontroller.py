"""Controller interface definition."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class IController(ABC):
    """Base interface for all controllers in the application."""
    
    @abstractmethod
    async def initialize(self) -> bool:
        """
        Initialize the controller.
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        pass
    
    @abstractmethod
    def get_controller_type(self) -> str:
        """
        Get the controller type identifier.
        
        Returns:
            str: Controller type identifier
        """
        pass
    
    @abstractmethod
    def cleanup(self) -> None:
        """Clean up controller resources."""
        pass
    
    def get_dependencies(self) -> Dict[str, Any]:
        """
        Get controller dependencies.
        
        Returns:
            Dict[str, Any]: Dictionary of dependencies
        """
        return {}
    
    def is_initialized(self) -> bool:
        """
        Check if controller is initialized.
        
        Returns:
            bool: True if initialized, False otherwise
        """
        return True
