"""View interface definition."""

from abc import ABC, abstractmethod
from typing import Any, Optional


class IView(ABC):
    """Base interface for all views in the application."""
    
    @abstractmethod
    def build_ui(self) -> Any:
        """
        Build the view's user interface.
        
        Returns:
            Any: The UI component/widget
        """
        pass
    
    @abstractmethod
    def refresh(self) -> None:
        """Refresh the view's content."""
        pass
    
    @abstractmethod
    def show(self) -> None:
        """Show the view."""
        pass
    
    @abstractmethod
    def hide(self) -> None:
        """Hide the view."""
        pass
    
    def is_visible(self) -> bool:
        """
        Check if view is visible.
        
        Returns:
            bool: True if visible, False otherwise
        """
        return True
    
    def get_widget(self) -> Optional[Any]:
        """
        Get the view's main widget.
        
        Returns:
            Optional[Any]: The main widget or None if not built
        """
        return None
    
    def update_data(self, data: Any) -> None:
        """
        Update view with new data.
        
        Args:
            data: New data to display
        """
        pass
