"""
Base Tab Class for DataForge Application
"""
from typing import Any
from abc import ABC, abstractmethod

from frontend.services.ui_framework_adapter import UIFrameworkAdapter


class BaseTab(ABC):
    """Base class for application tabs"""
    
    def __init__(self, app):
        self.app = app
        self.ui_adapter = app.get_ui_adapter()
        self.content_frame = None
    
    @abstractmethod
    def build_content(self, parent) -> None:
        """Build the tab content in the given parent widget"""
        pass
    
    def _log(self, message: str):
        """Log message through the app"""
        self.app._log(message)
    
    def _status(self, message: str):
        """Show status message through the app"""
        self.app._status(message)
