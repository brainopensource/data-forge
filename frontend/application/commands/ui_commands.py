"""
UI-related commands for navigation, state changes, and user interface operations.
"""
from dataclasses import dataclass
from typing import Any, Dict, Optional, Callable
from frontend.application.commands.base import BaseCommand


@dataclass
class NavigateToTabCommand(BaseCommand):
    """Command to navigate to a specific tab."""
    tab_id: str
    skip_history: bool = False
    context_data: Optional[Dict[str, Any]] = None
    
    def validate(self) -> bool:
        return bool(self.tab_id and isinstance(self.tab_id, str))
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'tab_id': self.tab_id,
            'skip_history': self.skip_history,
            'context_data': self.context_data
        })
        return base


@dataclass
class UpdateButtonStateCommand(BaseCommand):
    """Command to update button state (enabled/disabled, style variant)."""
    button_id: str
    enabled: Optional[bool] = None
    variant: Optional[str] = None
    text: Optional[str] = None
    
    def validate(self) -> bool:
        return bool(self.button_id)
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'button_id': self.button_id,
            'enabled': self.enabled,
            'variant': self.variant,
            'text': self.text
        })
        return base


@dataclass
class ShowDialogCommand(BaseCommand):
    """Command to show a dialog or modal."""
    dialog_type: str
    title: str
    message: str
    buttons: Optional[list[str]] = None
    callback: Optional[Callable] = None
    
    def __post_init__(self):
        if self.buttons is None:
            self.buttons = ["OK"]
        super().__post_init__()
    
    def validate(self) -> bool:
        return bool(self.dialog_type and self.title and self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'dialog_type': self.dialog_type,
            'title': self.title,
            'message': self.message,
            'buttons': self.buttons
        })
        return base


@dataclass
class UpdateProgressCommand(BaseCommand):
    """Command to update progress indication."""
    value: float  # 0.0 to 1.0
    message: Optional[str] = None
    
    def validate(self) -> bool:
        return 0.0 <= self.value <= 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'value': self.value,
            'message': self.message
        })
        return base


@dataclass
class LogMessageCommand(BaseCommand):
    """Command to log a message."""
    message: str
    level: str = "INFO"  # INFO, WARNING, ERROR, DEBUG
    category: Optional[str] = None
    
    def validate(self) -> bool:
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        return bool(self.message) and self.level in valid_levels
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'message': self.message,
            'level': self.level,
            'category': self.category
        })
        return base


@dataclass
class RefreshDataCommand(BaseCommand):
    """Command to refresh data in a component."""
    component_id: str
    force_reload: bool = False
    filters: Optional[Dict[str, Any]] = None
    
    def validate(self) -> bool:
        return bool(self.component_id)
    
    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            'component_id': self.component_id,
            'force_reload': self.force_reload,
            'filters': self.filters
        })
        return base
