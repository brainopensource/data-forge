"""
Base command classes and interfaces for CQRS implementation.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, TypeVar, Generic
from dataclasses import dataclass, field
from datetime import datetime

T = TypeVar('T', bound='ICommand')


@dataclass
class CommandResult:
    """Result object for command execution."""
    success: bool
    message: str
    data: Optional[Any] = None
    error: Optional[Exception] = None
    timestamp: datetime = field(default_factory=datetime.now)


class ICommand(ABC):
    """Base interface for all commands."""
    
    @abstractmethod
    def validate(self) -> bool:
        """Validate command parameters."""
        pass
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert command to dictionary for logging/serialization."""
        pass


class ICommandHandler(Generic[T], ABC):
    """Base interface for command handlers."""
    
    @abstractmethod
    async def handle(self, command: T) -> CommandResult:
        """Handle the command execution."""
        pass


@dataclass
class BaseCommand(ICommand):
    """Base implementation for commands with common functionality."""
    
    def __post_init__(self):
        """Post-initialization validation."""
        if not self.validate():
            raise ValueError(f"Invalid command parameters: {self.to_dict()}")
    
    def validate(self) -> bool:
        """Default validation - can be overridden."""
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Default dictionary conversion."""
        return {
            'command_type': self.__class__.__name__,
            'timestamp': datetime.now().isoformat()
        }
