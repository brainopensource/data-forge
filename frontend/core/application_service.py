"""
Application Service implementing CQRS patterns for the DataForge frontend.

This service coordinates commands and queries, maintains application state,
and provides a clean separation between UI concerns and business logic.
"""
from typing import Dict, Any, Optional, TypeVar, Generic, Type
import asyncio
from frontend.application.commands.base import ICommand, ICommandHandler, CommandResult
from frontend.application.queries.base import IQuery, IQueryHandler, QueryResult
from frontend.application.handlers.ui_handlers import (
    NavigateToTabHandler,
    UpdateButtonStateHandler,
    LogMessageHandler,
    UpdateProgressHandler,
    RefreshDataHandler
)

T = TypeVar('T')


class ApplicationService:
    """
    Central application service implementing CQRS patterns.
    
    This service:
    - Registers and routes commands and queries to appropriate handlers
    - Maintains application state
    - Provides a unified interface for UI components
    - Implements the Command and Query Responsibility Segregation pattern
    """
    
    def __init__(self, app_instance):
        """
        Initialize the application service.
        
        Args:
            app_instance: The main application instance (DataForgeApp)
        """
        self.app = app_instance
        self._command_handlers: Dict[Type[ICommand], ICommandHandler] = {}
        self._query_handlers: Dict[Type[IQuery], IQueryHandler] = {}
        self._application_state: Dict[str, Any] = {}
        
        # Initialize default handlers
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """Register default command and query handlers."""
        # Register command handlers
        self.register_command_handler(NavigateToTabHandler(self.app))
        self.register_command_handler(UpdateButtonStateHandler(self.app))
        self.register_command_handler(LogMessageHandler(self.app))
        self.register_command_handler(UpdateProgressHandler(self.app))
        self.register_command_handler(RefreshDataHandler(self.app))
    
    def register_command_handler(self, handler: ICommandHandler):
        """
        Register a command handler.
        
        Args:
            handler: Command handler instance
        """
        # Extract the command type from the handler's generic type
        # This is a simplified approach; in practice, you might use more sophisticated type inspection
        command_type = self._extract_command_type(handler)
        if command_type:
            self._command_handlers[command_type] = handler
    
    def register_query_handler(self, handler: IQueryHandler):
        """
        Register a query handler.
        
        Args:
            handler: Query handler instance
        """
        # Extract the query type from the handler's generic type
        query_type = self._extract_query_type(handler)
        if query_type:
            self._query_handlers[query_type] = handler
    
    def _extract_command_type(self, handler: ICommandHandler) -> Optional[Type[ICommand]]:
        """Extract command type from handler - simplified implementation."""
        # In a real implementation, you'd use proper type introspection
        handler_name = handler.__class__.__name__
        if 'NavigateToTab' in handler_name:
            from frontend.application.commands.ui_commands import NavigateToTabCommand
            return NavigateToTabCommand
        elif 'UpdateButtonState' in handler_name:
            from frontend.application.commands.ui_commands import UpdateButtonStateCommand
            return UpdateButtonStateCommand
        elif 'LogMessage' in handler_name:
            from frontend.application.commands.ui_commands import LogMessageCommand
            return LogMessageCommand
        elif 'UpdateProgress' in handler_name:
            from frontend.application.commands.ui_commands import UpdateProgressCommand
            return UpdateProgressCommand
        elif 'RefreshData' in handler_name:
            from frontend.application.commands.ui_commands import RefreshDataCommand
            return RefreshDataCommand
        return None
    
    def _extract_query_type(self, handler: IQueryHandler) -> Optional[Type[IQuery]]:
        """Extract query type from handler - simplified implementation."""
        # Similar to command type extraction
        return None
    
    async def execute_command(self, command: ICommand) -> CommandResult:
        """
        Execute a command using the appropriate handler.
        
        Args:
            command: Command to execute
            
        Returns:
            CommandResult with execution status and data
        """
        command_type = type(command)
        handler = self._command_handlers.get(command_type)
        
        if not handler:
            return CommandResult(
                success=False,
                message=f"No handler registered for command type: {command_type.__name__}"
            )
        
        try:
            return await handler.handle(command)
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"Command execution failed: {str(e)}",
                error=e
            )
    
    async def execute_query(self, query: IQuery) -> QueryResult:
        """
        Execute a query using the appropriate handler.
        
        Args:
            query: Query to execute
            
        Returns:
            QueryResult with query results
        """
        query_type = type(query)
        handler = self._query_handlers.get(query_type)
        
        if not handler:
            return QueryResult(
                success=False,
                message=f"No handler registered for query type: {query_type.__name__}"
            )
        
        try:
            return await handler.handle(query)
        except Exception as e:
            return QueryResult(
                success=False,
                message=f"Query execution failed: {str(e)}",
                error=e
            )
    
    def execute_command_sync(self, command: ICommand) -> CommandResult:
        """
        Synchronous wrapper for command execution.
        
        Args:
            command: Command to execute
            
        Returns:
            CommandResult with execution status and data
        """
        # Create new event loop if none exists, or use existing one
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            # If loop is already running, we need to handle this differently
            # For now, we'll just return a placeholder result
            return CommandResult(
                success=False,
                message="Cannot execute async command in running event loop"
            )
        else:
            return loop.run_until_complete(self.execute_command(command))
    
    def execute_query_sync(self, query: IQuery) -> QueryResult:
        """
        Synchronous wrapper for query execution.
        
        Args:
            query: Query to execute
            
        Returns:
            QueryResult with query results
        """
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            return QueryResult(
                success=False,
                message="Cannot execute async query in running event loop"
            )
        else:
            return loop.run_until_complete(self.execute_query(query))
    
    def get_state(self, key: str, default: Any = None) -> Any:
        """
        Get application state value.
        
        Args:
            key: State key
            default: Default value if key not found
            
        Returns:
            State value or default
        """
        return self._application_state.get(key, default)
    
    def set_state(self, key: str, value: Any):
        """
        Set application state value.
        
        Args:
            key: State key
            value: Value to set
        """
        self._application_state[key] = value
    
    def update_state(self, updates: Dict[str, Any]):
        """
        Update multiple state values.
        
        Args:
            updates: Dictionary of key-value pairs to update
        """
        self._application_state.update(updates)
    
    def clear_state(self):
        """Clear all application state."""
        self._application_state.clear()


# Convenience functions for common operations
def create_application_service(app_instance) -> ApplicationService:
    """
    Factory function to create an ApplicationService.
    
    Args:
        app_instance: Main application instance
        
    Returns:
        Configured ApplicationService instance
    """
    return ApplicationService(app_instance)
