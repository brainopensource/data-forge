"""
Command handlers for UI-related operations.
"""
from typing import Any
from frontend.application.commands.base import CommandResult, ICommandHandler
from frontend.application.commands.ui_commands import (
    NavigateToTabCommand,
    UpdateButtonStateCommand,
    ShowDialogCommand,
    UpdateProgressCommand,
    LogMessageCommand,
    RefreshDataCommand
)


class NavigateToTabHandler(ICommandHandler[NavigateToTabCommand]):
    """Handler for tab navigation commands."""
    
    def __init__(self, app_instance):
        self.app = app_instance
    
    async def handle(self, command: NavigateToTabCommand) -> CommandResult:
        """Handle tab navigation."""
        try:
            # Call the app's navigation method
            self.app._show_tab(command.tab_id, command.skip_history)
            
            # If context data is provided, apply it
            if command.context_data:
                for key, value in command.context_data.items():
                    setattr(self.app, key, value)
            
            return CommandResult(
                success=True,
                message=f"Successfully navigated to tab: {command.tab_id}",
                data={'tab_id': command.tab_id}
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"Failed to navigate to tab: {command.tab_id}",
                error=e
            )


class UpdateButtonStateHandler(ICommandHandler[UpdateButtonStateCommand]):
    """Handler for button state update commands."""
    
    def __init__(self, app_instance):
        self.app = app_instance
    
    async def handle(self, command: UpdateButtonStateCommand) -> CommandResult:
        """Handle button state updates."""
        try:
            # Find the button by ID (this would need a button registry)
            button = self._find_button_by_id(command.button_id)
            
            if not button:
                return CommandResult(
                    success=False,
                    message=f"Button not found: {command.button_id}"
                )
            
            # Apply updates
            if command.enabled is not None:
                if command.enabled:
                    button.enable()
                else:
                    button.disable()
            
            if command.variant is not None:
                button.update_variant(command.variant)
            
            if command.text is not None:
                button.update_text(command.text)
            
            return CommandResult(
                success=True,
                message=f"Button {command.button_id} updated successfully",
                data={'button_id': command.button_id}
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"Failed to update button: {command.button_id}",
                error=e
            )
    
    def _find_button_by_id(self, button_id: str):
        """Find button by ID - would need implementation based on button registry."""
        # This would need to be implemented with a proper button registry
        return None


class LogMessageHandler(ICommandHandler[LogMessageCommand]):
    """Handler for log message commands."""
    
    def __init__(self, app_instance):
        self.app = app_instance
    
    async def handle(self, command: LogMessageCommand) -> CommandResult:
        """Handle log message creation."""
        try:
            # Format message with level and category
            formatted_message = command.message
            if command.category:
                formatted_message = f"[{command.category}] {formatted_message}"
            
            # Call the app's log method
            self.app.log(formatted_message)
            
            return CommandResult(
                success=True,
                message="Log message recorded",
                data={
                    'level': command.level,
                    'category': command.category,
                    'message': command.message
                }
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message="Failed to log message",
                error=e
            )


class UpdateProgressHandler(ICommandHandler[UpdateProgressCommand]):
    """Handler for progress update commands."""
    
    def __init__(self, app_instance):
        self.app = app_instance
    
    async def handle(self, command: UpdateProgressCommand) -> CommandResult:
        """Handle progress updates."""
        try:
            # Call the app's progress method
            self.app.progress(command.value)
            
            if command.message:
                self.app.status(command.message)
            
            return CommandResult(
                success=True,
                message="Progress updated",
                data={'value': command.value, 'message': command.message}
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message="Failed to update progress",
                error=e
            )


class RefreshDataHandler(ICommandHandler[RefreshDataCommand]):
    """Handler for data refresh commands."""
    
    def __init__(self, app_instance):
        self.app = app_instance
    
    async def handle(self, command: RefreshDataCommand) -> CommandResult:
        """Handle data refresh requests."""
        try:
            # This would need component-specific refresh logic
            component = self._find_component_by_id(command.component_id)
            
            if not component:
                return CommandResult(
                    success=False,
                    message=f"Component not found: {command.component_id}"
                )
            
            # Call component refresh method if it exists
            if hasattr(component, 'refresh'):
                component.refresh(force=command.force_reload, filters=command.filters)
            
            return CommandResult(
                success=True,
                message=f"Component {command.component_id} refreshed",
                data={'component_id': command.component_id}
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"Failed to refresh component: {command.component_id}",
                error=e
            )
    
    def _find_component_by_id(self, component_id: str):
        """Find component by ID - would need implementation based on component registry."""
        # This would need to be implemented with a proper component registry
        return None
