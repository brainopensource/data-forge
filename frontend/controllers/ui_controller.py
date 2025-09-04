# frontend/controllers/ui_controller.py
from typing import Optional, Any, Dict
import tkinter.messagebox as messagebox

from frontend.core.interfaces.icontroller import IController
from frontend.utils.error_handler import ErrorHandler


class UIController(IController):
    """Handles UI state management and user feedback"""
    
    def __init__(self, app_instance, error_handler: Optional[ErrorHandler] = None):
        self.app = app_instance
        self.status_text = "Ready"
        self.progress_value = 0.0
        self.status_widget = None
        self.progress_widget = None
        self._error_handler = error_handler
        self._initialized = False
        
    def set_status_widget(self, widget):
        """Set the status widget for status updates"""
        self.status_widget = widget
        
    def set_progress_widget(self, widget):
        """Set the progress widget for progress updates"""
        self.progress_widget = widget
        
    def update_status(self, message: str):
        """Update status message"""
        self.status_text = message
        
        # Update status widget if available
        if self.status_widget:
            try:
                if hasattr(self.status_widget, 'configure'):
                    self.status_widget.configure(text=message)
                elif hasattr(self.status_widget, 'set'):
                    self.status_widget.set(message)
            except Exception as e:
                print(f"Error updating status widget: {e}")
                
        # Fallback to app status method if available
        if hasattr(self.app, 'status'):
            try:
                self.app.status(message)
            except Exception as e:
                print(f"Error calling app status method: {e}")
                
    def update_progress(self, value: float, message: Optional[str] = None):
        """Update progress indicator (0.0 to 1.0)"""
        self.progress_value = max(0.0, min(1.0, value))
        
        # Update progress widget if available
        if self.progress_widget:
            try:
                if hasattr(self.progress_widget, 'set'):
                    self.progress_widget.set(self.progress_value)
            except Exception as e:
                print(f"Error updating progress widget: {e}")
                
        # Update status message if provided
        if message:
            self.update_status(message)
            
        # Fallback to app progress method if available
        if hasattr(self.app, 'progress'):
            try:
                self.app.progress(self.progress_value)
            except Exception as e:
                print(f"Error calling app progress method: {e}")
                
    def show_error(self, message: str, title: str = "Error"):
        """Show error message to user"""
        self.update_status(f"ERROR: {message}")
        
        try:
            messagebox.showerror(title, message)
        except Exception:
            # Fallback to console output
            print(f"ERROR: {message}")
            
    def show_warning(self, message: str, title: str = "Warning"):
        """Show warning message to user"""
        self.update_status(f"WARNING: {message}")
        
        try:
            messagebox.showwarning(title, message)
        except Exception:
            print(f"WARNING: {message}")
            
    def show_info(self, message: str, title: str = "Information"):
        """Show information message to user"""
        try:
            messagebox.showinfo(title, message)
        except Exception:
            print(f"INFO: {message}")
            
    def show_success(self, message: str):
        """Show success message"""
        self.update_status(f"SUCCESS: {message}")
        
    def confirm_action(self, message: str, title: str = "Confirm") -> bool:
        """Show confirmation dialog and return user choice"""
        try:
            return messagebox.askyesno(title, message)
        except Exception:
            print(f"CONFIRM: {message}")
            return False
            
    def reset_progress(self):
        """Reset progress to 0"""
        self.update_progress(0.0, "Ready")
        
    def get_status(self) -> str:
        """Get current status text"""
        return self.status_text
        
    def get_progress(self) -> float:
        """Get current progress value"""
        return self.progress_value
        
    def enable_widget(self, widget):
        """Enable a widget"""
        try:
            if hasattr(widget, 'configure'):
                widget.configure(state="normal")
        except Exception as e:
            print(f"Error enabling widget: {e}")
            
    def disable_widget(self, widget):
        """Disable a widget"""
        try:
            if hasattr(widget, 'configure'):
                widget.configure(state="disabled")
        except Exception as e:
            print(f"Error disabling widget: {e}")
    
    # IController interface implementation
    async def initialize(self) -> bool:
        """
        Initialize the controller.
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            # Initialize UI controller - nothing complex needed
            self._initialized = True
            return True
        except Exception as e:
            if self._error_handler:
                self._error_handler.handle_error(e, "Failed to initialize UI controller")
            return False
    
    def get_controller_type(self) -> str:
        """
        Get the controller type identifier.
        
        Returns:
            str: Controller type identifier
        """
        return "ui"
    
    def cleanup(self) -> None:
        """Clean up controller resources."""
        try:
            self.status_widget = None
            self.progress_widget = None
        except Exception as e:
            if self._error_handler:
                self._error_handler.handle_error(e, "Error during UI controller cleanup")
    
    def get_dependencies(self) -> Dict[str, Any]:
        """
        Get controller dependencies.
        
        Returns:
            Dict[str, Any]: Dictionary of dependencies
        """
        return {
            'error_handler': self._error_handler,
            'app_instance': self.app
        }
    
    def is_initialized(self) -> bool:
        """
        Check if controller is initialized.
        
        Returns:
            bool: True if initialized, False otherwise
        """
        return self._initialized
