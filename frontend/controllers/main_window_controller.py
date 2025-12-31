"""
Main Window Controller

Handles main window setup, management, and lifecycle events.
Separates window management logic from the main application class.
"""

from typing import Optional, Callable, Dict, Any
from pathlib import Path
import os

from frontend.core.interfaces.icontroller import IController
from frontend.utils.error_handler import ErrorHandler


class MainWindowController(IController):
    """
    Controller for main window management following SOLID principles.
    
    Responsibilities:
    - Window setup and configuration
    - Window positioning and sizing
    - Icon and title management
    - Window lifecycle events (close, minimize, maximize)
    - Error handling for window operations
    """
    
    def __init__(self, root, app_title: str = "DataForge - Data Exploration Tool", 
                 error_handler: Optional[ErrorHandler] = None):
        """
        Initialize the main window controller.
        
        Args:
            root: The root window instance (tkinter or customtkinter)
            app_title: Application title for the window
            error_handler: Optional error handler for dependency injection
        """
        self.root = root
        self.title = app_title
        self.default_geometry = "1400x900"
        self.min_width = 800
        self.min_height = 600
        self._close_callback: Optional[Callable] = None
        self._error_handler = error_handler
        self._initialized = False
        
    def setup_window(self, center: bool = True) -> None:
        """
        Setup main window properties and configuration.
        
        Args:
            center: Whether to center the window on screen
        """
        try:
            # Set basic window properties
            self.root.title(self.title)
            self.root.geometry(self.default_geometry)
            
            # Set minimum size constraints
            self.root.minsize(self.min_width, self.min_height)
            
            # Set up window close protocol
            self.root.protocol("WM_DELETE_WINDOW", self._handle_close_event)
            
            # Try to set window icon
            self._set_window_icon()
            
            # Center window if requested
            if center:
                self.center_window()
                
        except Exception as e:
            print(f"Warning: Error during window setup: {e}")
            # Continue with basic setup even if some features fail
            
    def center_window(self, width: Optional[int] = None, height: Optional[int] = None) -> None:
        """
        Center window on screen.
        
        Args:
            width: Custom width (uses current width if None)
            height: Custom height (uses current height if None)
        """
        try:
            # Get dimensions
            if width is None or height is None:
                self.root.update_idletasks()
                current_width = self.root.winfo_width()
                current_height = self.root.winfo_height()
                width = width or current_width
                height = height or current_height
                
            # Calculate center position
            screen_width = self.root.winfo_screenwidth()
            screen_height = self.root.winfo_screenheight()
            
            x = max(0, (screen_width - width) // 2)
            y = max(0, (screen_height - height) // 2)
            
            # Set geometry
            self.root.geometry(f"{width}x{height}+{x}+{y}")
            
        except Exception as e:
            print(f"Warning: Could not center window: {e}")
            
    def set_close_callback(self, callback: Callable) -> None:
        """
        Set a callback function to be called when the window is closing.
        
        Args:
            callback: Function to call before window closes
        """
        self._close_callback = callback
        
    def maximize_window(self) -> None:
        """Maximize the window to full screen."""
        try:
            self.root.state('zoomed')
        except Exception as e:
            print(f"Warning: Could not maximize window: {e}")
            
    def minimize_window(self) -> None:
        """Minimize the window to taskbar."""
        try:
            self.root.iconify()
        except Exception as e:
            print(f"Warning: Could not minimize window: {e}")
            
    def restore_window(self) -> None:
        """Restore window from minimized or maximized state."""
        try:
            self.root.state('normal')
        except Exception as e:
            print(f"Warning: Could not restore window: {e}")
            
    def set_title(self, title: str) -> None:
        """
        Update the window title.
        
        Args:
            title: New title for the window
        """
        try:
            self.title = title
            self.root.title(title)
        except Exception as e:
            print(f"Warning: Could not set window title: {e}")
            
    def update_title_with_status(self, status: str) -> None:
        """
        Update window title with status information.
        
        Args:
            status: Status text to append to title
        """
        status_title = f"{self.title} - {status}"
        self.set_title(status_title)
        
    def get_window_geometry(self) -> dict:
        """
        Get current window geometry information.
        
        Returns:
            Dictionary with window position and size information
        """
        try:
            self.root.update_idletasks()
            return {
                'width': self.root.winfo_width(),
                'height': self.root.winfo_height(),
                'x': self.root.winfo_x(),
                'y': self.root.winfo_y(),
                'geometry': self.root.geometry()
            }
        except Exception as e:
            print(f"Warning: Could not get window geometry: {e}")
            return {}
            
    def set_geometry(self, width: int, height: int, x: Optional[int] = None, y: Optional[int] = None) -> None:
        """
        Set window geometry.
        
        Args:
            width: Window width
            height: Window height
            x: X position (centers if None)
            y: Y position (centers if None)
        """
        try:
            if x is None or y is None:
                # Center the window
                self.center_window(width, height)
            else:
                self.root.geometry(f"{width}x{height}+{x}+{y}")
        except Exception as e:
            print(f"Warning: Could not set window geometry: {e}")
            
    def _handle_close_event(self) -> None:
        """Handle window closing event with proper cleanup."""
        try:
            # Call custom close callback if set
            if self._close_callback:
                should_close = self._close_callback()
                if should_close is False:
                    return  # Cancel close operation
                    
            # Perform cleanup and close
            self._cleanup()
            self.root.quit()
            
        except Exception as e:
            print(f"Error during window closing: {e}")
            # Force close if cleanup fails
            try:
                self.root.destroy()
            except Exception:
                pass
                
    def _cleanup(self) -> None:
        """Perform any necessary cleanup before window closes."""
        # This method can be overridden by subclasses for custom cleanup
        pass
        
    def _set_window_icon(self) -> None:
        """Try to set window icon from static assets."""
        try:
            # Look for icon in multiple possible locations
            possible_paths = [
                Path(__file__).parent.parent.parent / "static" / "images" / "favicon.ico",
                Path(__file__).parent.parent.parent / "static" / "favicon.ico",
                Path(__file__).parent.parent / "static" / "images" / "favicon.ico",
            ]
            
            for icon_path in possible_paths:
                if icon_path.exists():
                    self.root.iconbitmap(str(icon_path))
                    return
                    
            # If no icon found, try to create a simple default
            self._create_default_icon()
            
        except Exception as e:
            # Icon loading failed, continue without icon
            print(f"Note: Could not set window icon: {e}")
            
    def _create_default_icon(self) -> None:
        """Create a simple default icon if no icon file is found."""
        try:
            # This is a placeholder for creating a programmatic icon
            # Could use PIL or tkinter drawing if needed
            pass
        except Exception:
            pass
            
    def is_maximized(self) -> bool:
        """
        Check if window is currently maximized.
        
        Returns:
            True if window is maximized, False otherwise
        """
        try:
            return self.root.state() == 'zoomed'
        except Exception:
            return False
            
    def is_minimized(self) -> bool:
        """
        Check if window is currently minimized.
        
        Returns:
            True if window is minimized, False otherwise
        """
        try:
            return self.root.state() == 'iconic'
        except Exception:
            return False
            
    def bring_to_front(self) -> None:
        """Bring window to front and focus it."""
        try:
            self.root.lift()
            self.root.focus_force()
        except Exception as e:
            print(f"Warning: Could not bring window to front: {e}")
            
    def set_always_on_top(self, on_top: bool = True) -> None:
        """
        Set window to always stay on top.
        
        Args:
            on_top: True to keep window on top, False to allow normal behavior
        """
        try:
            self.root.attributes('-topmost', on_top)
        except Exception as e:
            print(f"Warning: Could not set always on top: {e}")
            
    def set_resizable(self, width_resizable: bool = True, height_resizable: bool = True) -> None:
        """
        Set whether window can be resized.
        
        Args:
            width_resizable: Allow width resizing
            height_resizable: Allow height resizing
        """
        try:
            self.root.resizable(width_resizable, height_resizable)
        except Exception as e:
            print(f"Warning: Could not set resizable properties: {e}")
    
    # IController interface implementation
    async def initialize(self) -> bool:
        """
        Initialize the controller.
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            self.setup_window()
            self._initialized = True
            return True
        except Exception as e:
            if self._error_handler:
                self._error_handler.handle_error(e, "Failed to initialize main window controller")
            return False
    
    def get_controller_type(self) -> str:
        """
        Get the controller type identifier.
        
        Returns:
            str: Controller type identifier
        """
        return "main_window"
    
    def cleanup(self) -> None:
        """Clean up controller resources."""
        try:
            if self.root and hasattr(self.root, 'destroy'):
                # Don't actually destroy - let the app handle that
                pass
        except Exception as e:
            if self._error_handler:
                self._error_handler.handle_error(e, "Error during main window cleanup")
    
    def get_dependencies(self) -> Dict[str, Any]:
        """
        Get controller dependencies.
        
        Returns:
            Dict[str, Any]: Dictionary of dependencies
        """
        return {
            'error_handler': self._error_handler,
            'root': self.root
        }
    
    def is_initialized(self) -> bool:
        """
        Check if controller is initialized.
        
        Returns:
            bool: True if initialized, False otherwise
        """
        return self._initialized
            
    def __str__(self) -> str:
        """String representation of the controller."""
        return f"MainWindowController(title='{self.title}', geometry='{self.default_geometry}')"
        
    def __repr__(self) -> str:
        """Detailed string representation of the controller."""
        return self.__str__()
