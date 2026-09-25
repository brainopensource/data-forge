"""
Navigation Controller

Handles tab navigation and routing within the application following SOLID principles.
Extracts navigation logic from the main application for better separation of concerns.
"""

from typing import Dict, List, Optional, Callable, Any
from enum import Enum


class NavigationState(Enum):
    """Enum for different navigation states"""
    MAIN_TAB = "main_tab"
    SUB_VIEW = "sub_view"
    MODAL = "modal"


class NavigationController:
    """
    Handles tab navigation and routing within the application.
    
    Responsibilities:
    - Tab switching and state management
    - Navigation history tracking
    - Back button functionality
    - Route validation and error handling
    - Navigation callbacks and hooks
    
    Follows SOLID principles:
    - Single Responsibility: Only handles navigation logic
    - Open/Closed: Extensible for new navigation patterns
    - Interface Segregation: Clean interface for navigation operations
    - Dependency Inversion: Accepts callbacks rather than depending on specific UI
    """
    
    def __init__(self, app_instance: Any):
        """
        Initialize the navigation controller.
        
        Args:
            app_instance: Reference to the main application instance
        """
        self.app = app_instance
        self.current_tab = "home"
        self.current_state = NavigationState.MAIN_TAB
        self.tab_history: List[str] = []
        self.max_history = 10
        
        # Registry for tab widgets and callbacks
        self.tab_widgets: Dict[str, Any] = {}
        self.tab_callbacks: Dict[str, Callable] = {}
        self.tab_builders: Dict[str, Callable] = {}
        self.tab_titles: Dict[str, str] = {}
        
        # Navigation state tracking
        self.view_stack: List[Dict[str, Any]] = []
        self.navigation_hooks: List[Callable] = []
        
        # Register default tabs
        self._register_default_tabs()
        
    def _register_default_tabs(self):
        """Register default application tabs with their titles."""
        default_tabs = {
            "home": "🏠 Home",
            "database": "🗄️ Database", 
            "external": "🌐 External Fetch",
            "sync": "🔄 Sync",
            "gateway": "⚡ Features",
            "exploration": "🔍 Exploration",
            "help": "❓ Help"
        }
        
        for tab_id, title in default_tabs.items():
            self.tab_titles[tab_id] = title
            
    def register_tab(self, tab_id: str, widget: Any = None, 
                    callback: Optional[Callable] = None,
                    builder: Optional[Callable] = None,
                    title: Optional[str] = None):
        """
        Register a tab with its widget, callback, and builder.
        
        Args:
            tab_id: Unique identifier for the tab
            widget: The widget container for the tab content
            callback: Optional callback to execute when tab is shown
            builder: Optional function to build/rebuild tab content
            title: Display title for the tab
        """
        if widget:
            self.tab_widgets[tab_id] = widget
        if callback:
            self.tab_callbacks[tab_id] = callback
        if builder:
            self.tab_builders[tab_id] = builder
        if title:
            self.tab_titles[tab_id] = title
            
    def switch_to_tab(self, tab_id: str, save_history: bool = True, 
                     force_rebuild: bool = False) -> bool:
        """
        Switch to a specific tab with proper state management.
        
        Args:
            tab_id: ID of the tab to switch to
            save_history: Whether to save current tab to history
            force_rebuild: Whether to force rebuilding of tab content
            
        Returns:
            True if switch was successful, False otherwise
        """
        try:
            # Validate tab exists
            if not self._is_valid_tab(tab_id):
                self._handle_navigation_error(f"Tab '{tab_id}' not registered")
                return False
                
            # Save current tab to history if requested and different
            if save_history and self.current_tab != tab_id:
                self._add_to_history(self.current_tab)
                
            # Hide current tab
            if not self._hide_current_tab():
                return False
                
            # Update state
            old_tab = self.current_tab
            self.current_tab = tab_id
            self.current_state = NavigationState.MAIN_TAB
            
            # Show new tab
            if not self._show_tab(tab_id, force_rebuild):
                # Revert state on failure
                self.current_tab = old_tab
                return False
                
            # Update navigation UI
            self._update_navigation_ui()
            
            # Execute navigation hooks
            self._execute_navigation_hooks(old_tab, tab_id)
            
            # Execute tab-specific callback
            self._execute_tab_callback(tab_id)
            
            return True
            
        except Exception as e:
            self._handle_navigation_error(f"Error switching to tab '{tab_id}': {e}")
            return False
    
    def go_back(self) -> bool:
        """
        Go back to the previous tab in history.
        
        Returns:
            True if successful, False if no history or error
        """
        try:
            if not self.tab_history:
                return False
                
            previous_tab = self.tab_history.pop()
            return self.switch_to_tab(previous_tab, save_history=False)
            
        except Exception as e:
            self._handle_navigation_error(f"Error going back: {e}")
            return False
            
    def push_sub_view(self, view_id: str, view_data: Optional[Dict[str, Any]] = None):
        """
        Push a sub-view onto the navigation stack.
        
        Args:
            view_id: Identifier for the sub-view
            view_data: Optional data associated with the view
        """
        view_info = {
            'view_id': view_id,
            'parent_tab': self.current_tab,
            'data': view_data or {},
            'timestamp': self._get_timestamp()
        }
        
        self.view_stack.append(view_info)
        self.current_state = NavigationState.SUB_VIEW
        
        # Show back button
        if hasattr(self.app, '_show_back_button'):
            self.app._show_back_button()
            
    def pop_sub_view(self) -> Optional[Dict[str, Any]]:
        """
        Pop the current sub-view from the stack.
        
        Returns:
            The popped view information or None if stack is empty
        """
        if not self.view_stack:
            return None
            
        view_info = self.view_stack.pop()
        
        # Update state
        if not self.view_stack:
            self.current_state = NavigationState.MAIN_TAB
            # Hide back button
            if hasattr(self.app, '_hide_back_button'):
                self.app._hide_back_button()
        
        return view_info
        
    def get_current_tab(self) -> str:
        """Get the current active tab ID."""
        return self.current_tab
        
    def get_current_state(self) -> NavigationState:
        """Get the current navigation state."""
        return self.current_state
        
    def get_tab_history(self) -> List[str]:
        """Get a copy of the navigation history."""
        return self.tab_history.copy()
        
    def get_current_view_stack(self) -> List[Dict[str, Any]]:
        """Get a copy of the current view stack."""
        return self.view_stack.copy()
        
    def clear_history(self):
        """Clear navigation history."""
        self.tab_history.clear()
        
    def clear_view_stack(self):
        """Clear the view stack."""
        self.view_stack.clear()
        self.current_state = NavigationState.MAIN_TAB
        
    def add_navigation_hook(self, hook: Callable):
        """
        Add a hook to be called on navigation changes.
        
        Args:
            hook: Function that accepts (old_tab, new_tab) parameters
        """
        if callable(hook):
            self.navigation_hooks.append(hook)
            
    def remove_navigation_hook(self, hook: Callable):
        """Remove a navigation hook."""
        if hook in self.navigation_hooks:
            self.navigation_hooks.remove(hook)
            
    def get_tab_title(self, tab_id: str) -> str:
        """Get the display title for a tab."""
        return self.tab_titles.get(tab_id, tab_id.title())
        
    def refresh_current_tab(self):
        """Refresh the current tab by rebuilding its content."""
        if self.current_tab:
            self.switch_to_tab(self.current_tab, save_history=False, force_rebuild=True)
            
    def is_main_tab_active(self) -> bool:
        """Check if a main tab is currently active (not in sub-view)."""
        return self.current_state == NavigationState.MAIN_TAB
        
    def can_go_back(self) -> bool:
        """Check if back navigation is possible."""
        return len(self.tab_history) > 0 or len(self.view_stack) > 0
        
    def handle_back_action(self):
        """Handle the back button action intelligently."""
        # First try to pop sub-view
        if self.view_stack:
            popped_view = self.pop_sub_view()
            if popped_view and hasattr(self.app, '_handle_back_from_subview'):
                self.app._handle_back_from_subview(popped_view)
            return True
            
        # Then try navigation history
        return self.go_back()
        
    def _is_valid_tab(self, tab_id: str) -> bool:
        """Check if a tab ID is valid/registered."""
        return (tab_id in self.tab_titles or 
                tab_id in self.tab_widgets or 
                tab_id in self.tab_builders)
                
    def _add_to_history(self, tab_id: str):
        """Add a tab to navigation history."""
        if tab_id and tab_id != self.current_tab:
            self.tab_history.append(tab_id)
            
            # Limit history size
            if len(self.tab_history) > self.max_history:
                self.tab_history.pop(0)
                
    def _hide_current_tab(self) -> bool:
        """Hide the currently active tab."""
        try:
            if self.current_tab in self.tab_widgets:
                widget = self.tab_widgets[self.current_tab]
                if hasattr(widget, 'pack_forget'):
                    widget.pack_forget()
                elif hasattr(widget, 'grid_forget'):
                    widget.grid_forget()
                elif hasattr(widget, 'place_forget'):
                    widget.place_forget()
            return True
        except Exception as e:
            self._handle_navigation_error(f"Error hiding tab '{self.current_tab}': {e}")
            return False
            
    def _show_tab(self, tab_id: str, force_rebuild: bool = False) -> bool:
        """Show the specified tab."""
        try:
            # Build/rebuild tab content if needed
            if force_rebuild or tab_id not in self.tab_widgets:
                if not self._build_tab_content(tab_id):
                    return False
                    
            # Show the tab widget
            if tab_id in self.tab_widgets:
                widget = self.tab_widgets[tab_id]
                if hasattr(widget, 'pack'):
                    widget.pack(fill="both", expand=True)
                elif hasattr(widget, 'grid'):
                    widget.grid(row=0, column=0, sticky="nsew")
                    
            return True
        except Exception as e:
            self._handle_navigation_error(f"Error showing tab '{tab_id}': {e}")
            return False
            
    def _build_tab_content(self, tab_id: str) -> bool:
        """Build tab content using registered builder."""
        try:
            if tab_id in self.tab_builders:
                # Use registered builder function
                builder = self.tab_builders[tab_id]
                widget = builder()
                if widget:
                    self.tab_widgets[tab_id] = widget
                    return True
            elif hasattr(self.app, f'_build_{tab_id}_tab'):
                # Use app's built-in builder method
                builder_method = getattr(self.app, f'_build_{tab_id}_tab')
                builder_method()
                return True
                
            return False
        except Exception as e:
            self._handle_navigation_error(f"Error building tab '{tab_id}': {e}")
            return False
            
    def _update_navigation_ui(self):
        """Update navigation UI elements (buttons, titles, etc.)."""
        try:
            # Update tab title in header
            if hasattr(self.app, 'tab_title'):
                title = self.get_tab_title(self.current_tab)
                self.app.tab_title.configure(text=title)
                
            # Update navigation button states
            if hasattr(self.app, 'nav_buttons'):
                self._update_navigation_buttons()
                
            # Update back button visibility
            self._update_back_button()
            
        except Exception as e:
            self._handle_navigation_error(f"Error updating navigation UI: {e}")
            
    def _update_navigation_buttons(self):
        """Update the state of navigation buttons."""
        try:
            if not hasattr(self.app, 'nav_buttons'):
                return
                
            for btn_id, btn in self.app.nav_buttons.items():
                if btn_id == self.current_tab:
                    # Selected/active button
                    btn.configure(
                        state="disabled",
                        fg_color=getattr(self.app, 'Colors', {}).get('PRIMARY_HOVER', '#1f538d'),
                        hover_color=getattr(self.app, 'Colors', {}).get('PRIMARY_HOVER', '#1f538d')
                    )
                else:
                    # Normal button
                    btn.configure(
                        state="normal",
                        fg_color=getattr(self.app, 'Colors', {}).get('PRIMARY', '#1f538d'),
                        hover_color=getattr(self.app, 'Colors', {}).get('PRIMARY_HOVER', '#1f538d')
                    )
        except Exception as e:
            self._handle_navigation_error(f"Error updating navigation buttons: {e}")
            
    def _update_back_button(self):
        """Update back button visibility and state."""
        try:
            # Show back button for sub-views or if history exists
            if self.view_stack or (self.tab_history and self.current_state == NavigationState.MAIN_TAB):
                if hasattr(self.app, '_show_back_button'):
                    self.app._show_back_button()
            else:
                if hasattr(self.app, '_hide_back_button'):
                    self.app._hide_back_button()
        except Exception as e:
            self._handle_navigation_error(f"Error updating back button: {e}")
            
    def _execute_navigation_hooks(self, old_tab: str, new_tab: str):
        """Execute registered navigation hooks."""
        for hook in self.navigation_hooks:
            try:
                hook(old_tab, new_tab)
            except Exception as e:
                self._handle_navigation_error(f"Error in navigation hook: {e}")
                
    def _execute_tab_callback(self, tab_id: str):
        """Execute tab-specific callback if registered."""
        if tab_id in self.tab_callbacks:
            try:
                self.tab_callbacks[tab_id]()
            except Exception as e:
                self._handle_navigation_error(f"Error executing callback for tab '{tab_id}': {e}")
                
    def _handle_navigation_error(self, error_message: str):
        """Handle navigation errors with proper logging."""
        print(f"Navigation Error: {error_message}")
        
        # Log to app if available
        if hasattr(self.app, '_log'):
            self.app._log(f"Navigation: {error_message}")
            
    def _get_timestamp(self) -> float:
        """Get current timestamp."""
        import time
        return time.time()
        
    def __str__(self) -> str:
        """String representation of the navigation controller."""
        return f"NavigationController(current_tab='{self.current_tab}', state={self.current_state.value})"
        
    def __repr__(self) -> str:
        """Detailed string representation."""
        return (f"NavigationController(current_tab='{self.current_tab}', "
                f"state={self.current_state.value}, "
                f"history_length={len(self.tab_history)}, "
                f"view_stack_depth={len(self.view_stack)})")
