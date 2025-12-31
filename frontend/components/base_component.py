# frontend/components/base_component.py
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, Callable
import uuid

class BaseComponent(ABC):
    """Base class for all UI components in DataForge"""
    
    def __init__(self, parent, component_id: Optional[str] = None):
        self.parent = parent
        self.component_id = component_id or self._generate_id()
        self._widgets: Dict[str, Any] = {}
        self._event_handlers: Dict[str, Callable] = {}
        self._is_built = False
        
    def _generate_id(self) -> str:
        """Generate unique component ID"""
        return f"{self.__class__.__name__}_{uuid.uuid4().hex[:8]}"
        
    @abstractmethod
    def get_component_type(self) -> str:
        """Get component type for identification"""
        pass
        
    @abstractmethod
    def build_ui(self):
        """Build the component's UI - must be implemented by subclasses"""
        pass
        
    def initialize(self):
        """Initialize the component (called after creation)"""
        if not self._is_built:
            self.build_ui()
            self._is_built = True
            
    def register_widget(self, name: str, widget: Any):
        """Register a widget for later access"""
        self._widgets[name] = widget
        
    def get_widget(self, name: str) -> Optional[Any]:
        """Get a registered widget by name"""
        return self._widgets.get(name)
        
    def add_event_handler(self, event: str, handler: Callable):
        """Add event handler for component events"""
        self._event_handlers[event] = handler
        
    def trigger_event(self, event: str, *args, **kwargs):
        """Trigger an event and call associated handler"""
        handler = self._event_handlers.get(event)
        if handler:
            try:
                handler(*args, **kwargs)
            except Exception as e:
                print(f"Error in event handler for '{event}': {e}")
                
    def refresh(self):
        """Refresh component data/state - override in subclasses"""
        pass
        
    def reset(self):
        """Reset component to initial state - override in subclasses"""
        pass
        
    def destroy(self):
        """Clean up component resources"""
        # Destroy all registered widgets
        for widget in self._widgets.values():
            try:
                if hasattr(widget, 'destroy'):
                    widget.destroy()
            except Exception as e:
                print(f"Error destroying widget: {e}")
                
        # Clear references
        self._widgets.clear()
        self._event_handlers.clear()
        
    def show(self):
        """Show the component"""
        main_widget = self.get_main_widget()
        if main_widget:
            try:
                if hasattr(main_widget, 'pack'):
                    main_widget.pack(fill="both", expand=True)
                elif hasattr(main_widget, 'grid'):
                    main_widget.grid(row=0, column=0, sticky="nsew")
            except Exception as e:
                print(f"Error showing component: {e}")
                
    def hide(self):
        """Hide the component"""
        main_widget = self.get_main_widget()
        if main_widget:
            try:
                if hasattr(main_widget, 'pack_forget'):
                    main_widget.pack_forget()
                elif hasattr(main_widget, 'grid_forget'):
                    main_widget.grid_forget()
                elif hasattr(main_widget, 'place_forget'):
                    main_widget.place_forget()
            except Exception as e:
                print(f"Error hiding component: {e}")
                
    def get_main_widget(self) -> Optional[Any]:
        """Get the main widget for this component - override in subclasses"""
        return self._widgets.get('main_frame')
        
    def set_enabled(self, enabled: bool):
        """Enable or disable the component"""
        for widget in self._widgets.values():
            try:
                if hasattr(widget, 'configure'):
                    state = "normal" if enabled else "disabled"
                    widget.configure(state=state)
            except Exception as e:
                print(f"Error setting widget state: {e}")
                
    def get_component_info(self) -> Dict[str, Any]:
        """Get component information for debugging"""
        return {
            'id': self.component_id,
            'type': self.get_component_type(),
            'class': self.__class__.__name__,
            'widgets_count': len(self._widgets),
            'handlers_count': len(self._event_handlers),
            'is_built': self._is_built
        }
        
    def log_error(self, message: str, exception: Optional[Exception] = None):
        """Log component errors"""
        error_msg = f"[{self.component_id}] {message}"
        if exception:
            error_msg += f": {str(exception)}"
        print(error_msg)
        
    def log_info(self, message: str):
        """Log component info"""
        print(f"[{self.component_id}] {message}")
