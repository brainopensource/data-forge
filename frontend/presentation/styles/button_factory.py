"""
Enhanced Button Factory for DataForge Frontend.

This module provides a centralized button creation system with consistent styling,
hover effects, and behavior management across the entire application.
"""
from typing import Callable, Optional, Dict, Any, Union
import tkinter as tk

try:
    import customtkinter as ctk
    HAS_CTK = True
except ImportError:
    import tkinter as tk
    from tkinter import ttk
    HAS_CTK = False
    # Create mock ctk module for fallback
    class MockCTk:
        CTkButton = tk.Button
        CTkFrame = tk.Frame
        CTkLabel = tk.Label
    ctk = MockCTk()

from frontend.presentation.styles.theme import DEFAULT_THEME, ButtonVariants


class DataForgeButton:
    """
    Enhanced button wrapper providing consistent styling and behavior.
    
    This class wraps either CustomTkinter buttons or standard tkinter buttons
    and applies consistent theming, hover effects, and interaction patterns.
    """
    
    def __init__(
        self,
        parent,
        text: str = "",
        command: Optional[Callable] = None,
        variant: str = "PRIMARY",
        icon: Optional[str] = None,
        tooltip: Optional[str] = None,
        enabled: bool = True,
        **kwargs
    ):
        """
        Initialize a DataForge button with consistent styling.
        
        Args:
            parent: Parent widget
            text: Button text
            command: Callback function
            variant: Button style variant (PRIMARY, SECONDARY, etc.)
            icon: Optional icon text/emoji
            tooltip: Optional tooltip text
            enabled: Whether button is enabled
            **kwargs: Additional widget-specific arguments
        """
        self.parent = parent
        self.text = text
        self.command = command
        self.variant = variant
        self.icon = icon
        self.tooltip = tooltip
        self.enabled = enabled
        
        # Get theme configuration
        self.theme = DEFAULT_THEME
        self.style_config = self.theme.get_button_kwargs(variant)
        
        # Override with any provided kwargs
        self.style_config.update(kwargs)
        
        # Create the actual button widget
        self.widget = self._create_button()
        
        # Apply behavior enhancements
        self._apply_hover_behavior()
        self._apply_tooltip()
        
        # Set initial state
        if not enabled:
            self.disable()
    
    def _create_button(self) -> Any:
        """Create the underlying button widget."""
        # Prepare display text
        display_text = f"{self.icon} {self.text}" if self.icon else self.text
        
        # Handle command parameter
        command = self.command if self.command is not None else lambda: None
        
        # Get style configuration and handle font properly
        style_config = self.style_config.copy()
        
        if HAS_CTK:
            # For CustomTkinter, create a proper CTkFont object
            try:
                import customtkinter as ctk
                font_tuple = style_config.get("font", ("Arial", 12, "normal"))
                if isinstance(font_tuple, tuple) and len(font_tuple) >= 3:
                    style_config["font"] = ctk.CTkFont(
                        family=font_tuple[0], 
                        size=font_tuple[1], 
                        weight=font_tuple[2]
                    )
            except (ImportError, AttributeError):
                # Fallback to tuple format
                pass
            
            # Use CustomTkinter button
            button = ctk.CTkButton(
                self.parent,
                text=display_text,
                command=command,
                **style_config
            )
        else:
            # Use standard tkinter button with manual styling
            font_config = style_config.get("font", ("Arial", 12, "normal"))
            button = tk.Button(
                self.parent,
                text=display_text,
                command=command,
                bg=style_config.get("fg_color", "#1976D2"),
                fg=style_config.get("text_color", "white"),
                activebackground=style_config.get("hover_color", "#8A2BE2"),
                activeforeground=style_config.get("text_color", "white"),
                relief="flat",
                borderwidth=style_config.get("border_width", 0),
                height=style_config.get("height", 36) // 20,  # Convert to text lines
                font=font_config
            )
        
        return button
    
    def _apply_hover_behavior(self):
        """Apply hover effects for consistent interaction feedback."""
        if not HAS_CTK:
            # For tkinter fallback, implement manual hover effects
            original_bg = self.style_config.get("fg_color", "#1976D2")
            hover_bg = self.style_config.get("hover_color", "#8A2BE2")
            
            def on_enter(event):
                if self.enabled:
                    self.widget.configure(bg=hover_bg)
            
            def on_leave(event):
                if self.enabled:
                    self.widget.configure(bg=original_bg)
            
            self.widget.bind("<Enter>", on_enter)
            self.widget.bind("<Leave>", on_leave)
    
    def _apply_tooltip(self):
        """Apply tooltip if provided."""
        if self.tooltip:
            # Simple tooltip implementation
            def show_tooltip(event):
                # In a real implementation, create a tooltip widget
                pass
            
            def hide_tooltip(event):
                pass
            
            self.widget.bind("<Enter>", show_tooltip, add='+')
            self.widget.bind("<Leave>", hide_tooltip, add='+')
    
    def enable(self):
        """Enable the button."""
        self.enabled = True
        if HAS_CTK:
            self.widget.configure(state="normal")
        else:
            self.widget.configure(state="normal")
    
    def disable(self):
        """Disable the button."""
        self.enabled = False
        if HAS_CTK:
            self.widget.configure(state="disabled")
        else:
            self.widget.configure(state="disabled")
    
    def update_text(self, new_text: str):
        """Update button text."""
        self.text = new_text
        display_text = f"{self.icon} {self.text}" if self.icon else self.text
        self.widget.configure(text=display_text)
    
    def update_variant(self, new_variant: str):
        """Update button style variant."""
        self.variant = new_variant
        new_config = self.theme.get_button_kwargs(new_variant)
        
        if HAS_CTK:
            self.widget.configure(**new_config)
        else:
            # Map CTK config to tkinter
            self.widget.configure(
                bg=new_config.get("fg_color"),
                activebackground=new_config.get("hover_color"),
                fg=new_config.get("text_color"),
                font=new_config.get("font")
            )
    
    # Delegate grid and pack methods to the underlying widget
    def grid(self, **kwargs):
        """Grid layout method."""
        return self.widget.grid(**kwargs)
    
    def pack(self, **kwargs):
        """Pack layout method."""
        return self.widget.pack(**kwargs)
    
    def place(self, **kwargs):
        """Place layout method."""
        if hasattr(self.widget, 'place'):
            return self.widget.place(**kwargs)


class ButtonFactory:
    """
    Factory class for creating consistent buttons throughout the application.
    
    This factory ensures all buttons follow the same styling patterns and
    behavior while providing convenient creation methods for common use cases.
    """
    
    @staticmethod
    def create_primary_button(
        parent, 
        text: str, 
        command: Optional[Callable] = None,
        **kwargs
    ) -> DataForgeButton:
        """Create a primary action button."""
        return DataForgeButton(parent, text, command, "PRIMARY", **kwargs)
    
    @staticmethod
    def create_secondary_button(
        parent, 
        text: str, 
        command: Optional[Callable] = None,
        **kwargs
    ) -> DataForgeButton:
        """Create a secondary action button."""
        return DataForgeButton(parent, text, command, "SECONDARY", **kwargs)
    
    @staticmethod
    def create_success_button(
        parent, 
        text: str, 
        command: Optional[Callable] = None,
        **kwargs
    ) -> DataForgeButton:
        """Create a success/confirmation button."""
        return DataForgeButton(parent, text, command, "SUCCESS", **kwargs)
    
    @staticmethod
    def create_warning_button(
        parent, 
        text: str, 
        command: Optional[Callable] = None,
        **kwargs
    ) -> DataForgeButton:
        """Create a warning button."""
        return DataForgeButton(parent, text, command, "WARNING", **kwargs)
    
    @staticmethod
    def create_error_button(
        parent, 
        text: str, 
        command: Optional[Callable] = None,
        **kwargs
    ) -> DataForgeButton:
        """Create an error/destructive action button."""
        return DataForgeButton(parent, text, command, "ERROR", **kwargs)
    
    @staticmethod
    def create_sidebar_button(
        parent, 
        text: str, 
        command: Optional[Callable] = None,
        is_active: bool = False,
        **kwargs
    ) -> DataForgeButton:
        """Create a sidebar navigation button."""
        variant = "SIDEBAR_ACTIVE" if is_active else "SIDEBAR"
        return DataForgeButton(parent, text, command, variant, **kwargs)
    
    @staticmethod
    def create_icon_button(
        parent, 
        icon: str,
        text: str = "",
        command: Optional[Callable] = None,
        variant: str = "PRIMARY",
        **kwargs
    ) -> DataForgeButton:
        """Create an icon button with optional text."""
        return DataForgeButton(parent, text, command, variant, icon=icon, **kwargs)
    
    @staticmethod
    def create_action_button_group(
        parent,
        actions: list[tuple[str, Callable]],
        variant: str = "PRIMARY",
        **kwargs
    ) -> list[DataForgeButton]:
        """
        Create a group of related action buttons.
        
        Args:
            parent: Parent widget
            actions: List of (text, command) tuples
            variant: Button style variant
            **kwargs: Additional arguments for all buttons
        
        Returns:
            List of created buttons
        """
        buttons = []
        for text, command in actions:
            button = DataForgeButton(parent, text, command, variant, **kwargs)
            buttons.append(button)
        return buttons


# Convenience functions for backward compatibility
def create_button(parent, text: str, command: Optional[Callable] = None, **kwargs) -> DataForgeButton:
    """Create a primary button (convenience function)."""
    return ButtonFactory.create_primary_button(parent, text, command, **kwargs)


def create_sidebar_button(parent, text: str, command: Optional[Callable] = None, is_active: bool = False, **kwargs) -> DataForgeButton:
    """Create a sidebar button (convenience function)."""
    return ButtonFactory.create_sidebar_button(parent, text, command, is_active, **kwargs)
