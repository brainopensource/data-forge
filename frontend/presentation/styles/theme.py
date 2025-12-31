from __future__ import annotations
from typing import Dict, Any

class ButtonVariants:
    """Defines the style variants for buttons."""
    PRIMARY = "PRIMARY"
    SECONDARY = "SECONDARY"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    ERROR = "ERROR"
    SIDEBAR = "SIDEBAR"
    SIDEBAR_ACTIVE = "SIDEBAR_ACTIVE"

class Theme:
    """
    Centralized theme for the application.
    Provides colors, fonts, and component-specific style configurations.
    """
    # --- Base Colors ---
    COLOR_BACKGROUND = "#000000"
    COLOR_SURFACE = "#1a1a1a"
    COLOR_SURFACE_LIGHT = "#2b2b2b"
    COLOR_PRIMARY = "#1f538d"  # Blue for selected/active
    COLOR_SECONDARY = "#5e35b1" # Purple for hover
    COLOR_SUCCESS = "#00897b"  # Teal
    COLOR_WARNING = "#fdd835"  # Yellow
    COLOR_ERROR = "#d32f2f"   # Red
    COLOR_TEXT_PRIMARY = "#ffffff"
    COLOR_TEXT_SECONDARY = "#b0bec5"
    COLOR_TRANSPARENT = "transparent"
    GRAY_LIGHT = '#404040'

    # --- Fonts ---
    FONT_FAMILY_DEFAULT = ("Roboto", 13)
    FONT_WEIGHT_NORMAL = "normal"
    FONT_WEIGHT_BOLD = "bold"

    def get_button_kwargs(self, variant: str) -> Dict[str, Any]:
        """
        Returns a dictionary of styling keyword arguments for a given button variant.
        """
        # --- Base Button Style ---
        base_style = {
            "corner_radius": 6,
            "height": 36,
            "border_width": 1,
            "border_color": self.COLOR_PRIMARY,
            "text_color": self.COLOR_TEXT_PRIMARY,
            "font": self.FONT_FAMILY_DEFAULT,
        }

        # --- Variant-Specific Styles ---
        if variant == ButtonVariants.PRIMARY:
            return {
                **base_style,
                "fg_color": self.COLOR_PRIMARY,
                "hover_color": self.COLOR_SECONDARY,
            }
        if variant == ButtonVariants.SECONDARY:
            return {
                **base_style,
                "fg_color": self.COLOR_SURFACE_LIGHT,
                "hover_color": self.COLOR_SECONDARY,
                "border_color": self.COLOR_SECONDARY,
            }
        if variant == ButtonVariants.SUCCESS:
            return {
                **base_style,
                "fg_color": self.COLOR_SUCCESS,
                "hover_color": "#00a991",
                "border_color": self.COLOR_SUCCESS,
            }
        if variant == ButtonVariants.WARNING:
            return {
                **base_style,
                "fg_color": self.COLOR_WARNING,
                "hover_color": "#ffeb3b",
                "border_color": self.COLOR_WARNING,
                "text_color": self.COLOR_BACKGROUND,
            }
        if variant == ButtonVariants.ERROR:
            return {
                **base_style,
                "fg_color": self.COLOR_ERROR,
                "hover_color": "#e53935",
                "border_color": self.COLOR_ERROR,
            }
        if variant == ButtonVariants.SIDEBAR:
            return {
                "corner_radius": 0,
                "height": 40,
                "border_spacing": 10,
                "fg_color": self.COLOR_TRANSPARENT,
                "text_color": self.COLOR_TEXT_PRIMARY,
                "hover_color": self.COLOR_SECONDARY,
                "anchor": "w",
                "font": (self.FONT_FAMILY_DEFAULT[0], self.FONT_FAMILY_DEFAULT[1], self.FONT_WEIGHT_BOLD),
            }
        if variant == ButtonVariants.SIDEBAR_ACTIVE:
            return {
                **self.get_button_kwargs(ButtonVariants.SIDEBAR),
                "fg_color": self.COLOR_PRIMARY,
            }
        
        # Default fallback
        return self.get_button_kwargs(ButtonVariants.PRIMARY)

# --- Default Theme Instance ---
# This instance is imported and used throughout the application.
DEFAULT_THEME = Theme()
