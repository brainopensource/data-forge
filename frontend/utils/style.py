"""Centralized UI styling utilities for the DataForge frontend.

DEPRECATED: This module is maintained for backward compatibility.
New code should use the presentation layer styles in frontend.presentation.styles

This module centralizes visual constants and small helpers so the
main UI code can remain focused on behavior. It intentionally
avoids importing heavy UI libraries; callers should pass the
HAS_CTK flag from the running environment so this module can
apply the right runtime bindings for hover effects.
"""
from typing import Dict

# Re-export from new architecture for backward compatibility
try:
    from frontend.presentation.styles.theme import DEFAULT_THEME
    from frontend.presentation.styles.button_factory import ButtonFactory, create_button
    
    # Map old constants to new theme
    DEFAULT_BG = DEFAULT_THEME.colors.BACKGROUND_SECONDARY
    HOVER_BG = DEFAULT_THEME.colors.SECONDARY  # Purple hover per request
    ACTIVE_BG = DEFAULT_THEME.colors.PRIMARY_DARK
    TITLE_BG = "transparent"
    
    BUTTON_HEIGHT = DEFAULT_THEME.dimensions.BUTTON_HEIGHT_MEDIUM
    SIDEBAR_WIDTH = DEFAULT_THEME.dimensions.SIDEBAR_WIDTH
    BUTTON_PADX = DEFAULT_THEME.spacing.SM
    BUTTON_PADY = 2
    
    def get_button_kwargs() -> Dict:
        """Return construction kwargs which are safe for CustomTkinter usage."""
        return DEFAULT_THEME.get_button_kwargs("SIDEBAR")
    
    # Enhanced functions using new architecture
    def apply_button_behavior(widget, has_ctk: bool):
        """Apply runtime hover/leave behavior for buttons."""
        if has_ctk:
            # CustomTkinter handles hover effects natively
            return
        
        # For tkinter fallback, apply manual hover effects
        try:
            orig_bg = widget.cget("bg") if "bg" in widget.keys() else DEFAULT_BG
            orig_fg = widget.cget("fg") if "fg" in widget.keys() else "black"
            
            def _on_enter(e):
                try:
                    widget.configure(bg=HOVER_BG, fg="white")
                except Exception:
                    pass
            
            def _on_leave(e):
                try:
                    widget.configure(bg=orig_bg, fg=orig_fg)
                except Exception:
                    pass
            
            widget.bind("<Enter>", _on_enter)
            widget.bind("<Leave>", _on_leave)
        except Exception:
            return
    
    def set_active_style(widget, has_ctk: bool):
        """Style a widget as active/selected (used for current tab button)."""
        try:
            if has_ctk:
                widget.configure(fg_color=ACTIVE_BG)
            else:
                widget.configure(bg=ACTIVE_BG, fg="white")
        except Exception:
            pass
    
    def set_default_style(widget, has_ctk: bool):
        """Reset a widget to default (non-selected) style."""
        try:
            if has_ctk:
                widget.configure(fg_color=DEFAULT_BG)
            else:
                widget.configure(bg=DEFAULT_BG, fg="black")
        except Exception:
            pass

except ImportError:
    # Fallback to original implementation if new architecture is not available
    # Color / size palette (single source of truth)
    DEFAULT_BG = "#4a4a4a"
    HOVER_BG = "#8A2BE2"  # Purple hover per request
    ACTIVE_BG = "#1f538d"
    TITLE_BG = "transparent"

    BUTTON_HEIGHT = 28
    SIDEBAR_WIDTH = 200
    BUTTON_PADX = 10
    BUTTON_PADY = 2

    def get_button_kwargs() -> Dict:
        """Return construction kwargs which are safe for CustomTkinter usage."""
        return {
            "anchor": "w",
            "height": BUTTON_HEIGHT,
            "fg_color": DEFAULT_BG,
            "hover_color": HOVER_BG,
        }

    def apply_button_behavior(widget, has_ctk: bool):
        """Apply runtime hover/leave behavior for buttons."""
        if has_ctk:
            return

        try:
            orig_bg = widget.cget("bg") if "bg" in widget.keys() else DEFAULT_BG
            orig_fg = widget.cget("fg") if "fg" in widget.keys() else "black"

            def _on_enter(e):
                try:
                    widget.configure(bg=HOVER_BG, fg="white")
                except Exception:
                    pass

            def _on_leave(e):
                try:
                    widget.configure(bg=orig_bg, fg=orig_fg)
                except Exception:
                    pass

            widget.bind("<Enter>", _on_enter)
            widget.bind("<Leave>", _on_leave)
        except Exception:
            return

    def set_active_style(widget, has_ctk: bool):
        """Style a widget as active/selected (used for current tab button)."""
        try:
            if has_ctk:
                widget.configure(fg_color=ACTIVE_BG)
            else:
                widget.configure(bg=ACTIVE_BG, fg="white")
        except Exception:
            pass

    def set_default_style(widget, has_ctk: bool):
        """Reset a widget to default (non-selected) style."""
        try:
            if has_ctk:
                widget.configure(fg_color=DEFAULT_BG)
            else:
                widget.configure(bg=DEFAULT_BG, fg="black")
        except Exception:
            pass
