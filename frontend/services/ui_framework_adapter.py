"""
UI Framework Adapter

Centralized UI framework abstraction for CustomTkinter/tkinter fallback.
Eliminates code duplication and provides consistent interface.
"""

from typing import Any, Optional, Callable, Dict, Union, List, Type, TYPE_CHECKING
import tkinter as tk
from tkinter import ttk

try:
    import customtkinter as ctk  # type: ignore
    HAS_CTK = True
except ImportError:
    HAS_CTK = False
    ctk = None  # type: ignore

# Type aliases for better compatibility
if TYPE_CHECKING:
    Widget = Union[tk.Widget, Any]
else:
    Widget = Any


class UIFrameworkAdapter:
    """Centralized UI framework abstraction for CustomTkinter/tkinter"""
    
    @staticmethod
    def is_customtkinter_available() -> bool:
        """Check if CustomTkinter is available"""
        return HAS_CTK

    @staticmethod
    def get_framework_name() -> str:
        """Get current framework name"""
        return "CustomTkinter" if HAS_CTK else "tkinter"
    
    @staticmethod
    def initialize_framework(appearance_mode: str = "dark", color_theme: str = "blue"):
        """Initialize the UI framework with theme settings"""
        if HAS_CTK and ctk is not None:
            try:
                ctk.set_appearance_mode(appearance_mode)  # type: ignore
                ctk.set_default_color_theme(color_theme)  # type: ignore
            except Exception as e:
                print(f"Error initializing CustomTkinter theme: {e}")
    
    @staticmethod
    def create_main_window(**kwargs) -> Any:
        """Create main application window"""
        if HAS_CTK and ctk is not None:
            return ctk.CTk(**kwargs)  # type: ignore
        else:
            window = tk.Tk(**kwargs)
            # Apply dark theme styling for tkinter
            window.configure(bg='#2b2b2b')
            return window
    
    @staticmethod
    def create_button(parent, text: str = "", command: Optional[Callable[[], None]] = None, **kwargs) -> Any:
        """Create button with framework abstraction"""
        if HAS_CTK and ctk is not None:
            # Convert custom font parameters to CTkFont
            safe_kwargs = dict(kwargs)
            font_size = safe_kwargs.pop('font_size', 12)
            font_weight = safe_kwargs.pop('font_weight', 'normal')
            font_family = safe_kwargs.pop('font_family', 'Arial')
            
            # Create CTkFont if any font parameters are specified
            if 'font_size' in kwargs or 'font_weight' in kwargs or 'font_family' in kwargs:
                safe_kwargs['font'] = ctk.CTkFont(size=font_size, weight=font_weight, family=font_family)
                
            return ctk.CTkButton(parent, text=text, command=command, **safe_kwargs)  # type: ignore
        else:
            # tkinter fallback with styling - only set safe defaults
            safe_kwargs = dict(kwargs)
            font_size = safe_kwargs.pop('font_size', 12)
            font_weight = safe_kwargs.pop('font_weight', 'normal')
            font_family = safe_kwargs.pop('font_family', 'Arial')
            
            if 'bg' not in safe_kwargs:
                safe_kwargs['bg'] = '#1976D2'
            if 'fg' not in safe_kwargs:
                safe_kwargs['fg'] = 'white'
            if 'activebackground' not in safe_kwargs:
                safe_kwargs['activebackground'] = '#1565C0'
            if 'activeforeground' not in safe_kwargs:
                safe_kwargs['activeforeground'] = 'white'
            if 'relief' not in safe_kwargs:
                safe_kwargs['relief'] = 'flat'
            if 'borderwidth' not in safe_kwargs:
                safe_kwargs['borderwidth'] = 0
            if 'padx' not in safe_kwargs:
                safe_kwargs['padx'] = 15
            if 'pady' not in safe_kwargs:
                safe_kwargs['pady'] = 8
                
            # Create font tuple for tkinter
            safe_kwargs['font'] = (font_family, font_size, font_weight)
            
            return tk.Button(parent, text=text, command=command or (lambda: None), **safe_kwargs)

    @staticmethod
    def create_label(parent, text: str = "", **kwargs) -> Any:
        """Create label with framework abstraction"""
        if HAS_CTK and ctk is not None:
            # Convert custom font parameters to CTkFont
            safe_kwargs = dict(kwargs)
            font_size = safe_kwargs.pop('font_size', 12)
            font_weight = safe_kwargs.pop('font_weight', 'normal')
            font_family = safe_kwargs.pop('font_family', 'Arial')
            wraplength = safe_kwargs.pop('wraplength', None)
            
            # Create CTkFont if any font parameters are specified
            if 'font_size' in kwargs or 'font_weight' in kwargs or 'font_family' in kwargs:
                safe_kwargs['font'] = ctk.CTkFont(size=font_size, weight=font_weight, family=font_family)
            
            # Handle wraplength for CustomTkinter (it uses a different parameter)
            if wraplength:
                safe_kwargs['wraplength'] = wraplength
                
            return ctk.CTkLabel(parent, text=text, **safe_kwargs)  # type: ignore
        else:
            safe_kwargs = dict(kwargs)
            font_size = safe_kwargs.pop('font_size', 12)
            font_weight = safe_kwargs.pop('font_weight', 'normal')
            font_family = safe_kwargs.pop('font_family', 'Arial')
            
            if 'bg' not in safe_kwargs:
                try:
                    if hasattr(parent, 'cget'):
                        safe_kwargs['bg'] = parent.cget('bg')
                    else:
                        safe_kwargs['bg'] = '#2b2b2b'
                except:
                    safe_kwargs['bg'] = '#2b2b2b'
            if 'fg' not in safe_kwargs:
                safe_kwargs['fg'] = 'white'
            
            # Create font tuple for tkinter
            safe_kwargs['font'] = (font_family, font_size, font_weight)
            
            return tk.Label(parent, text=text, **safe_kwargs)

    @staticmethod
    def create_entry(parent, **kwargs) -> Any:
        """Create entry with framework abstraction"""
        if HAS_CTK and ctk is not None:
            # Handle placeholder_text parameter
            safe_kwargs = dict(kwargs)
            placeholder = safe_kwargs.pop('placeholder_text', None)
            if placeholder:
                safe_kwargs['placeholder_text'] = placeholder
            return ctk.CTkEntry(parent, **safe_kwargs)  # type: ignore
        else:
            safe_kwargs = dict(kwargs)
            # Remove placeholder_text for tkinter (not supported)
            placeholder = safe_kwargs.pop('placeholder_text', None)
            
            if 'bg' not in safe_kwargs:
                safe_kwargs['bg'] = '#404040'
            if 'fg' not in safe_kwargs:
                safe_kwargs['fg'] = 'white'
            if 'insertbackground' not in safe_kwargs:
                safe_kwargs['insertbackground'] = 'white'
            if 'relief' not in safe_kwargs:
                safe_kwargs['relief'] = 'flat'
            if 'borderwidth' not in safe_kwargs:
                safe_kwargs['borderwidth'] = 1
            
            entry = tk.Entry(parent, **safe_kwargs)
            
            # Simulate placeholder text for tkinter
            if placeholder:
                def on_focus_in(event):
                    if entry.get() == placeholder:
                        entry.delete(0, "end")
                        entry.configure(fg='white')
                
                def on_focus_out(event):
                    if entry.get() == "":
                        entry.insert(0, placeholder)
                        entry.configure(fg='gray')
                
                entry.insert(0, placeholder)
                entry.configure(fg='gray')
                entry.bind("<FocusIn>", on_focus_in)
                entry.bind("<FocusOut>", on_focus_out)
            
            return entry

    @staticmethod
    def create_frame(parent, **kwargs) -> Any:
        """Create frame with framework abstraction"""
        if HAS_CTK and ctk is not None:
            return ctk.CTkFrame(parent, **kwargs)  # type: ignore
        else:
            safe_kwargs = dict(kwargs)
            if 'bg' not in safe_kwargs:
                safe_kwargs['bg'] = '#2b2b2b'
            return tk.Frame(parent, **safe_kwargs)

    @staticmethod
    def create_textbox(parent, **kwargs) -> Any:
        """Create textbox with framework abstraction"""
        if HAS_CTK and ctk is not None:
            return ctk.CTkTextbox(parent, **kwargs)  # type: ignore
        else:
            safe_kwargs = dict(kwargs)
            if 'bg' not in safe_kwargs:
                safe_kwargs['bg'] = '#404040'
            if 'fg' not in safe_kwargs:
                safe_kwargs['fg'] = 'white'
            if 'insertbackground' not in safe_kwargs:
                safe_kwargs['insertbackground'] = 'white'
            if 'relief' not in safe_kwargs:
                safe_kwargs['relief'] = 'flat'
            if 'borderwidth' not in safe_kwargs:
                safe_kwargs['borderwidth'] = 1
            return tk.Text(parent, **safe_kwargs)
            
    @staticmethod
    def create_combobox(parent, values: Optional[List[str]] = None, **kwargs) -> Any:
        """Create combobox with framework abstraction"""
        if HAS_CTK and ctk is not None:
            # Handle variable and command parameters
            safe_kwargs = dict(kwargs)
            variable = safe_kwargs.pop('variable', None)
            command = safe_kwargs.pop('command', None)
            
            combobox = ctk.CTkComboBox(parent, values=values or [], **safe_kwargs)  # type: ignore
            
            # Set variable if provided
            if variable:
                combobox.set(variable.get())
                # Bind variable changes (CTkComboBox uses command parameter differently)
                if command:
                    combobox.configure(command=command)
            
            return combobox
        else:
            # Use ttk.Combobox with custom styling
            safe_kwargs = dict(kwargs)
            variable = safe_kwargs.pop('variable', None)
            command = safe_kwargs.pop('command', None)
            
            style = ttk.Style()
            style.theme_use('clam')
            style.configure('Custom.TCombobox',
                          fieldbackground='#404040',
                          background='#404040',
                          foreground='white',
                          borderwidth=1)
            
            # Set textvariable if variable provided
            if variable:
                safe_kwargs['textvariable'] = variable
            
            combobox = ttk.Combobox(parent, values=values or [], style='Custom.TCombobox', **safe_kwargs)
            
            # Bind command if provided
            if command:
                combobox.bind('<<ComboboxSelected>>', lambda e: command(combobox.get()))
            
            return combobox
            
    @staticmethod
    def create_checkbox(parent, text: str = "", **kwargs) -> Any:
        """Create checkbox with framework abstraction"""
        if HAS_CTK and ctk is not None:
            return ctk.CTkCheckBox(parent, text=text, **kwargs)  # type: ignore
        else:
            safe_kwargs = dict(kwargs)
            if 'bg' not in safe_kwargs:
                try:
                    if hasattr(parent, 'cget'):
                        safe_kwargs['bg'] = parent.cget('bg')
                    else:
                        safe_kwargs['bg'] = '#2b2b2b'
                except:
                    safe_kwargs['bg'] = '#2b2b2b'
            if 'fg' not in safe_kwargs:
                safe_kwargs['fg'] = 'white'
            if 'activebackground' not in safe_kwargs:
                safe_kwargs['activebackground'] = '#2b2b2b'
            if 'activeforeground' not in safe_kwargs:
                safe_kwargs['activeforeground'] = 'white'
            if 'selectcolor' not in safe_kwargs:
                safe_kwargs['selectcolor'] = '#1976D2'
            return tk.Checkbutton(parent, text=text, **safe_kwargs)
            
    @staticmethod
    def create_progressbar(parent, **kwargs) -> Any:
        """Create progress bar with framework abstraction"""
        if HAS_CTK and ctk is not None:
            return ctk.CTkProgressBar(parent, **kwargs)  # type: ignore
        else:
            style = ttk.Style()
            style.theme_use('clam')
            style.configure('Custom.Horizontal.TProgressbar',
                          background='#1976D2',
                          troughcolor='#404040',
                          borderwidth=1,
                          lightcolor='#1976D2',
                          darkcolor='#1976D2')
            return ttk.Progressbar(parent, style='Custom.Horizontal.TProgressbar', **kwargs)
            
    @staticmethod
    def create_scrollable_frame(parent, **kwargs) -> Any:
        """Create scrollable frame"""
        if HAS_CTK and ctk is not None:
            return ctk.CTkScrollableFrame(parent, **kwargs)  # type: ignore
        else:
            # Create a frame with scrollbars for tkinter
            main_frame = UIFrameworkAdapter.create_frame(parent, **kwargs)
            
            # Add scrollbars
            v_scrollbar = tk.Scrollbar(main_frame, orient=tk.VERTICAL)
            h_scrollbar = tk.Scrollbar(main_frame, orient=tk.HORIZONTAL)
            
            # Create canvas for scrolling
            canvas = tk.Canvas(main_frame, 
                             yscrollcommand=v_scrollbar.set,
                             xscrollcommand=h_scrollbar.set,
                             bg='#2b2b2b',
                             highlightthickness=0)
            
            v_scrollbar.config(command=canvas.yview)
            h_scrollbar.config(command=canvas.xview)
            
            # Create inner frame
            inner_frame = UIFrameworkAdapter.create_frame(canvas)
            canvas.create_window((0, 0), window=inner_frame, anchor=tk.NW)
            
            # Pack scrollbars and canvas
            v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
            canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            
            # Update scroll region when inner frame changes
            def configure_scroll_region(event=None):
                canvas.configure(scrollregion=canvas.bbox("all"))
            
            inner_frame.bind('<Configure>', configure_scroll_region)
            
            # Add the inner frame as an attribute for easy access
            main_frame.inner_frame = inner_frame  # type: ignore
            main_frame.canvas = canvas  # type: ignore
            
            return main_frame
    
    @staticmethod
    def create_font(size: int = 12, weight: str = "normal", family: str = "Arial") -> Any:
        """Create font with framework abstraction"""
        if HAS_CTK and ctk is not None:
            return ctk.CTkFont(size=size, weight=weight, family=family)  # type: ignore
        else:
            return (family, size, weight)
    
    @staticmethod
    def apply_theme(theme: str = "dark"):
        """Apply theme if CustomTkinter is available"""
        if HAS_CTK and ctk is not None:
            try:
                ctk.set_appearance_mode(theme)  # type: ignore
            except Exception as e:
                print(f"Error setting theme: {e}")
                
    @staticmethod
    def get_theme() -> str:
        """Get current theme"""
        if HAS_CTK and ctk is not None:
            try:
                return ctk.get_appearance_mode()  # type: ignore
            except Exception:
                return "dark"
        return "tkinter"
        
    @staticmethod
    def create_styled_widget(widget_type: str, parent, **kwargs) -> Any:
        """Factory method to create widgets by type"""
        widget_creators = {
            'button': UIFrameworkAdapter.create_button,
            'label': UIFrameworkAdapter.create_label,
            'entry': UIFrameworkAdapter.create_entry,
            'frame': UIFrameworkAdapter.create_frame,
            'textbox': UIFrameworkAdapter.create_textbox,
            'combobox': UIFrameworkAdapter.create_combobox,
            'checkbox': UIFrameworkAdapter.create_checkbox,
            'progressbar': UIFrameworkAdapter.create_progressbar,
            'scrollable_frame': UIFrameworkAdapter.create_scrollable_frame,
        }
        
        creator = widget_creators.get(widget_type)
        if creator:
            return creator(parent, **kwargs)
        else:
            raise ValueError(f"Unknown widget type: {widget_type}")
    
    @staticmethod
    def get_color_scheme() -> Dict[str, str]:
        """Get standardized color scheme"""
        return {
            'primary': '#1976D2',
            'primary_hover': '#1565C0',
            'secondary': '#7B1FA2',
            'background': '#2b2b2b',
            'surface': '#404040',
            'text_primary': '#FFFFFF',
            'text_secondary': '#B0B0B0',
            'success': '#4CAF50',
            'warning': '#FF9800',
            'error': '#F44336'
        }


# Backward compatibility aliases
_adapter_instance = UIFrameworkAdapter()

def get_ui_adapter() -> UIFrameworkAdapter:
    """Get UI adapter instance (singleton pattern)"""
    return _adapter_instance


# Module-level convenience functions
def create_button(parent, text: str = "", command: Optional[Callable[[], None]] = None, **kwargs) -> Any:
    """Module-level convenience function for creating buttons"""
    return UIFrameworkAdapter.create_button(parent, text, command, **kwargs)


def create_frame(parent, **kwargs) -> Any:
    """Module-level convenience function for creating frames"""
    return UIFrameworkAdapter.create_frame(parent, **kwargs)


def create_label(parent, text: str = "", **kwargs) -> Any:
    """Module-level convenience function for creating labels"""
    return UIFrameworkAdapter.create_label(parent, text, **kwargs)
