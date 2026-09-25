"""
UI Compatibility Layer for CustomTkinter/Tkinter fallback
"""
import tkinter as tk
from typing import Any, Optional

try:
    import customtkinter as ctk
    HAS_CTK = True
except ImportError:
    HAS_CTK = False
    ctk = None

from frontend.services.ui_framework_adapter import UIFrameworkAdapter


class CompatCTK:
    """Backward compatibility for CTK usage."""
    
    def __init__(self):
        self.ui_adapter = UIFrameworkAdapter()
    
    @staticmethod
    def CTk(**kwargs):
        return UIFrameworkAdapter.create_main_window(**kwargs)
    
    @staticmethod
    def CTkFrame(parent, **kwargs):
        return UIFrameworkAdapter.create_frame(parent, **kwargs)
    
    @staticmethod
    def CTkButton(parent, **kwargs):
        return UIFrameworkAdapter.create_button(parent, **kwargs)
    
    @staticmethod
    def CTkLabel(parent, **kwargs):
        return UIFrameworkAdapter.create_label(parent, **kwargs)
    
    @staticmethod
    def CTkEntry(parent, **kwargs):
        return UIFrameworkAdapter.create_entry(parent, **kwargs)
    
    @staticmethod
    def CTkTextbox(parent, **kwargs):
        return UIFrameworkAdapter.create_textbox(parent, **kwargs)
    
    @staticmethod
    def CTkComboBox(parent, **kwargs):
        return UIFrameworkAdapter.create_combobox(parent, **kwargs)
    
    @staticmethod
    def CTkCheckBox(parent, **kwargs):
        return UIFrameworkAdapter.create_checkbox(parent, **kwargs)
    
    @staticmethod
    def CTkProgressBar(parent, **kwargs):
        return UIFrameworkAdapter.create_progressbar(parent, **kwargs)
    
    @staticmethod
    def CTkScrollableFrame(parent, **kwargs):
        return UIFrameworkAdapter.create_scrollable_frame(parent, **kwargs)
    
    @staticmethod
    def CTkFont(**kwargs):
        return UIFrameworkAdapter.create_font(**kwargs)
    
    @staticmethod
    def CTkOptionMenu(parent, **kwargs):
        # For now, use combobox as fallback
        return UIFrameworkAdapter.create_combobox(parent, **kwargs)
    
    @staticmethod
    def CTkTabview(parent, **kwargs):
        # Create basic tabview using tkinter
        if HAS_CTK and ctk is not None:
            return ctk.CTkTabview(parent, **kwargs)
        else:
            from tkinter import ttk
            return ttk.Notebook(parent, **kwargs)
    
    # Add direct access to tkinter modules
    @property
    def filedialog(self):
        from tkinter import filedialog
        return filedialog
    
    @property  
    def messagebox(self):
        from tkinter import messagebox
        return messagebox
    
    @property
    def ttk(self):
        from tkinter import ttk
        return ttk


# Create compatibility object for backward compatibility
def get_ctk_instance():
    """Get CTK compatibility instance"""
    return CompatCTK()
