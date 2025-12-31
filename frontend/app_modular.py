"""
Main DataForge Application - Modular Version

This is the refactored main application that uses the CQRS architecture
and modular components.
"""

import tkinter as tk
from typing import Any, Dict, List, Optional

# Import configuration
from frontend.config import AppConfig, Colors

# Import services
from frontend.services.api_client import ApiClient
from frontend.services.data_generator import DataGenerator
from frontend.services.ui_framework_adapter import UIFrameworkAdapter

# Import components
from frontend.components import (
    DataExplorer, 
    EnhancedPlotExplorer, 
    FloatingDataExplorer
)

# Import controllers
from frontend.controllers import MainWindowController, UIController

# Import utilities
from frontend.utils.async_runner import AsyncRunner
from frontend.utils.ui_compat import get_ctk_instance
from frontend.utils.json_utils import format_json

# Import core services
from frontend.core.container import configure_services, get_service
from frontend.core.plugin_system import PluginManager

# Import tabs
from frontend.tabs.home_tab import HomeTab
from frontend.tabs.database_tab_new import DatabaseTab
from frontend.tabs.exploration_tab_new import ExplorationTab
from frontend.tabs.plugins_tab_new import PluginsTab
from frontend.tabs.help_tab_new import HelpTab

# Initialize UI compatibility
ctk = get_ctk_instance()
HAS_CTK = UIFrameworkAdapter.is_customtkinter_available()

# Create global UI adapter instance
ui_adapter = UIFrameworkAdapter()
ui_adapter.initialize_framework("dark", "blue")


class DataForgeApp:
    """Main DataForge Application with modular architecture"""
    
    def __init__(self):
        # Create main window
        self.root = ui_adapter.create_main_window()
        self.root.title("DataForge - Modern Data Management")
        self.root.geometry("1200x800")
        
        # Configure services
        configure_services()
        
        # Initialize services
        self.api_client = ApiClient(AppConfig.API_BASE_URL)
        self.data_generator = DataGenerator()
        self.async_runner = AsyncRunner(self.root)
        
        # Initialize plugin system directly
        from frontend.utils.error_handler import ErrorHandler
        self.plugin_manager = PluginManager(ErrorHandler())
        
        # Initialize controllers with proper parameters
        self.main_controller = MainWindowController(self.root)
        self.ui_controller = UIController(self)
        
        # UI state
        self.current_tab = "home"
        self._previous_tab = None
        self.sidebar_buttons = {}
        self.content_frames = {}
        self.exploration_data = []
        
        # Initialize tabs
        self.tabs = {
            "home": HomeTab(self),
            "database": DatabaseTab(self), 
            "exploration": ExplorationTab(self),
            "plugins": PluginsTab(self),
            "help": HelpTab(self)
        }
        
        # Build the UI
        self._build_layout()
        
        # Initialize with home tab
        self._show_tab("home")
    
    def _build_layout(self):
        """Build the main application layout"""
        # Main container
        self.main_frame = ui_adapter.create_frame(self.root)
        self.main_frame.pack(fill="both", expand=True)
        
        # Build sidebar and content area
        self._build_sidebar()
        self._build_content_area()
        self._build_log_section()
    
    def _build_sidebar(self):
        """Build the navigation sidebar"""
        self.sidebar = ui_adapter.create_frame(self.main_frame, width=200)
        self.sidebar.pack(side="left", fill="y", padx=(10, 5), pady=10)
        self.sidebar.pack_propagate(False)
        
        # Logo/Title
        title_frame = ui_adapter.create_frame(self.sidebar)
        title_frame.pack(fill="x", pady=(10, 20))
        
        ui_adapter.create_label(
            title_frame,
            text="🔧 DataForge",
            font_size=18,
            font_weight="bold"
        ).pack()
        
        ui_adapter.create_label(
            title_frame,
            text="Modern Data Platform",
            font_size=10
        ).pack()
        
        # Navigation buttons
        nav_buttons = [
            ("🏠 Home", "home"),
            ("🗄️ Database", "database"), 
            ("📊 Exploration", "exploration"),
            ("🔌 Plugins", "plugins"),
            ("❓ Help", "help")
        ]
        
        for text, tab_id in nav_buttons:
            btn = ui_adapter.create_button(
                self.sidebar,
                text=text,
                command=lambda t=tab_id: self._show_tab(t),
                width=180,
                height=40
            )
            btn.pack(pady=5, padx=10, fill="x")
            self.sidebar_buttons[tab_id] = btn
        
        # Back button (initially hidden)
        self.back_button = ui_adapter.create_button(
            self.sidebar,
            text="← Back",
            command=self._handle_back_action,
            width=180,
            height=30
        )
        # Don't pack initially - it will be shown when needed
    
    def _build_content_area(self):
        """Build the main content area"""
        self.content_area = ui_adapter.create_frame(self.main_frame)
        self.content_area.pack(side="left", fill="both", expand=True, padx=(5, 10), pady=10)
    
    def _build_log_section(self):
        """Build the bottom log section"""
        log_frame = ui_adapter.create_frame(self.root, height=120)
        log_frame.pack(side="bottom", fill="x", padx=10, pady=(0, 10))
        log_frame.pack_propagate(False)
        
        # Log header
        log_header = ui_adapter.create_frame(log_frame)
        log_header.pack(fill="x", pady=(5, 0))
        
        ui_adapter.create_label(
            log_header,
            text="📝 Application Log",
            font_weight="bold"
        ).pack(side="left")
        
        # Clear logs button
        ui_adapter.create_button(
            log_header,
            text="Clear",
            command=self._clear_logs,
            width=60,
            height=25
        ).pack(side="right", padx=5)
        
        # Log text area
        self.log_text = ui_adapter.create_textbox(log_frame)
        self.log_text.pack(fill="both", expand=True, padx=5, pady=5)
    
    def _show_tab(self, tab_id: str):
        """Show the specified tab"""
        # Clear content area
        for widget in self.content_area.winfo_children():
            widget.destroy()
        
        # Store previous tab
        if self.current_tab != tab_id:
            self._previous_tab = self.current_tab
        
        # Update current tab
        self.current_tab = tab_id
        
        # Update sidebar button states
        for btn_id, btn in self.sidebar_buttons.items():
            current_text = btn.cget('text')
            if btn_id == tab_id:
                # Add selection indicator if not already present
                if not current_text.startswith("▶ "):
                    btn.configure(text=f"▶ {current_text}")
            else:
                # Remove selection indicator if present
                if current_text.startswith("▶ "):
                    btn.configure(text=current_text[2:])
        
        # Show the tab content
        if tab_id in self.tabs:
            self.tabs[tab_id].build_content(self.content_area)
        
        self._log(f"Switched to {tab_id} tab")
    
    def _show_back_button(self, text="← Back", action=None):
        """Show the back button in sidebar"""
        self.back_button.configure(text=text)
        if action:
            self.back_button.configure(command=action)
        self.back_button.pack(side="bottom", pady=10, padx=10, fill="x")
    
    def _hide_back_button(self):
        """Hide the back button"""
        self.back_button.pack_forget()
    
    def _handle_back_action(self):
        """Handle back button action"""
        # Default back action - return to previous tab
        if hasattr(self, '_previous_tab') and self._previous_tab:
            self._show_tab(self._previous_tab)
        else:
            self._show_tab("home")
        self._hide_back_button()
    
    # Logging and status methods
    def _log(self, text: str):
        """Add message to application log"""
        from datetime import datetime
        timestamp = f"[{datetime.now().strftime('%H:%M:%S')}]"
        message = f"{timestamp} {text}\n"
        
        if hasattr(self, 'log_text'):
            self.log_text.insert("end", message)
            self.log_text.see("end")
    
    def _clear_logs(self):
        """Clear the application log"""
        if hasattr(self, 'log_text'):
            self.log_text.delete("1.0", "end")
        self._log("Logs cleared")
    
    def _status(self, text: str):
        """Show status message (same as log for now)"""
        self._log(f"Status: {text}")
    
    # Getters for services (used by tabs)
    def get_api_client(self) -> ApiClient:
        """Get API client instance"""
        return self.api_client
    
    def get_data_generator(self) -> DataGenerator:
        """Get data generator instance"""
        return self.data_generator
    
    def get_async_runner(self) -> AsyncRunner:
        """Get async runner instance"""
        return self.async_runner
    
    def get_plugin_manager(self) -> PluginManager:
        """Get plugin manager instance"""
        return self.plugin_manager
    
    def get_ui_adapter(self) -> UIFrameworkAdapter:
        """Get UI adapter instance"""
        return ui_adapter
    
    # Data management methods
    def set_exploration_data(self, data: List[Dict]):
        """Set data for exploration"""
        self.exploration_data = data
        self._log(f"Loaded {len(data)} records for exploration")
    
    def get_exploration_data(self) -> List[Dict]:
        """Get current exploration data"""
        return self.exploration_data
    
    def run(self):
        """Start the application"""
        # Log startup message
        self._log("🚀 DataForge Frontend started!")
        self._log(f"📡 API Endpoint: {AppConfig.API_BASE_URL}")
        self._log(f"📋 Default Schema: {AppConfig.DEFAULT_SCHEMA}")
        self._log("Welcome! Use the sidebar to navigate between different features.")
        
        # Start the main loop
        self.root.mainloop()


def main():
    """Main entry point for the DataForge frontend application"""
    app = DataForgeApp()
    app.run()


if __name__ == "__main__":
    main()
