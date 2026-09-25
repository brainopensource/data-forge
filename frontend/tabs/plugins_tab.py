"""
Plugin Management Tab for DataForge Frontend

This module provides a comprehensive UI for managing plugins including:
- Plugin discovery and installation
- Plugin loading/unloading
- Plugin configuration
- Plugin status monitoring
- Security validation
"""

from typing import List, Dict, Any, Optional
import tkinter as tk
from pathlib import Path
import json

from frontend.core.plugin_system import PluginManager, PluginType, PluginStatus
from frontend.utils.error_handler import ErrorHandler
from frontend.services.ui_framework_adapter import UIFrameworkAdapter


class PluginsTab:
    """Plugin management interface with modern UI design."""
    
    def __init__(self, parent, plugin_manager: PluginManager, error_handler: ErrorHandler, 
                 ui_adapter: UIFrameworkAdapter):
        self.parent = parent
        self.plugin_manager = plugin_manager
        self.error_handler = error_handler
        self.ui_adapter = ui_adapter
        
        self.tab_frame = None
        self.plugins_list_frame = None
        self.details_frame = None
        self.details_label = None
        self.selected_plugin = None
        
        # UI state
        self.plugin_rows = {}  # plugin_name -> row_frame mapping
        
        self._build_plugins_tab()
        self._setup_event_handlers()
    
    def _build_plugins_tab(self):
        """Build the plugins management tab."""
        # Main container
        self.tab_frame = self.ui_adapter.create_frame(self.parent)
        self.tab_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Title section
        self._build_title_section()
        
        # Action buttons
        self._build_action_buttons()
        
        # Main content area (split between list and details)
        self._build_main_content()
        
        # Status bar
        self._build_status_bar()
        
        # Initial data load
        self._refresh_plugin_list()
    
    def _build_title_section(self):
        """Build title and description section."""
        title_frame = self.ui_adapter.create_frame(self.tab_frame)
        title_frame.pack(fill="x", padx=10, pady=(10, 20))
        
        # Main title
        title_label = self.ui_adapter.create_label(
            title_frame,
            text="🔌 Plugin Management",
            font=("Arial", 18, "bold")
        )
        title_label.pack(anchor="w")
        
        # Subtitle
        subtitle_label = self.ui_adapter.create_label(
            title_frame,
            text="Manage and configure plugins to extend DataForge functionality",
            font=("Arial", 11)
        )
        subtitle_label.pack(anchor="w", pady=(5, 0))
    
    def _build_action_buttons(self):
        """Build action buttons row."""
        actions_frame = self.ui_adapter.create_frame(self.tab_frame)
        actions_frame.pack(fill="x", padx=10, pady=(0, 15))
        
        # Left side buttons
        left_buttons = self.ui_adapter.create_frame(actions_frame)
        left_buttons.pack(side="left")
        
        # Discover plugins button
        discover_btn = self.ui_adapter.create_button(
            left_buttons,
            text="🔍 Discover Plugins",
            command=self._discover_plugins,
            width=140
        )
        discover_btn.pack(side="left", padx=(0, 5))
        
        # Refresh button
        refresh_btn = self.ui_adapter.create_button(
            left_buttons,
            text="🔄 Refresh",
            command=self._refresh_plugin_list,
            width=100
        )
        refresh_btn.pack(side="left", padx=5)
        
        # Load all button
        load_all_btn = self.ui_adapter.create_button(
            left_buttons,
            text="⚡ Load All",
            command=self._load_all_plugins,
            width=100
        )
        load_all_btn.pack(side="left", padx=5)
        
        # Right side buttons
        right_buttons = self.ui_adapter.create_frame(actions_frame)
        right_buttons.pack(side="right")
        
        # Install from file button
        install_btn = self.ui_adapter.create_button(
            right_buttons,
            text="📦 Install from File",
            command=self._install_plugin_from_file,
            width=140
        )
        install_btn.pack(side="right", padx=5)
        
        # Create plugin button
        create_btn = self.ui_adapter.create_button(
            right_buttons,
            text="⚒️ Create Plugin",
            command=self._create_plugin_wizard,
            width=120
        )
        create_btn.pack(side="right", padx=5)
    
    def _build_main_content(self):
        """Build main content area with plugin list and details."""
        content_frame = self.ui_adapter.create_frame(self.tab_frame)
        content_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Left panel - Plugin list
        self._build_plugin_list_panel(content_frame)
        
        # Right panel - Plugin details
        self._build_details_panel(content_frame)
    
    def _build_plugin_list_panel(self, parent):
        """Build plugin list panel."""
        list_panel = self.ui_adapter.create_frame(parent)
        list_panel.pack(side="left", fill="both", expand=True, padx=(0, 10))
        
        # List header
        header_frame = self.ui_adapter.create_frame(list_panel)
        header_frame.pack(fill="x", padx=5, pady=(5, 10))
        
        header_label = self.ui_adapter.create_label(
            header_frame,
            text="📋 Available Plugins",
            font=("Arial", 14, "bold")
        )
        header_label.pack(side="left")
        
        # Plugin count label
        self.plugin_count_label = self.ui_adapter.create_label(
            header_frame,
            text="",
            font=("Arial", 10)
        )
        self.plugin_count_label.pack(side="right")
        
        # Column headers
        columns_frame = self.ui_adapter.create_frame(list_panel)
        columns_frame.pack(fill="x", padx=5, pady=(0, 5))
        
        headers = [
            ("Name", 0.25),
            ("Version", 0.1),
            ("Type", 0.15),
            ("Status", 0.15),
            ("Actions", 0.35)
        ]
        
        for header, weight in headers:
            label = self.ui_adapter.create_label(
                columns_frame, 
                text=header, 
                font=("Arial", 10, "bold")
            )
            label.pack(side="left", fill="x", expand=bool(weight), padx=5)
        
        # Scrollable plugin list
        self.plugins_list_frame = self.ui_adapter.create_scrollable_frame(list_panel)
        self.plugins_list_frame.pack(fill="both", expand=True, padx=5, pady=5)
    
    def _build_details_panel(self, parent):
        """Build plugin details panel."""
        details_panel = self.ui_adapter.create_frame(parent, width=350)
        details_panel.pack(side="right", fill="y")
        details_panel.pack_propagate(False)
        
        # Details header
        header_frame = self.ui_adapter.create_frame(details_panel)
        header_frame.pack(fill="x", padx=10, pady=(10, 15))
        
        header_label = self.ui_adapter.create_label(
            header_frame,
            text="📄 Plugin Details",
            font=("Arial", 14, "bold")
        )
        header_label.pack(anchor="w")
        
        # Details content (scrollable)
        self.details_frame = self.ui_adapter.create_scrollable_frame(details_panel)
        self.details_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Default message
        self._show_no_plugin_selected()
    
    def _build_status_bar(self):
        """Build status bar at bottom."""
        status_frame = self.ui_adapter.create_frame(self.tab_frame)
        status_frame.pack(fill="x", padx=10, pady=(10, 5))
        
        self.status_label = self.ui_adapter.create_label(
            status_frame,
            text="Ready",
            font=("Arial", 9)
        )
        self.status_label.pack(side="left")
        
        # Plugin system info
        info_label = self.ui_adapter.create_label(
            status_frame,
            text=f"Plugin System v1.0 | Directories: {len(self.plugin_manager.registry.plugin_directories)}",
            font=("Arial", 9)
        )
        info_label.pack(side="right")
    
    def _setup_event_handlers(self):
        """Setup plugin system event handlers."""
        self.plugin_manager.add_event_handler('plugin_loaded', self._on_plugin_loaded)
        self.plugin_manager.add_event_handler('plugin_unloaded', self._on_plugin_unloaded)
    
    def _discover_plugins(self):
        """Discover available plugins."""
        try:
            self._update_status("Discovering plugins...")
            discovered = self.plugin_manager.discover_plugins()
            
            count = len(discovered)
            self._show_message(f"✅ Discovered {count} plugin(s)", "success")
            self._update_status(f"Discovered {count} plugins")
            self._refresh_plugin_list()
            
        except Exception as e:
            self.error_handler.handle_error(e, "Failed to discover plugins")
            self._show_message("❌ Plugin discovery failed", "error")
            self._update_status("Plugin discovery failed")
    
    def _refresh_plugin_list(self):
        """Refresh the plugin list display."""
        # Clear existing list
        for child in self.plugins_list_frame.winfo_children():
            child.destroy()
        
        self.plugin_rows.clear()
        
        # Get all available plugins
        available_plugins = list(self.plugin_manager.registry.manifests.items())
        
        # Sort by name
        available_plugins.sort(key=lambda x: x[0].lower())
        
        # Add plugins to list
        for plugin_name, manifest in available_plugins:
            self._add_plugin_row(plugin_name, manifest)
        
        # Update count
        total_count = len(available_plugins)
        loaded_count = len(self.plugin_manager.loaded_plugins)
        self.plugin_count_label.configure(text=f"{loaded_count}/{total_count} loaded")
        
        if not available_plugins:
            self._show_empty_plugin_list()
    
    def _add_plugin_row(self, plugin_name: str, manifest):
        """Add a plugin row to the list."""
        plugin_frame = self.ui_adapter.create_frame(self.plugins_list_frame)
        plugin_frame.pack(fill="x", padx=2, pady=1)
        
        # Store reference
        self.plugin_rows[plugin_name] = plugin_frame
        
        # Plugin info
        info_frame = self.ui_adapter.create_frame(plugin_frame)
        info_frame.pack(fill="x", padx=5, pady=5)
        
        # Name (clickable)
        name_label = self.ui_adapter.create_label(
            info_frame, 
            text=manifest.info.name,
            font=("Arial", 10, "bold"),
            cursor="hand2"
        )
        name_label.pack(side="left", fill="x", expand=True, anchor="w")
        name_label.bind("<Button-1>", lambda e: self._select_plugin(plugin_name, manifest))
        
        # Version
        version_label = self.ui_adapter.create_label(
            info_frame, 
            text=manifest.info.version,
            font=("Arial", 9),
            width=10
        )
        version_label.pack(side="left", padx=5)
        
        # Type with icon
        type_icon = self._get_type_icon(manifest.info.plugin_type)
        type_label = self.ui_adapter.create_label(
            info_frame, 
            text=f"{type_icon} {manifest.info.plugin_type.value}",
            font=("Arial", 9),
            width=15
        )
        type_label.pack(side="left", padx=5)
        
        # Status
        status = self.plugin_manager.get_plugin_status(plugin_name)
        status_frame = self.ui_adapter.create_frame(info_frame)
        status_frame.pack(side="left", padx=5)
        
        status_label = self.ui_adapter.create_label(
            status_frame,
            text=self._get_status_text(status),
            font=("Arial", 9),
            width=12
        )
        status_label.pack()
        
        # Action buttons
        self._build_plugin_actions(info_frame, plugin_name, status)
    
    def _build_plugin_actions(self, parent, plugin_name: str, status: PluginStatus):
        """Build action buttons for plugin."""
        actions_frame = self.ui_adapter.create_frame(parent)
        actions_frame.pack(side="right", padx=5)
        
        if status == PluginStatus.ACTIVE:
            # Unload button
            unload_btn = self.ui_adapter.create_button(
                actions_frame,
                text="⏹️ Unload",
                command=lambda: self._unload_plugin(plugin_name),
                width=70,
                height=25
            )
            unload_btn.pack(side="left", padx=2)
            
            # Reload button
            reload_btn = self.ui_adapter.create_button(
                actions_frame,
                text="🔄 Reload",
                command=lambda: self._reload_plugin(plugin_name),
                width=70,
                height=25
            )
            reload_btn.pack(side="left", padx=2)
            
        else:
            # Load button
            load_btn = self.ui_adapter.create_button(
                actions_frame,
                text="▶️ Load",
                command=lambda: self._load_plugin(plugin_name),
                width=70,
                height=25
            )
            load_btn.pack(side="left", padx=2)
        
        # Configure button (always available)
        config_btn = self.ui_adapter.create_button(
            actions_frame,
            text="⚙️",
            command=lambda: self._configure_plugin(plugin_name),
            width=30,
            height=25
        )
        config_btn.pack(side="left", padx=2)
    
    def _show_empty_plugin_list(self):
        """Show message when no plugins are available."""
        empty_frame = self.ui_adapter.create_frame(self.plugins_list_frame)
        empty_frame.pack(fill="both", expand=True, padx=20, pady=50)
        
        # Icon
        icon_label = self.ui_adapter.create_label(
            empty_frame,
            text="🔌",
            font=("Arial", 48)
        )
        icon_label.pack(pady=(0, 15))
        
        # Message
        message_label = self.ui_adapter.create_label(
            empty_frame,
            text="No plugins found",
            font=("Arial", 14, "bold")
        )
        message_label.pack(pady=(0, 5))
        
        # Instruction
        instruction_label = self.ui_adapter.create_label(
            empty_frame,
            text="Click 'Discover Plugins' to search for available plugins\nor 'Install from File' to add a new plugin",
            font=("Arial", 10)
        )
        instruction_label.pack()
    
    def _select_plugin(self, plugin_name: str, manifest):
        """Select plugin and show details."""
        self.selected_plugin = plugin_name
        self._show_plugin_details(plugin_name, manifest)
        
        # Update visual selection
        for name, row_frame in self.plugin_rows.items():
            if hasattr(row_frame, 'configure'):
                if name == plugin_name:
                    # Highlight selected
                    try:
                        row_frame.configure(fg_color=("#E3F2FD", "#1565C0"))  # Light blue highlight
                    except:
                        row_frame.configure(bg="#E3F2FD")
                else:
                    # Reset to normal
                    try:
                        row_frame.configure(fg_color=("gray90", "gray20"))
                    except:
                        row_frame.configure(bg="white")
    
    def _show_plugin_details(self, plugin_name: str, manifest):
        """Show detailed plugin information."""
        # Clear existing details
        for child in self.details_frame.winfo_children():
            child.destroy()
        
        # Plugin header
        header_frame = self.ui_adapter.create_frame(self.details_frame)
        header_frame.pack(fill="x", pady=(0, 15))
        
        # Plugin name and type
        name_label = self.ui_adapter.create_label(
            header_frame,
            text=manifest.info.name,
            font=("Arial", 14, "bold")
        )
        name_label.pack(anchor="w")
        
        type_icon = self._get_type_icon(manifest.info.plugin_type)
        type_label = self.ui_adapter.create_label(
            header_frame,
            text=f"{type_icon} {manifest.info.plugin_type.value.title()} Plugin",
            font=("Arial", 10)
        )
        type_label.pack(anchor="w", pady=(2, 0))
        
        # Status indicator
        status = self.plugin_manager.get_plugin_status(plugin_name)
        status_frame = self.ui_adapter.create_frame(header_frame)
        status_frame.pack(fill="x", pady=(5, 0))
        
        status_label = self.ui_adapter.create_label(
            status_frame,
            text=f"Status: {self._get_status_text(status)}",
            font=("Arial", 10, "bold")
        )
        status_label.pack(side="left")
        
        # Details sections
        self._add_detail_section("📋 Information", {
            "Version": manifest.info.version,
            "Author": manifest.info.author,
            "Description": manifest.info.description,
            "License": manifest.info.license or "Not specified",
        })
        
        self._add_detail_section("🔧 Technical", {
            "Entry Point": manifest.info.entry_point,
            "Min App Version": manifest.info.min_app_version,
            "Path": str(manifest.path),
            "Dependencies": ", ".join(manifest.info.dependencies) if manifest.info.dependencies else "None",
        })
        
        self._add_detail_section("🔒 Security", {
            "Permissions": ", ".join(manifest.info.permissions) if manifest.info.permissions else "None",
            "Checksum": manifest.checksum[:16] + "..." if manifest.checksum else "Not verified",
        })
        
        if manifest.info.tags:
            self._add_detail_section("🏷️ Tags", {
                "Tags": ", ".join(manifest.info.tags)
            })
        
        # Links section
        if manifest.info.homepage or manifest.info.documentation:
            links = {}
            if manifest.info.homepage:
                links["Homepage"] = manifest.info.homepage
            if manifest.info.documentation:
                links["Documentation"] = manifest.info.documentation
            self._add_detail_section("🔗 Links", links)
        
        # Configuration section (if plugin is loaded)
        if status == PluginStatus.ACTIVE:
            plugin = self.plugin_manager.get_plugin(plugin_name)
            if plugin:
                config = plugin.get_configuration()
                if config:
                    config_items = {k: str(v) for k, v in config.items()}
                    self._add_detail_section("⚙️ Configuration", config_items)
    
    def _add_detail_section(self, title: str, items: Dict[str, str]):
        """Add a details section."""
        # Section header
        section_frame = self.ui_adapter.create_frame(self.details_frame)
        section_frame.pack(fill="x", pady=(10, 5))
        
        title_label = self.ui_adapter.create_label(
            section_frame,
            text=title,
            font=("Arial", 11, "bold")
        )
        title_label.pack(anchor="w")
        
        # Section content
        content_frame = self.ui_adapter.create_frame(self.details_frame)
        content_frame.pack(fill="x", padx=10, pady=(0, 5))
        
        for key, value in items.items():
            item_frame = self.ui_adapter.create_frame(content_frame)
            item_frame.pack(fill="x", pady=1)
            
            key_label = self.ui_adapter.create_label(
                item_frame,
                text=f"{key}:",
                font=("Arial", 9, "bold"),
                width=12
            )
            key_label.pack(side="left", anchor="nw")
            
            # Handle long values
            if len(str(value)) > 40:
                value_text = str(value)[:40] + "..."
            else:
                value_text = str(value)
            
            value_label = self.ui_adapter.create_label(
                item_frame,
                text=value_text,
                font=("Arial", 9),
                wraplength=200
            )
            value_label.pack(side="left", fill="x", expand=True, anchor="nw")
    
    def _show_no_plugin_selected(self):
        """Show message when no plugin is selected."""
        # Clear existing details
        for child in self.details_frame.winfo_children():
            child.destroy()
        
        placeholder_frame = self.ui_adapter.create_frame(self.details_frame)
        placeholder_frame.pack(fill="both", expand=True, padx=20, pady=50)
        
        # Icon
        icon_label = self.ui_adapter.create_label(
            placeholder_frame,
            text="📄",
            font=("Arial", 36)
        )
        icon_label.pack(pady=(0, 15))
        
        # Message
        message_label = self.ui_adapter.create_label(
            placeholder_frame,
            text="No plugin selected",
            font=("Arial", 12, "bold")
        )
        message_label.pack(pady=(0, 5))
        
        # Instruction
        instruction_label = self.ui_adapter.create_label(
            placeholder_frame,
            text="Click on a plugin name to view details",
            font=("Arial", 10)
        )
        instruction_label.pack()
    
    def _load_plugin(self, plugin_name: str):
        """Load a plugin."""
        try:
            self._update_status(f"Loading plugin: {plugin_name}")
            success = self.plugin_manager.load_plugin(plugin_name)
            
            if success:
                self._show_message(f"✅ Plugin '{plugin_name}' loaded successfully", "success")
                self._update_status(f"Plugin '{plugin_name}' loaded")
            else:
                self._show_message(f"❌ Failed to load plugin '{plugin_name}'", "error")
                self._update_status(f"Failed to load plugin '{plugin_name}'")
            
            self._refresh_plugin_row(plugin_name)
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Error loading plugin: {plugin_name}")
            self._show_message(f"❌ Error loading plugin '{plugin_name}'", "error")
    
    def _unload_plugin(self, plugin_name: str):
        """Unload a plugin."""
        try:
            self._update_status(f"Unloading plugin: {plugin_name}")
            success = self.plugin_manager.unload_plugin(plugin_name)
            
            if success:
                self._show_message(f"✅ Plugin '{plugin_name}' unloaded successfully", "success")
                self._update_status(f"Plugin '{plugin_name}' unloaded")
            else:
                self._show_message(f"❌ Failed to unload plugin '{plugin_name}'", "error")
                self._update_status(f"Failed to unload plugin '{plugin_name}'")
            
            self._refresh_plugin_row(plugin_name)
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Error unloading plugin: {plugin_name}")
            self._show_message(f"❌ Error unloading plugin '{plugin_name}'", "error")
    
    def _reload_plugin(self, plugin_name: str):
        """Reload a plugin."""
        try:
            self._update_status(f"Reloading plugin: {plugin_name}")
            success = self.plugin_manager.reload_plugin(plugin_name)
            
            if success:
                self._show_message(f"✅ Plugin '{plugin_name}' reloaded successfully", "success")
                self._update_status(f"Plugin '{plugin_name}' reloaded")
            else:
                self._show_message(f"❌ Failed to reload plugin '{plugin_name}'", "error")
                self._update_status(f"Failed to reload plugin '{plugin_name}'")
            
            self._refresh_plugin_row(plugin_name)
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Error reloading plugin: {plugin_name}")
            self._show_message(f"❌ Error reloading plugin '{plugin_name}'", "error")
    
    def _load_all_plugins(self):
        """Load all available plugins."""
        try:
            available_plugins = list(self.plugin_manager.registry.manifests.keys())
            loaded_count = 0
            failed_count = 0
            
            self._update_status("Loading all plugins...")
            
            for plugin_name in available_plugins:
                if self.plugin_manager.get_plugin_status(plugin_name) != PluginStatus.ACTIVE:
                    success = self.plugin_manager.load_plugin(plugin_name)
                    if success:
                        loaded_count += 1
                    else:
                        failed_count += 1
            
            self._show_message(
                f"✅ Loaded {loaded_count} plugins, {failed_count} failed", 
                "success" if failed_count == 0 else "warning"
            )
            self._update_status(f"Loaded {loaded_count} plugins")
            self._refresh_plugin_list()
            
        except Exception as e:
            self.error_handler.handle_error(e, "Error loading all plugins")
            self._show_message("❌ Error loading plugins", "error")
    
    def _configure_plugin(self, plugin_name: str):
        """Open plugin configuration dialog."""
        try:
            manifest = self.plugin_manager.registry.manifests.get(plugin_name)
            if not manifest:
                self._show_message(f"Plugin '{plugin_name}' not found", "error")
                return
            
            # For now, show a simple dialog
            self._show_configuration_dialog(plugin_name, manifest)
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Error configuring plugin: {plugin_name}")
            self._show_message(f"❌ Error configuring plugin '{plugin_name}'", "error")
    
    def _show_configuration_dialog(self, plugin_name: str, manifest):
        """Show plugin configuration dialog."""
        # Create configuration window
        config_window = tk.Toplevel(self.parent)
        config_window.title(f"Configure {plugin_name}")
        config_window.geometry("400x300")
        config_window.resizable(True, True)
        
        # Window content
        content_frame = self.ui_adapter.create_frame(config_window)
        content_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Header
        header_label = self.ui_adapter.create_label(
            content_frame,
            text=f"Configuration for {plugin_name}",
            font=("Arial", 14, "bold")
        )
        header_label.pack(pady=(0, 15))
        
        # Configuration message
        message_label = self.ui_adapter.create_label(
            content_frame,
            text="Plugin configuration UI will be implemented\nwhen the plugin provides a configuration schema.",
            font=("Arial", 11)
        )
        message_label.pack(pady=20)
        
        # Schema info
        if manifest.info.configuration_schema:
            schema_label = self.ui_adapter.create_label(
                content_frame,
                text="Configuration schema available",
                font=("Arial", 10, "bold")
            )
            schema_label.pack()
        
        # Close button
        close_btn = self.ui_adapter.create_button(
            content_frame,
            text="Close",
            command=config_window.destroy,
            width=100
        )
        close_btn.pack(pady=20)
    
    def _install_plugin_from_file(self):
        """Install plugin from file."""
        try:
            # File dialog to select plugin package
            from tkinter import filedialog
            
            file_path = filedialog.askopenfilename(
                title="Select Plugin Package",
                filetypes=[
                    ("ZIP files", "*.zip"),
                    ("TAR files", "*.tar.gz"),
                    ("All files", "*.*")
                ]
            )
            
            if file_path:
                self._show_message("Plugin installation will be implemented", "info")
                self._update_status("Plugin installation not yet implemented")
                
        except Exception as e:
            self.error_handler.handle_error(e, "Error installing plugin from file")
            self._show_message("❌ Error installing plugin", "error")
    
    def _create_plugin_wizard(self):
        """Open plugin creation wizard."""
        try:
            # Create wizard window
            wizard_window = tk.Toplevel(self.parent)
            wizard_window.title("Create New Plugin")
            wizard_window.geometry("500x400")
            wizard_window.resizable(True, True)
            
            # Wizard content
            content_frame = self.ui_adapter.create_frame(wizard_window)
            content_frame.pack(fill="both", expand=True, padx=20, pady=20)
            
            # Header
            header_label = self.ui_adapter.create_label(
                content_frame,
                text="🧙‍♂️ Plugin Creation Wizard",
                font=("Arial", 16, "bold")
            )
            header_label.pack(pady=(0, 20))
            
            # Message
            message_label = self.ui_adapter.create_label(
                content_frame,
                text="The plugin creation wizard will help you\ncreate a new plugin template with boilerplate code.",
                font=("Arial", 11)
            )
            message_label.pack(pady=20)
            
            # Coming soon
            coming_soon_label = self.ui_adapter.create_label(
                content_frame,
                text="🚧 Coming Soon 🚧\n\nThis feature will be implemented in a future update.",
                font=("Arial", 12, "bold")
            )
            coming_soon_label.pack(pady=30)
            
            # Close button
            close_btn = self.ui_adapter.create_button(
                content_frame,
                text="Close",
                command=wizard_window.destroy,
                width=100
            )
            close_btn.pack(pady=20)
            
        except Exception as e:
            self.error_handler.handle_error(e, "Error opening plugin creation wizard")
            self._show_message("❌ Error opening wizard", "error")
    
    def _refresh_plugin_row(self, plugin_name: str):
        """Refresh a specific plugin row."""
        if plugin_name in self.plugin_rows:
            # Remove old row
            self.plugin_rows[plugin_name].destroy()
            del self.plugin_rows[plugin_name]
            
            # Add updated row
            manifest = self.plugin_manager.registry.manifests.get(plugin_name)
            if manifest:
                self._add_plugin_row(plugin_name, manifest)
    
    def _get_type_icon(self, plugin_type: PluginType) -> str:
        """Get icon for plugin type."""
        icons = {
            PluginType.DATA_SOURCE: "🗄️",
            PluginType.VISUALIZATION: "📊", 
            PluginType.WORKFLOW: "⚙️",
            PluginType.UI_COMPONENT: "🎨",
            PluginType.EXPORT: "📤",
            PluginType.VALIDATION: "✅",
            PluginType.TRANSFORMATION: "🔄"
        }
        return icons.get(plugin_type, "🔌")
    
    def _get_status_text(self, status: PluginStatus) -> str:
        """Get text representation of plugin status."""
        status_texts = {
            PluginStatus.ACTIVE: "✅ Active",
            PluginStatus.INACTIVE: "⭕ Inactive",
            PluginStatus.ERROR: "❌ Error",
            PluginStatus.LOADING: "⏳ Loading",
            PluginStatus.UNLOADING: "⏳ Unloading",
            PluginStatus.DISABLED: "🚫 Disabled"
        }
        return status_texts.get(status, "❓ Unknown")
    
    def _update_status(self, message: str):
        """Update status bar message."""
        if hasattr(self, 'status_label'):
            self.status_label.configure(text=message)
    
    def _show_message(self, message: str, message_type: str = "info"):
        """Show status message."""
        # For now, just print to console
        # In a real implementation, this would show a toast notification
        print(f"[{message_type.upper()}] {message}")
        
        # Also update status bar
        self._update_status(message)
    
    def _on_plugin_loaded(self, plugin_name: str, plugin):
        """Handle plugin loaded event."""
        self._refresh_plugin_row(plugin_name)
        if self.selected_plugin == plugin_name:
            manifest = self.plugin_manager.registry.manifests.get(plugin_name)
            if manifest:
                self._show_plugin_details(plugin_name, manifest)
    
    def _on_plugin_unloaded(self, plugin_name: str):
        """Handle plugin unloaded event."""
        self._refresh_plugin_row(plugin_name)
        if self.selected_plugin == plugin_name:
            manifest = self.plugin_manager.registry.manifests.get(plugin_name)
            if manifest:
                self._show_plugin_details(plugin_name, manifest)
