"""
Plugins Tab for DataForge Application
"""
from .base_tab import BaseTab
from tkinter import messagebox


class PluginsTab(BaseTab):
    """Plugins tab for plugin management"""
    
    def build_content(self, parent):
        """Build the plugins tab content"""
        self.content_frame = self.ui_adapter.create_frame(parent)
        self.content_frame.pack(fill="both", expand=True)
        
        # Header
        header_frame = self.ui_adapter.create_frame(self.content_frame)
        header_frame.pack(fill="x", pady=20)
        
        self.ui_adapter.create_label(
            header_frame,
            text="🔌 Plugin Management",
            font_size=20,
            font_weight="bold"
        ).pack()
        
        self.ui_adapter.create_label(
            header_frame,
            text="Manage and configure application plugins",
            font_size=12
        ).pack(pady=(5, 0))
        
        # Plugin discovery section
        discovery_frame = self.ui_adapter.create_frame(self.content_frame)
        discovery_frame.pack(fill="x", pady=20, padx=50)
        
        self.ui_adapter.create_label(
            discovery_frame,
            text="Plugin Discovery",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        self.ui_adapter.create_button(
            discovery_frame,
            text="🔍 Discover Plugins",
            command=self._discover_plugins,
            width=200,
            height=40
        ).pack(anchor="w")
        
        # Plugin list section
        list_frame = self.ui_adapter.create_frame(self.content_frame)
        list_frame.pack(fill="both", expand=True, pady=20, padx=50)
        
        self.ui_adapter.create_label(
            list_frame,
            text="Available Plugins",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        # Plugin list will be populated here
        self.plugin_list_frame = self.ui_adapter.create_frame(list_frame)
        self.plugin_list_frame.pack(fill="both", expand=True)
        
        # Load plugins on initialization
        self._load_plugins()
    
    def _discover_plugins(self):
        """Discover available plugins"""
        try:
            plugin_manager = self.app.get_plugin_manager()
            discovered = plugin_manager.discover_plugins()
            
            self._log(f"🔍 Discovered {len(discovered)} plugins")
            
            # Refresh the plugin list
            self._load_plugins()
            
            messagebox.showinfo("Plugin Discovery", f"Discovered {len(discovered)} plugins")
            
        except Exception as e:
            self._log(f"❌ Error discovering plugins: {e}")
            messagebox.showerror("Error", f"Failed to discover plugins: {e}")
    
    def _load_plugins(self):
        """Load and display plugin list"""
        # Clear existing plugins
        for widget in self.plugin_list_frame.winfo_children():
            widget.destroy()
        
        try:
            plugin_manager = self.app.get_plugin_manager()
            discovered = plugin_manager.discover_plugins()
            
            if not discovered:
                self.ui_adapter.create_label(
                    self.plugin_list_frame,
                    text="No plugins found. Plugins should be placed in the 'plugins' directory.",
                    font_size=12
                ).pack(pady=20)
                return
            
            # Display each plugin
            for manifest in discovered:
                plugin_frame = self.ui_adapter.create_frame(self.plugin_list_frame)
                plugin_frame.pack(fill="x", pady=5, padx=10)
                
                # Plugin info
                info_frame = self.ui_adapter.create_frame(plugin_frame)
                info_frame.pack(side="left", fill="x", expand=True)
                
                # Plugin name and version
                name_text = f"{manifest.info.name} v{manifest.info.version}"
                self.ui_adapter.create_label(
                    info_frame,
                    text=name_text,
                    font_size=14,
                    font_weight="bold"
                ).pack(anchor="w")
                
                # Plugin description
                if hasattr(manifest.info, 'description'):
                    self.ui_adapter.create_label(
                        info_frame,
                        text=manifest.info.description,
                        font_size=10
                    ).pack(anchor="w")
                
                # Plugin type
                type_text = f"Type: {manifest.info.plugin_type.value}"
                self.ui_adapter.create_label(
                    info_frame,
                    text=type_text,
                    font_size=10
                ).pack(anchor="w")
                
                # Plugin actions
                actions_frame = self.ui_adapter.create_frame(plugin_frame)
                actions_frame.pack(side="right")
                
                self.ui_adapter.create_button(
                    actions_frame,
                    text="ℹ️ Info",
                    command=lambda m=manifest: self._show_plugin_info(m),
                    width=60,
                    height=30
                ).pack(side="right", padx=2)
                
        except Exception as e:
            self._log(f"❌ Error loading plugins: {e}")
            self.ui_adapter.create_label(
                self.plugin_list_frame,
                text=f"Error loading plugins: {e}",
                font_size=12
            ).pack(pady=20)
    
    def _show_plugin_info(self, manifest):
        """Show detailed plugin information"""
        info_text = f"""Plugin Information:

Name: {manifest.info.name}
Version: {manifest.info.version}
Type: {manifest.info.plugin_type.value}
Path: {manifest.plugin_path}

Capabilities:
"""
        
        if hasattr(manifest.info, 'capabilities'):
            for capability in manifest.info.capabilities:
                info_text += f"• {capability}\n"
        else:
            info_text += "• No capabilities listed\n"
        
        messagebox.showinfo(f"Plugin: {manifest.info.name}", info_text)
