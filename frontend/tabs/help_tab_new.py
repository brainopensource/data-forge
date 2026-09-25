"""
Help Tab for DataForge Application
"""
from .base_tab import BaseTab


class HelpTab(BaseTab):
    """Help tab with documentation and support information"""
    
    def build_content(self, parent):
        """Build the help tab content"""
        self.content_frame = self.ui_adapter.create_frame(parent)
        self.content_frame.pack(fill="both", expand=True)
        
        # Header
        header_frame = self.ui_adapter.create_frame(self.content_frame)
        header_frame.pack(fill="x", pady=20)
        
        self.ui_adapter.create_label(
            header_frame,
            text="❓ Help & Documentation",
            font_size=20,
            font_weight="bold"
        ).pack()
        
        self.ui_adapter.create_label(
            header_frame,
            text="Get help and learn about DataForge features",
            font_size=12
        ).pack(pady=(5, 0))
        
        # Help content
        content_frame = self.ui_adapter.create_frame(self.content_frame)
        content_frame.pack(fill="both", expand=True, pady=20, padx=50)
        
        # Quick start section
        quickstart_frame = self.ui_adapter.create_frame(content_frame)
        quickstart_frame.pack(fill="x", pady=20)
        
        self.ui_adapter.create_label(
            quickstart_frame,
            text="🚀 Quick Start Guide",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        help_text = """1. 🗄️ Database Tab: Generate sample data or connect to your backend
2. 📊 Exploration Tab: Load data and use table or plot explorers
3. 🔌 Plugins Tab: Manage available plugins and extensions
4. 🏠 Home Tab: Quick access to all features

Tips:
• Use the sidebar to navigate between different sections
• Check the log panel at the bottom for status messages
• All operations run in the background to keep the UI responsive"""
        
        self.ui_adapter.create_label(
            quickstart_frame,
            text=help_text,
            font_size=11,
            wraplength=600
        ).pack(anchor="w")
        
        # Features section
        features_frame = self.ui_adapter.create_frame(content_frame)
        features_frame.pack(fill="x", pady=20)
        
        self.ui_adapter.create_label(
            features_frame,
            text="✨ Key Features",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        features_text = """📈 Data Exploration: Advanced table view with filtering and pagination
📊 Visualization: Interactive plotting with matplotlib integration
🔌 Plugin System: Extensible architecture for custom functionality
🎨 Modern UI: Dark theme with CustomTkinter or tkinter fallback
⚡ Async Operations: Non-blocking operations for better performance
💾 Data Export: Export filtered data to CSV or JSON formats"""
        
        self.ui_adapter.create_label(
            features_frame,
            text=features_text,
            font_size=11,
            wraplength=600
        ).pack(anchor="w")
        
        # System info section
        system_frame = self.ui_adapter.create_frame(content_frame)
        system_frame.pack(fill="x", pady=20)
        
        self.ui_adapter.create_label(
            system_frame,
            text="🔧 System Information",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        # Display system info
        api_url = self.app.get_api_client().base_url
        ui_framework = self.ui_adapter.get_framework_name()
        
        system_info = f"""API Endpoint: {api_url}
UI Framework: {ui_framework}
Architecture: CQRS with modular components
Plugin System: Active"""
        
        self.ui_adapter.create_label(
            system_frame,
            text=system_info,
            font_size=11,
            wraplength=600
        ).pack(anchor="w")
        
        # Troubleshooting section
        trouble_frame = self.ui_adapter.create_frame(content_frame)
        trouble_frame.pack(fill="x", pady=20)
        
        self.ui_adapter.create_label(
            trouble_frame,
            text="🔨 Troubleshooting",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        trouble_text = """Common Issues:
• API Connection Failed: Check if the backend server is running
• Plugin Not Loading: Verify plugin structure and manifest.json
• Data Export Failed: Ensure you have write permissions to the target directory
• UI Rendering Issues: Try restarting the application

Check the application log (bottom panel) for detailed error messages."""
        
        self.ui_adapter.create_label(
            trouble_frame,
            text=trouble_text,
            font_size=11,
            wraplength=600
        ).pack(anchor="w")
