"""
Home Tab for DataForge Application
"""
from .base_tab import BaseTab


class HomeTab(BaseTab):
    """Home tab with application overview and quick actions"""
    
    def build_content(self, parent):
        """Build the home tab content"""
        self.content_frame = self.ui_adapter.create_frame(parent)
        self.content_frame.pack(fill="both", expand=True)
        
        # Welcome header
        header_frame = self.ui_adapter.create_frame(self.content_frame)
        header_frame.pack(fill="x", pady=20)
        
        self.ui_adapter.create_label(
            header_frame,
            text="Welcome to DataForge",
            font_size=24,
            font_weight="bold"
        ).pack()
        
        self.ui_adapter.create_label(
            header_frame,
            text="Modern Data Management Platform",
            font_size=14
        ).pack(pady=(5, 0))
        
        # Quick actions section
        actions_frame = self.ui_adapter.create_frame(self.content_frame)
        actions_frame.pack(fill="x", pady=20, padx=50)
        
        self.ui_adapter.create_label(
            actions_frame,
            text="Quick Actions",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        # Action buttons in grid
        buttons_frame = self.ui_adapter.create_frame(actions_frame)
        buttons_frame.pack(fill="x")
        
        # Row 1
        row1 = self.ui_adapter.create_frame(buttons_frame)
        row1.pack(fill="x", pady=5)
        
        self.ui_adapter.create_button(
            row1,
            text="🗄️ Manage Database",
            command=lambda: self.app._show_tab("database"),
            width=200,
            height=50
        ).pack(side="left", padx=(0, 10))
        
        self.ui_adapter.create_button(
            row1,
            text="📊 Explore Data", 
            command=lambda: self.app._show_tab("exploration"),
            width=200,
            height=50
        ).pack(side="left", padx=(0, 10))
        
        # Status section showing basic info
        status_frame = self.ui_adapter.create_frame(self.content_frame)
        status_frame.pack(fill="x", pady=20, padx=50)
        
        self.ui_adapter.create_label(
            status_frame,
            text="System Status",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        # Basic status info
        self.ui_adapter.create_label(
            status_frame,
            text=f"🌐 API: Ready ({self.app.get_api_client().base_url})",
            font_size=12
        ).pack(anchor="w", pady=2)
        
        framework_name = self.ui_adapter.get_framework_name()
        self.ui_adapter.create_label(
            status_frame,
            text=f"🎨 UI Framework: {framework_name}",
            font_size=12
        ).pack(anchor="w", pady=2)
