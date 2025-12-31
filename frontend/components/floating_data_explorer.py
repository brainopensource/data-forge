"""
Floating Data Explorer Component
"""
import tkinter as tk
from tkinter import ttk
from typing import List, Dict, Any
import math

from frontend.services.ui_framework_adapter import UIFrameworkAdapter
from .base_component import BaseComponent


class FloatingDataExplorer(BaseComponent):
    """Floating data explorer with pagination support"""
    
    def __init__(self, parent_window, data, filter_info="", component_id=None):
        # Create floating window
        self.floating_window = tk.Toplevel(parent_window)
        super().__init__(self.floating_window, component_id)
        
        self.data = data
        self.filter_info = filter_info
        self.page_size = 50
        self.current_page = 1
        
        # Initialize UI adapter
        self.ui_adapter = UIFrameworkAdapter()
        
        self._build_floating_explorer()
    
    def _build_floating_explorer(self):
        """Build the floating explorer interface"""
        self.floating_window.title(f"Data Explorer - {len(self.data)} records")
        self.floating_window.geometry("1000x600")
        self.floating_window.configure(bg='#2b2b2b')
        
        # Make it modal
        self.floating_window.transient()
        self.floating_window.grab_set()
        
        # Header with filter info
        if self.filter_info:
            header_frame = self.ui_adapter.create_frame(self.floating_window)
            header_frame.pack(fill="x", padx=10, pady=5)
            
            self.ui_adapter.create_label(
                header_frame,
                text=f"Filtered Data: {self.filter_info}",
                font_size=12,
                font_weight="bold"
            ).pack(side="left")
        
        # Table area
        self._build_table(self.floating_window)
        
        # Pagination controls
        self._build_pagination_controls(self.floating_window)
        
        # Load first page
        self._load_page()
    
    def _build_table(self, parent):
        """Build the data table"""
        table_frame = self.ui_adapter.create_frame(parent)
        table_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Create treeview for table
        columns = list(self.data[0].keys()) if self.data else []
        self.tree = ttk.Treeview(table_frame, columns=columns, show='headings')
        
        # Configure column headings
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=100, anchor="w")
        
        # Add scrollbars
        v_scrollbar = tk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.tree.yview)
        h_scrollbar = tk.Scrollbar(table_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        # Pack table and scrollbars
        self.tree.pack(side="left", fill="both", expand=True)
        v_scrollbar.pack(side="right", fill="y")
        h_scrollbar.pack(side="bottom", fill="x")
    
    def _build_pagination_controls(self, parent):
        """Build pagination control panel"""
        pagination_frame = self.ui_adapter.create_frame(parent)
        pagination_frame.pack(fill="x", padx=10, pady=5)
        
        # Navigation buttons
        nav_frame = self.ui_adapter.create_frame(pagination_frame)
        nav_frame.pack(side="left")
        
        self.first_btn = self.ui_adapter.create_button(
            nav_frame,
            text="⏮ First",
            command=self._first_page,
            width=80
        )
        self.first_btn.pack(side="left", padx=2)
        
        self.prev_btn = self.ui_adapter.create_button(
            nav_frame,
            text="◀ Prev",
            command=self._prev_page,
            width=80
        )
        self.prev_btn.pack(side="left", padx=2)
        
        self.next_btn = self.ui_adapter.create_button(
            nav_frame,
            text="Next ▶",
            command=self._next_page,
            width=80
        )
        self.next_btn.pack(side="left", padx=2)
        
        self.last_btn = self.ui_adapter.create_button(
            nav_frame,
            text="Last ⏭",
            command=self._last_page,
            width=80
        )
        self.last_btn.pack(side="left", padx=2)
        
        # Page info
        info_frame = self.ui_adapter.create_frame(pagination_frame)
        info_frame.pack(side="right")
        
        self.page_info = self.ui_adapter.create_label(info_frame, text="")
        self.page_info.pack(side="right", padx=10)
        
        # Page size selector
        self.ui_adapter.create_label(info_frame, text="Page size:").pack(side="right", padx=5)
        
        self.page_size_var = tk.StringVar(value=str(self.page_size))
        page_size_combo = self.ui_adapter.create_combobox(
            info_frame,
            values=["25", "50", "100", "200"],
            variable=self.page_size_var,
            command=self._on_page_size_change,
            width=80
        )
        page_size_combo.pack(side="right", padx=5)
    
    def _load_page(self):
        """Load current page data into table"""
        # Clear existing data
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        if not self.data:
            return
        
        # Calculate page bounds
        start_idx = (self.current_page - 1) * self.page_size
        end_idx = min(start_idx + self.page_size, len(self.data))
        
        # Load page data
        page_data = self.data[start_idx:end_idx]
        
        for i, record in enumerate(page_data):
            values = [str(record.get(col, "")) for col in self.tree["columns"]]
            self.tree.insert("", "end", values=values)
        
        # Update pagination info and navigation
        self._update_pagination_info()
        self._update_navigation_buttons()
    
    def _update_pagination_info(self):
        """Update pagination information display"""
        if not self.data:
            info_text = "No data"
        else:
            total_pages = math.ceil(len(self.data) / self.page_size)
            start_idx = (self.current_page - 1) * self.page_size + 1
            end_idx = min(start_idx + self.page_size - 1, len(self.data))
            info_text = f"Page {self.current_page} of {total_pages} | Showing {start_idx}-{end_idx} of {len(self.data)}"
        
        self.page_info.configure(text=info_text)
    
    def _update_navigation_buttons(self):
        """Update navigation button states"""
        if not self.data:
            return
        
        total_pages = math.ceil(len(self.data) / self.page_size)
        
        # Enable/disable buttons based on current page
        first_prev_state = "normal" if self.current_page > 1 else "disabled"
        next_last_state = "normal" if self.current_page < total_pages else "disabled"
        
        self.first_btn.configure(state=first_prev_state)
        self.prev_btn.configure(state=first_prev_state)
        self.next_btn.configure(state=next_last_state)
        self.last_btn.configure(state=next_last_state)
    
    def _first_page(self):
        """Go to first page"""
        self.current_page = 1
        self._load_page()
    
    def _prev_page(self):
        """Go to previous page"""
        if self.current_page > 1:
            self.current_page -= 1
            self._load_page()
    
    def _next_page(self):
        """Go to next page"""
        total_pages = math.ceil(len(self.data) / self.page_size)
        if self.current_page < total_pages:
            self.current_page += 1
            self._load_page()
    
    def _last_page(self):
        """Go to last page"""
        total_pages = math.ceil(len(self.data) / self.page_size)
        self.current_page = total_pages
        self._load_page()
    
    def _on_page_size_change(self, event=None):
        """Handle page size change"""
        try:
            new_size = int(self.page_size_var.get())
            if new_size > 0:
                self.page_size = new_size
                # Adjust current page to maintain position
                self.current_page = max(1, min(self.current_page, math.ceil(len(self.data) / self.page_size)))
                self._load_page()
        except ValueError:
            # Reset to previous value if invalid
            self.page_size_var.set(str(self.page_size))
