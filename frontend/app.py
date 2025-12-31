"""
CustomTkinter MVP Frontend for DataForge

Modern Frontend Features:
- Dark theme with blue accents
- Modern left sidebar navigation with tabs
- Fixed API configuration 
- Upload/Download functionality
- Sync capabilities

Notes:
- Uses CustomTkinter dark + blue theme when available; falls back to tkinter.
- Long-running calls run in background threads to keep UI responsive.
- Modern sidebar navigation design
"""

from __future__ import annotations

import json
import threading
import time
import uuid
import random
import csv
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass
from enum import Enum

import requests
import importlib
import importlib.util

# Import our new utilities
from frontend.utils import StringUtils, ErrorHandler
from frontend.utils.data_type_detector import DataTypeDetector
from frontend.controllers import MainWindowController, UIController
from frontend.core.plugin_system import PluginManager

# PyArrow for IPC stream parsing from read endpoints
try:
	import pyarrow as pa
	import pyarrow.ipc as pa_ipc
except Exception:  # pragma: no cover - allow UI to start even if pyarrow missing
	pa = None  # type: ignore
	pa_ipc = None  # type: ignore

# Polars for robust CSV export
try:
	import polars as pl  # type: ignore
except Exception:  # pragma: no cover
	pl = None  # type: ignore


# Use UI Framework Adapter for consistent widget handling
from frontend.services.ui_framework_adapter import UIFrameworkAdapter

# Import plotting libraries
import matplotlib
matplotlib.use('TkAgg')  # Use tkinter backend
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
import pandas as pd

# Configure matplotlib for dark theme
plt.style.use('dark_background')

# Create global UI adapter instance for backward compatibility
ui_adapter = UIFrameworkAdapter()
ui_adapter.initialize_framework("dark", "blue")

# Create backward compatibility ctk object
class CompatCTK:
    """Backward compatibility for CTK usage."""
    
    # Import additional tkinter modules for fallback
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
    
    @staticmethod
    def CTk(**kwargs):
        return ui_adapter.create_main_window(**kwargs)
    
    @staticmethod
    def CTkFrame(parent, **kwargs):
        return ui_adapter.create_frame(parent, **kwargs)
    
    @staticmethod
    def CTkButton(parent, **kwargs):
        return ui_adapter.create_button(parent, **kwargs)
    
    @staticmethod
    def CTkLabel(parent, **kwargs):
        return ui_adapter.create_label(parent, **kwargs)
    
    @staticmethod
    def CTkEntry(parent, **kwargs):
        return ui_adapter.create_entry(parent, **kwargs)
    
    @staticmethod
    def CTkTextbox(parent, **kwargs):
        return ui_adapter.create_textbox(parent, **kwargs)
    
    @staticmethod
    def CTkComboBox(parent, **kwargs):
        return ui_adapter.create_combobox(parent, **kwargs)
    
    @staticmethod
    def CTkCheckBox(parent, **kwargs):
        return ui_adapter.create_checkbox(parent, **kwargs)
    
    @staticmethod
    def CTkProgressBar(parent, **kwargs):
        return ui_adapter.create_progressbar(parent, **kwargs)
    
    @staticmethod
    def CTkScrollableFrame(parent, **kwargs):
        return ui_adapter.create_scrollable_frame(parent, **kwargs)
    
    @staticmethod
    def CTkFont(**kwargs):
        return ui_adapter.create_font(**kwargs)
    
    @staticmethod
    def CTkOptionMenu(parent, **kwargs):
        # Use combobox as option menu fallback
        return ui_adapter.create_combobox(parent, **kwargs)
    
    @staticmethod
    def CTkTabview(parent, **kwargs):
        # Create a simple frame-based tabview fallback
        import tkinter as tk
        from tkinter import ttk
        if ui_adapter.is_customtkinter_available():
            # Try to import real CTkTabview if available
            try:
                import customtkinter as real_ctk
                return real_ctk.CTkTabview(parent, **kwargs)
            except:
                pass
        # Fallback to ttk.Notebook
        return ttk.Notebook(parent, **kwargs)
    
    # Add direct access to tkinter modules
    @property
    def filedialog(self):
        import tkinter.filedialog
        return tkinter.filedialog
    
    @property  
    def messagebox(self):
        import tkinter.messagebox
        return tkinter.messagebox

# Create compatibility object
ctk = CompatCTK()
HAS_CTK = ui_adapter.is_customtkinter_available()

# Standardized Color Scheme
from frontend.presentation.styles.theme import Theme

# Backward-compatible color shim for refactored theming
class Colors:
	"""Shim to keep old color references working after style refactor."""
	PRIMARY = Theme.COLOR_PRIMARY
	# Use secondary (purple) as hover accent per new theme design
	PRIMARY_HOVER = Theme.COLOR_SECONDARY
	TEXT_PRIMARY = Theme.COLOR_TEXT_PRIMARY

# ---------------------------
# Configuration
# ---------------------------

class AppConfig:
	"""Fixed application configuration"""
	API_BASE_URL = "http://localhost:8080"
	FAVICON_PATH = Path(__file__).parent.parent / "static" / "images" / "favicon.ico"
	DEFAULT_SCHEMA = "well_production"
	DEFAULT_RECORDS = "10000"
	DEFAULT_COMPRESSION = "zstd"


# ---------------------------
# Service Layer (CQRS style)
# ---------------------------


class ApiClient:
	def __init__(self, base_url: str):
		self.base_url = base_url.rstrip("/")

	# Commands
	def write_polars(self, schema_name: str, data: List[Dict[str, Any]], compression: str = "zstd") -> Dict[str, Any]:
		url = f"{self.base_url}/write/polars/{schema_name}"
		payload = {"data": data, "compression": compression}
		resp = requests.post(url, json=payload, timeout=600)
		resp.raise_for_status()
		return resp.json()

	# Queries
	def read_polars(self, schema_name: str) -> Tuple[Optional[Any], int]:
		url = f"{self.base_url}/read/polars/{schema_name}"
		resp = requests.get(url, timeout=600)
		resp.raise_for_status()
		if pa is None or pa_ipc is None:
			return None, len(resp.content)
		# Parse Arrow IPC stream
		reader = pa_ipc.open_stream(pa.BufferReader(resp.content))
		table = reader.read_all()
		return table, len(resp.content)

	# Schemas
	def list_schema_families(self) -> List[str]:
		url = f"{self.base_url}/schemas/"
		resp = requests.get(url, timeout=60)
		resp.raise_for_status()
		return resp.json()

	def get_latest_schema(self, schema_name: str) -> Dict[str, Any]:
		url = f"{self.base_url}/schemas/{schema_name}/latest"
		resp = requests.get(url, timeout=60)
		resp.raise_for_status()
		return resp.json()

	def get_schema_versions(self, schema_name: str) -> List[int]:
		url = f"{self.base_url}/schemas/{schema_name}"
		resp = requests.get(url, timeout=60)
		resp.raise_for_status()
		return resp.json()

	def register_schema(self, schema_name: str, schema_definition: Dict[str, Any]) -> Dict[str, Any]:
		url = f"{self.base_url}/schemas/{schema_name}"
		resp = requests.post(url, json=schema_definition, timeout=60)
		resp.raise_for_status()
		return resp.json() if resp.content else {"status": resp.status_code}


class DataGenerator:
	@staticmethod
	def generate_sample_data(num_records: int) -> List[Dict[str, Any]]:
		data: List[Dict[str, Any]] = []
		base_prod_date = datetime(2020, 1, 1)
		now = datetime.now()
		for i in range(num_records):
			created_at_dt = now - timedelta(days=random.randint(0, 365))
			prod_date_dt = base_prod_date + timedelta(days=(i % 3650), hours=random.randint(0, 23))
			record = {
				"id": str(uuid.uuid4()),
				"created_at": created_at_dt.isoformat() + "Z",
				"version": 1,
				"field_code": random.randint(1, 1000),
				"field_name": f"Field_{random.randint(1, 1000)}",
				"well_code": random.randint(1, 100),
				"well_reference": f"WELL_REF_{random.randint(1,100):03d}",
				"well_name": f"Well_{random.randint(1,100)}",
				"production_period": prod_date_dt.isoformat() + "Z",
				"days_on_production": random.randint(15, 30),
				"oil_production_kbd": round(random.uniform(10.0, 500.0) + (i * 0.01), 2),
				"gas_production_mmcfd": round(random.uniform(5.0, 200.0) + (i * 0.005), 2),
				"liquids_production_kbd": round(random.uniform(2.0, 100.0) + (i * 0.0025), 2),
				"water_production_kbd": round(random.uniform(20.0, 1000.0) + (i * 0.0075), 2),
				"data_source": "mvp_frontend",
				"source_data": json.dumps({"ui": "tk", "row": i}),
				"partition_0": f"partition_{random.randint(0,9)}",
			}
			data.append(record)
		return data


# ---------------------------
# Plotting Components
# ---------------------------

class PlotType:
	"""Available plot types for data visualization"""
	SCATTER = "scatter"
	LINE = "line"
	BAR = "bar"
	HISTOGRAM = "histogram"
	BOX = "box"
	HEATMAP = "heatmap"
	CORRELATION = "correlation"

class DataType:
	"""Data type options for axis formatting"""
	AUTO = "auto"
	NUMERIC = "numeric"
	CATEGORICAL = "categorical"
	DATETIME = "datetime"
	STRING = "string"

class PlotExplorer:
	"""Advanced plotting component with matplotlib integration"""
	
	def __init__(self, parent, data=None):
		self.parent = parent
		self.data = data or []
		self.columns = list(data[0].keys()) if data else []
		
		# Plot mode: 'individual' or 'group'
		self.plot_mode = 'individual'
		
		# Plot configuration
		self.x_column = None
		self.y_column = None
		self.x_data_type = DataType.AUTO
		self.y_data_type = DataType.AUTO
		self.plot_type = PlotType.SCATTER
		
		# Individual plot filtering
		self.filter_column = None
		self.filter_value = None
		self.filtered_data = self.data.copy()
		
		# Group plot configuration
		self.group_column = None
		self.group_values = []
		self.aggregate_function = "mean"  # mean, sum, count, min, max
		
		# Current figure
		self.figure = None
		self.canvas = None
		self.toolbar = None
		
		self._build_plot_explorer()
	
	def _build_plot_explorer(self):
		"""Build the complete plotting interface"""
		# Main container
		self.main_frame = ctk.CTkFrame(self.parent)
		self.main_frame.pack(fill="both", expand=True)
		
		# Top control panel
		self._build_control_panel()
		
		# Plot area
		self._build_plot_area()
		
		# Bottom info panel
		self._build_info_panel()
	
	def _build_control_panel(self):
		"""Build the plot configuration control panel"""
		control_frame = ctk.CTkFrame(self.main_frame)
		control_frame.pack(fill="x", padx=10, pady=(10, 5))
		
		# Title and mode selection
		title_frame = ctk.CTkFrame(control_frame)
		title_frame.pack(fill="x", padx=10, pady=(10, 5))
		
		ctk.CTkLabel(
			title_frame,
			text="📊 Plot Configuration",
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(side="left", padx=10, pady=8)
		
		# Mode selection buttons
		mode_frame = ctk.CTkFrame(title_frame)
		mode_frame.pack(side="right", padx=10, pady=5)
		
		ctk.CTkLabel(mode_frame, text="Mode:", font=ctk.CTkFont(size=12, weight="bold") if HAS_CTK else ("Arial", 10, "bold")).pack(side="left", padx=(5, 10))
		
		self.individual_btn = ctk.CTkButton(
			mode_frame,
			text="👤 Individual Plots",
			command=lambda: self._switch_mode('individual'),
			width=140,
			height=30
		)
		self.individual_btn.pack(side="left", padx=2)
		
		self.group_btn = ctk.CTkButton(
			mode_frame,
			text="👥 Group Plots",
			command=lambda: self._switch_mode('group'),
			width=140,
			height=30
		)
		self.group_btn.pack(side="left", padx=2)
		
		# Update button states
		self._update_mode_buttons()
		
		# Mode-specific configuration
		self.config_container = ctk.CTkFrame(control_frame)
		self.config_container.pack(fill="x", padx=10, pady=5)
		
		self._build_mode_specific_config()
	
	def _switch_mode(self, mode):
		"""Switch between individual and group plot modes"""
		self.plot_mode = mode
		self._update_mode_buttons()
		self._build_mode_specific_config()
		self._update_info(f"📊 Switched to {mode} plot mode")
	
	def _update_mode_buttons(self):
		"""Update the appearance of mode buttons"""
		if self.plot_mode == 'individual':
			try:
				self.individual_btn.configure(fg_color="#1f538d", state="disabled")
				self.group_btn.configure(fg_color="#404040", state="normal")
			except:
				pass
		else:
			try:
				self.individual_btn.configure(fg_color="#404040", state="normal")
				self.group_btn.configure(fg_color="#1f538d", state="disabled")
			except:
				pass
	
	def _build_mode_specific_config(self):
		"""Build configuration UI based on current mode"""
		# Clear existing configuration
		for widget in self.config_container.winfo_children():
			widget.destroy()
		
		if self.plot_mode == 'individual':
			self._build_individual_config()
		else:
			self._build_group_config()
	
	def _build_individual_config(self):
		"""Build configuration for individual plots with filtering"""
		# Filter section
		filter_frame = ctk.CTkFrame(self.config_container)
		filter_frame.pack(fill="x", padx=10, pady=(10, 5))
		
		ctk.CTkLabel(
			filter_frame,
			text="🔍 Data Filter (Optional)",
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		).pack(anchor="w", padx=10, pady=(8, 5))
		
		filter_controls = ctk.CTkFrame(filter_frame)
		filter_controls.pack(fill="x", padx=10, pady=(0, 8))
		
		# Filter column selection
		filter_col_frame = ctk.CTkFrame(filter_controls)
		filter_col_frame.pack(side="left", padx=(0, 10))
		
		ctk.CTkLabel(filter_col_frame, text="Filter Column:", font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8)).pack(anchor="w", padx=5, pady=(2, 0))
		
		self.filter_column_var = self._get_string_var("None")
		filter_options = ["None"] + self.columns
		self.filter_column_menu = ctk.CTkOptionMenu(
			filter_col_frame,
			values=filter_options,
			variable=self.filter_column_var,
			command=self._on_filter_column_change,
			width=120
		)
		self.filter_column_menu.pack(padx=5, pady=(0, 5))
		
		# Filter value selection button
		filter_val_frame = ctk.CTkFrame(filter_controls)
		filter_val_frame.pack(side="left", padx=(0, 10))
		
		ctk.CTkLabel(filter_val_frame, text="Filter Values:", font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8)).pack(anchor="w", padx=5, pady=(2, 0))
		
		# Initialize filter values
		self.filter_values = []
		
		# Button to open filter selection dialog
		self.filter_select_btn = ctk.CTkButton(
			filter_val_frame,
			text="📋 Select Values",
			command=self._open_filter_selector,
			width=120,
			height=25
		)
		self.filter_select_btn.pack(padx=5, pady=(0, 5))
		
		# Filter status label
		self.filter_selection_label = ctk.CTkLabel(
			filter_val_frame,
			text="No filters selected",
			font=ctk.CTkFont(size=9) if HAS_CTK else ("Arial", 7),
			text_color="#888888"
		)
		self.filter_selection_label.pack(padx=5, pady=(0, 2))
		
		# Apply filter button
		apply_filter_btn = ctk.CTkButton(
			filter_controls,
			text="🔍 Apply Filter",
			command=self._apply_individual_filter,
			width=100,
			height=30
		)
		apply_filter_btn.pack(side="left", padx=(10, 0), pady=12)
		
		# Status label
		self.filter_status_label = ctk.CTkLabel(
			filter_controls,
			text=f"📊 {len(self.data):,} records available",
			font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8),
			text_color="#888888"
		)
		self.filter_status_label.pack(side="left", padx=(15, 0), pady=12)
		
		# Column selection (same as before but using filtered data)
		self._build_column_selection()
	
	def _build_group_config(self):
		"""Build configuration for group plots with aggregation"""
		# Group section
		group_frame = ctk.CTkFrame(self.config_container)
		group_frame.pack(fill="x", padx=10, pady=(10, 5))
		
		ctk.CTkLabel(
			group_frame,
			text="👥 Group Configuration",
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		).pack(anchor="w", padx=10, pady=(8, 5))
		
		group_controls = ctk.CTkFrame(group_frame)
		group_controls.pack(fill="x", padx=10, pady=(0, 8))
		
		# Group column selection
		group_col_frame = ctk.CTkFrame(group_controls)
		group_col_frame.pack(side="left", padx=(0, 10))
		
		ctk.CTkLabel(group_col_frame, text="Group By Column:", font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8)).pack(anchor="w", padx=5, pady=(2, 0))
		
		self.group_column_var = self._get_string_var(self.columns[0] if self.columns else "")
		self.group_column_menu = ctk.CTkOptionMenu(
			group_col_frame,
			values=self.columns if self.columns else ["No data"],
			variable=self.group_column_var,
			command=self._on_group_column_change,
			width=120
		)
		self.group_column_menu.pack(padx=5, pady=(0, 5))
		
		# Aggregation function selection
		agg_frame = ctk.CTkFrame(group_controls)
		agg_frame.pack(side="left", padx=(0, 10))
		
		ctk.CTkLabel(agg_frame, text="Aggregation:", font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8)).pack(anchor="w", padx=5, pady=(2, 0))
		
		self.aggregate_var = self._get_string_var("mean")
		self.aggregate_menu = ctk.CTkOptionMenu(
			agg_frame,
			values=["mean", "sum", "count", "min", "max", "median"],
			variable=self.aggregate_var,
			command=self._on_aggregate_change,
			width=100
		)
		self.aggregate_menu.pack(padx=5, pady=(0, 5))
		
		# Group selection
		groups_frame = ctk.CTkFrame(group_controls)
		groups_frame.pack(side="left", padx=(0, 10))
		
		ctk.CTkLabel(groups_frame, text="Select Groups:", font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8)).pack(anchor="w", padx=5, pady=(2, 0))
		
		self.groups_button = ctk.CTkButton(
			groups_frame,
			text="📋 Select Groups",
			command=self._open_group_selector,
			width=120,
			height=25
		)
		self.groups_button.pack(padx=5, pady=(0, 5))
		
		# Status label
		self.group_status_label = ctk.CTkLabel(
			group_controls,
			text="📊 Select grouping column",
			font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8),
			text_color="#888888"
		)
		self.group_status_label.pack(side="left", padx=(15, 0), pady=12)
		
		# Apply Groups button
		apply_groups_btn = ctk.CTkButton(
			group_controls,
			text="✓ Apply Groups",
			command=self._apply_group_settings,
			width=100,
			height=25,
			fg_color=Theme.COLOR_PRIMARY,
			hover_color=Colors.PRIMARY_HOVER
		)
		apply_groups_btn.pack(side="right", padx=10, pady=12)

		# Column selection (same as before but for group data)
		self._build_column_selection()
	
	def _apply_group_settings(self):
		"""Apply the current group settings"""
		if not hasattr(self, 'group_values') or not self.group_values:
			self._update_info("❌ Please select groups first")
			return
		
		if not self.group_column:
			self._update_info("❌ Please select a group column first")
			return
		
		self._update_info(f"✅ Group settings applied: {len(self.group_values)} groups selected")
		self._log(f"📊 Group plotting configured: {self.group_column} with {len(self.group_values)} groups")
	
	def _build_column_selection(self):
		"""Build the common column selection interface"""
		# Column selection row
		columns_frame = ctk.CTkFrame(self.config_container)
		columns_frame.pack(fill="x", padx=10, pady=5)
		
		# X-axis column
		x_group = ctk.CTkFrame(columns_frame)
		x_group.pack(side="left", padx=(10, 5), pady=8)
		
		ctk.CTkLabel(x_group, text="X-Axis Column:", font=ctk.CTkFont(size=12, weight="bold") if HAS_CTK else ("Arial", 10, "bold")).pack(anchor="w", padx=5, pady=(5, 2))
		
		self.x_column_var = self._get_string_var(self.columns[0] if self.columns else "")
		self.x_column_menu = ctk.CTkOptionMenu(
			x_group,
			values=self.columns if self.columns else ["No data"],
			variable=self.x_column_var,
			command=self._on_x_column_change,
			width=150
		)
		self.x_column_menu.pack(padx=5, pady=(0, 5))
		
		# X-axis data type
		ctk.CTkLabel(x_group, text="Data Type:", font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8)).pack(anchor="w", padx=5)
		self.x_type_var = self._get_string_var(DataType.AUTO)
		self.x_type_menu = ctk.CTkOptionMenu(
			x_group,
			values=[DataType.AUTO, DataType.NUMERIC, DataType.CATEGORICAL, DataType.DATETIME, DataType.STRING],
			variable=self.x_type_var,
			command=self._on_x_type_change,
			width=150
		)
		self.x_type_menu.pack(padx=5, pady=(0, 8))
		
		# Y-axis column
		y_group = ctk.CTkFrame(columns_frame)
		y_group.pack(side="left", padx=5, pady=8)
		
		ctk.CTkLabel(y_group, text="Y-Axis Column:", font=ctk.CTkFont(size=12, weight="bold") if HAS_CTK else ("Arial", 10, "bold")).pack(anchor="w", padx=5, pady=(5, 2))
		
		self.y_column_var = self._get_string_var(self.columns[1] if len(self.columns) > 1 else (self.columns[0] if self.columns else ""))
		self.y_column_menu = ctk.CTkOptionMenu(
			y_group,
			values=self.columns if self.columns else ["No data"],
			variable=self.y_column_var,
			command=self._on_y_column_change,
			width=150
		)
		self.y_column_menu.pack(padx=5, pady=(0, 5))
		
		# Y-axis data type
		ctk.CTkLabel(y_group, text="Data Type:", font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8)).pack(anchor="w", padx=5)
		self.y_type_var = self._get_string_var(DataType.AUTO)
		self.y_type_menu = ctk.CTkOptionMenu(
			y_group,
			values=[DataType.AUTO, DataType.NUMERIC, DataType.CATEGORICAL, DataType.DATETIME, DataType.STRING],
			variable=self.y_type_var,
			command=self._on_y_type_change,
			width=150
		)
		self.y_type_menu.pack(padx=5, pady=(0, 8))
		
		# Plot type and actions
		actions_group = ctk.CTkFrame(columns_frame)
		actions_group.pack(side="left", padx=(5, 10), pady=8)
		
		ctk.CTkLabel(actions_group, text="Plot Type:", font=ctk.CTkFont(size=12, weight="bold") if HAS_CTK else ("Arial", 10, "bold")).pack(anchor="w", padx=5, pady=(5, 2))
		
		plot_types = [PlotType.SCATTER, PlotType.LINE, PlotType.BAR, PlotType.HISTOGRAM, PlotType.BOX]
		if self.plot_mode == 'individual':
			plot_types.append(PlotType.CORRELATION)
		
		self.plot_type_var = self._get_string_var(PlotType.SCATTER)
		self.plot_type_menu = ctk.CTkOptionMenu(
			actions_group,
			values=plot_types,
			variable=self.plot_type_var,
			command=self._on_plot_type_change,
			width=150
		)
		self.plot_type_menu.pack(padx=5, pady=(0, 5))
		
		# Action buttons
		button_frame = ctk.CTkFrame(actions_group)
		button_frame.pack(padx=5, pady=(5, 8))
		
		plot_btn = ctk.CTkButton(
			button_frame,
			text="📊 Generate Plot",
			command=self._generate_plot,
			width=120,
			height=32
		)
		plot_btn.pack(side="left", padx=2)
		
		clear_btn = ctk.CTkButton(
			button_frame,
			text="🗑️ Clear",
			command=self._clear_plot,
			width=80,
			height=32
		)
		clear_btn.pack(side="left", padx=2)
	
	def _build_plot_area(self):
		"""Build the matplotlib plot area"""
		# Create matplotlib figure
		plot_frame = ctk.CTkFrame(self.main_frame)
		plot_frame.pack(fill="both", expand=True, padx=10, pady=5)
		
		# Create tkinter frame for matplotlib
		try:
			import tkinter as tk
			self.plot_container = tk.Frame(plot_frame, bg='#2b2b2b')
			self.plot_container.pack(fill="both", expand=True, padx=10, pady=10)
		except Exception:
			self.plot_container = plot_frame
		
		# Initialize with empty plot
		self._create_empty_plot()
	
	def _build_info_panel(self):
		"""Build the information panel"""
		info_frame = ctk.CTkFrame(self.main_frame)
		info_frame.pack(fill="x", padx=10, pady=(5, 10))
		
		self.info_label = ctk.CTkLabel(
			info_frame,
			text="💡 Select X and Y columns, choose plot type, then click 'Generate Plot'",
			font=ctk.CTkFont(size=12) if HAS_CTK else ("Arial", 10),
			text_color="#888888"
		)
		self.info_label.pack(padx=10, pady=8)
	
	def _create_empty_plot(self):
		"""Create an empty plot placeholder"""
		try:
			# Create figure with dark theme
			self.figure = Figure(figsize=(10, 6), facecolor='#2b2b2b')
			self.figure.patch.set_facecolor(Theme.COLOR_SURFACE_LIGHT)
			
			# Create canvas
			self.canvas = FigureCanvasTkAgg(self.figure, self.plot_container)
			self.canvas.get_tk_widget().pack(fill="both", expand=True)
			
			# Add navigation toolbar
			toolbar_frame = ctk.CTkFrame(self.plot_container)
			toolbar_frame.pack(fill="x", pady=(5, 0))
			
			# Create tkinter frame for toolbar
			import tkinter as tk
			toolbar_container = tk.Frame(toolbar_frame, bg='#2b2b2b')
			toolbar_container.pack(fill="x", padx=5, pady=5)
			
			self.toolbar = NavigationToolbar2Tk(self.canvas, toolbar_container)
			self.toolbar.config(bg='#2b2b2b')
			self.toolbar.update()
			
			# Create empty subplot
			ax = self.figure.add_subplot(111, facecolor=Theme.GRAY_LIGHT)
			ax.text(0.5, 0.5, '📊 Select columns and plot type\nthen click "Generate Plot"', 
					ha='center', va='center', transform=ax.transAxes, 
					fontsize=14, color='white', alpha=0.7)
			ax.set_title('Data Visualization', color='white', fontsize=16, pad=20)
			ax.tick_params(colors='white')
			ax.spines['bottom'].set_color('white')
			ax.spines['top'].set_color('white')
			ax.spines['right'].set_color('white')
			ax.spines['left'].set_color('white')
			
			self.canvas.draw()
			
		except Exception as e:
			error_msg = ErrorHandler.handle_error(e, "creating empty plot")
			print(error_msg)
	
	def _generate_plot(self):
		"""Generate the plot based on current configuration"""
		if not self.data:
			self._update_info("❌ Cannot generate plot: no data available")
			return
		
		try:
			# Get current settings
			x_col = self._get_var_value(self.x_column_var) if hasattr(self, 'x_column_var') else ""
			y_col = self._get_var_value(self.y_column_var) if hasattr(self, 'y_column_var') else ""
			plot_type = self._get_var_value(self.plot_type_var) if hasattr(self, 'plot_type_var') else PlotType.SCATTER
			
			if not x_col or not y_col:
				self._update_info("❌ Please select both X and Y columns")
				return
			
			if x_col not in self.columns or y_col not in self.columns:
				self._update_info(f"❌ Invalid columns selected: {x_col}, {y_col}")
				return
			
			# Get data based on mode
			if self.plot_mode == 'individual':
				plot_data = self.filtered_data
				title_suffix = f" (Filtered: {len(plot_data):,} records)"
			else:
				plot_data = self._prepare_group_data(x_col, y_col)
				if not plot_data:
					self._update_info("❌ No group data available")
					return
				title_suffix = f" (Grouped by {self.group_column})"
			
			# Clear previous plot
			self.figure.clear()
			
			# Create subplot with dark theme
			ax = self.figure.add_subplot(111, facecolor=Theme.GRAY_LIGHT)
			
			# Generate plot based on mode and type
			if self.plot_mode == 'individual':
				self._create_individual_plot(ax, plot_data, x_col, y_col, plot_type)
			else:
				self._create_group_plot(ax, plot_data, x_col, y_col, plot_type)
			
			# Update title
			if plot_type not in [PlotType.HISTOGRAM, PlotType.CORRELATION, PlotType.BOX]:
				ax.set_title(f'{plot_type.title()} Plot: {x_col} vs {y_col}{title_suffix}', 
							color='white', fontsize=14, pad=20)
			
			# Update canvas
			self.canvas.draw()
			
			self._update_info(f"✅ Generated {plot_type} plot ({len(plot_data):,} data points)")
			
		except Exception as e:
			self._update_info(f"❌ Error generating plot: {e}")
			print(f"Plot generation error: {e}")
	
	def _prepare_group_data(self, x_col, y_col):
		"""Prepare aggregated data for group plotting"""
		if not hasattr(self, 'group_column') or not self.group_column:
			return []
		
		if not hasattr(self, 'group_values') or not self.group_values:
			return []
		
		# Group data by the selected column
		grouped_data = {}
		for row in self.data:
			group_val = StringUtils.extract_row_value(row, self.group_column)
			if group_val in self.group_values:
				if group_val not in grouped_data:
					grouped_data[group_val] = []
				grouped_data[group_val].append(row)
		
		# Aggregate data for each group
		aggregated_data = []
		agg_func = self._get_var_value(self.aggregate_var) if hasattr(self, 'aggregate_var') else 'mean'
		
		for group_name, group_rows in grouped_data.items():
			# Extract numeric values for aggregation
			x_values = []
			y_values = []
			
			for row in group_rows:
				try:
					x_val = StringUtils.parse_numeric_value(StringUtils.extract_row_value(row, x_col))
					y_val = StringUtils.parse_numeric_value(StringUtils.extract_row_value(row, y_col))
					x_values.append(x_val)
					y_values.append(y_val)
				except (ValueError, TypeError):
					continue
			
			if x_values and y_values:
				# Apply aggregation function
				if agg_func == 'mean':
					x_agg = sum(x_values) / len(x_values)
					y_agg = sum(y_values) / len(y_values)
				elif agg_func == 'sum':
					x_agg = sum(x_values)
					y_agg = sum(y_values)
				elif agg_func == 'count':
					x_agg = len(x_values)
					y_agg = len(y_values)
				elif agg_func == 'min':
					x_agg = min(x_values)
					y_agg = min(y_values)
				elif agg_func == 'max':
					x_agg = max(x_values)
					y_agg = max(y_values)
				elif agg_func == 'median':
					x_sorted = sorted(x_values)
					y_sorted = sorted(y_values)
					n_x = len(x_sorted)
					n_y = len(y_sorted)
					x_agg = x_sorted[n_x//2] if n_x % 2 else (x_sorted[n_x//2-1] + x_sorted[n_x//2]) / 2
					y_agg = y_sorted[n_y//2] if n_y % 2 else (y_sorted[n_y//2-1] + y_sorted[n_y//2]) / 2
				else:
					x_agg = sum(x_values) / len(x_values)  # Default to mean
					y_agg = sum(y_values) / len(y_values)
				
				aggregated_data.append({
					'group': group_name,
					x_col: x_agg,
					y_col: y_agg,
					'count': len(x_values)
				})
		
		return aggregated_data
	
	def _create_individual_plot(self, ax, plot_data, x_col, y_col, plot_type):
		"""Create individual plot (same as before but with filtered data)"""
		# Extract data
		x_data = [row.get(x_col) for row in plot_data if row.get(x_col) is not None]
		y_data = [row.get(y_col) for row in plot_data if row.get(y_col) is not None]
		
		if not x_data or not y_data:
			ax.text(0.5, 0.5, 'No valid data found for selected columns', 
					ha='center', va='center', transform=ax.transAxes, color='white')
			return
		
		# Clean and convert data
		x_clean, y_clean = self._clean_data(x_data, y_data, x_col, y_col)
		
		if len(x_clean) == 0:
			ax.text(0.5, 0.5, 'No valid data after cleaning', 
					ha='center', va='center', transform=ax.transAxes, color='white')
			return
		
		# Create the plot (reusing existing logic)
		self._create_plot(ax, x_clean, y_clean, x_col, y_col, plot_type)
	
	def _create_group_plot(self, ax, plot_data, x_col, y_col, plot_type):
		"""Create group plot with multiple series"""
		if not plot_data:
			ax.text(0.5, 0.5, 'No group data available', 
					ha='center', va='center', transform=ax.transAxes, color='white')
			return
		
		# Define colors for different groups
		colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
		
		# Group data by group column
		groups = {}
		for row in plot_data:
			group_name = row['group']
			if group_name not in groups:
				groups[group_name] = {'x': [], 'y': []}
			groups[group_name]['x'].append(row[x_col])
			groups[group_name]['y'].append(row[y_col])
		
		# Plot each group
		for i, (group_name, group_data) in enumerate(groups.items()):
			color = colors[i % len(colors)]
			
			if plot_type == PlotType.SCATTER:
				ax.scatter(group_data['x'], group_data['y'], 
						  alpha=0.7, s=80, c=color, label=group_name, 
						  edgecolors='white', linewidth=1)
			
			elif plot_type == PlotType.LINE:
				# Sort by x values for proper line connection
				paired_data = list(zip(group_data['x'], group_data['y']))
				paired_data.sort(key=lambda x: x[0])
				x_sorted, y_sorted = zip(*paired_data)
				
				ax.plot(x_sorted, y_sorted, marker='o', markersize=6, 
						linewidth=2, alpha=0.8, color=color, label=group_name)
			
			elif plot_type == PlotType.BAR:
				# For bar plots, show groups side by side
				x_pos = [x + i * 0.1 for x in range(len(group_data['x']))]
				ax.bar(x_pos, group_data['y'], alpha=0.7, color=color, 
					   label=group_name, width=0.1)
			
			elif plot_type == PlotType.BOX:
				# Create box plot for each group
				bp = ax.boxplot([group_data['y']], positions=[i], 
							   patch_artist=True, widths=0.6)
				bp['boxes'][0].set_facecolor(color)
				bp['boxes'][0].set_alpha(0.7)
		
		# Set labels and legend
		if plot_type not in [PlotType.HISTOGRAM, PlotType.CORRELATION]:
			ax.set_xlabel(x_col, color='white', fontsize=12)
			ax.set_ylabel(y_col, color='white', fontsize=12)
			ax.legend(loc='best', fancybox=True, framealpha=0.9)
		
		# Style the plot
		ax.tick_params(colors='white')
		ax.spines['bottom'].set_color('white')
		ax.spines['top'].set_color('white')
		ax.spines['right'].set_color('white')
		ax.spines['left'].set_color('white')
		ax.grid(True, alpha=0.3, color='white')
	
	def _on_filter_column_change(self, value):
		"""Handle filter column selection change"""
		self.filter_column = value
		if value == "None":
			self.filter_values = []
			self._update_filter_status()
		else:
			# Reset filter values when column changes
			self.filter_values = []
			self._update_filter_status()
	
	def _open_filter_selector(self):
		"""Open a dialog to select which values to include in filter"""
		if not hasattr(self, 'filter_column') or not self.filter_column or self.filter_column == "None":
			self._update_info("❌ Please select a filter column first")
			return
		
		# Get unique values for the filter column
		unique_values = set()
		for row in self.data:
			val = str(row.get(self.filter_column, ''))
			if val.strip():
				unique_values.add(val)
		
		if not unique_values:
			self._update_info("❌ No valid values found in selected column")
			return
		
		# Create filter selection dialog
		self._create_filter_selection_dialog(sorted(list(unique_values)))
	
	def _create_filter_selection_dialog(self, available_values):
		"""Create a dialog for filter value selection (unified UIUX with groups)"""
		import tkinter as tk
		from tkinter import ttk
		
		# Create dialog window
		dialog = tk.Toplevel()
		dialog.title("Select Filter Values")
		dialog.geometry("400x500")
		dialog.configure(bg='#2b2b2b')
		
		# Make it modal
		dialog.transient(self.parent)
		dialog.grab_set()
		
		# Title
		title_label = tk.Label(dialog, text="Select Values to Include in Filter", 
							  bg='#2b2b2b', fg='white', font=('Arial', 14, 'bold'))
		title_label.pack(pady=10)
		
		# Instruction
		instruction = tk.Label(dialog, text=f"Column: {self.filter_column}", 
							  bg='#2b2b2b', fg='#888888', font=('Arial', 10))
		instruction.pack(pady=(0, 10))
		
		# Scrollable frame for checkboxes
		canvas = tk.Canvas(dialog, bg='#404040', highlightthickness=0)
		scrollbar = ttk.Scrollbar(dialog, orient="vertical", command=canvas.yview)
		scrollable_frame = tk.Frame(canvas, bg='#404040')
		
		scrollable_frame.bind(
			"<Configure>",
			lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
		)
		
		canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
		canvas.configure(yscrollcommand=scrollbar.set)
		
		# Create checkboxes for each value
		value_vars = {}
		for value in available_values:
			var = tk.BooleanVar(value=value in self.filter_values)
			value_vars[value] = var
			
			cb = tk.Checkbutton(scrollable_frame, text=value, variable=var,
							   bg='#404040', fg='white', selectcolor='#404040',
							   activebackground='#555555', activeforeground='white',
							   font=('Arial', 10))
			cb.pack(anchor='w', padx=10, pady=2)
		
		canvas.pack(side="left", fill="both", expand=True, padx=(10, 0), pady=10)
		scrollbar.pack(side="right", fill="y", pady=10)
		
		# Buttons frame
		button_frame = tk.Frame(dialog, bg='#2b2b2b')
		button_frame.pack(fill='x', pady=10)
		
		def select_all():
			for var in value_vars.values():
				var.set(True)
		
		def select_none():
			for var in value_vars.values():
				var.set(False)
		
		def apply_selection():
			self.filter_values = [value for value, var in value_vars.items() if var.get()]
			self._update_filter_status()
			dialog.destroy()
		
		# Buttons (unified styling with standardized colors)
		tk.Button(button_frame, text="Select All", command=select_all,
				 bg=Theme.COLOR_PRIMARY, fg='white', font=('Arial', 10)).pack(side='left', padx=10)
		
		tk.Button(button_frame, text="Select None", command=select_none,
				 bg=Theme.COLOR_SURFACE_LIGHT, fg='white', font=('Arial', 10)).pack(side='left', padx=5)
		
		tk.Button(button_frame, text="Apply", command=apply_selection,
				 bg=Theme.COLOR_PRIMARY, fg='white', font=('Arial', 10, 'bold')).pack(side='right', padx=10)
		
		tk.Button(button_frame, text="Cancel", command=dialog.destroy,
				 bg=Theme.COLOR_PRIMARY, fg='white', font=('Arial', 10)).pack(side='right', padx=5)
	
	def _update_filter_status(self):
		"""Update the filter status label"""
		if hasattr(self, 'filter_selection_label'):
			if self.filter_values:
				if len(self.filter_values) <= 3:
					values_text = ", ".join(self.filter_values)
				else:
					values_text = f"{', '.join(self.filter_values[:3])}... (+{len(self.filter_values)-3} more)"
				status_text = f"✓ {len(self.filter_values)} values: {values_text}"
			else:
				status_text = "No filters selected"
			
			self.filter_selection_label.configure(text=status_text)
	
	def _on_filter_value_change(self, value):
		"""Handle filter value selection change (legacy - no longer used)"""
		pass
	
	def _apply_individual_filter(self):
		"""Apply the individual filter to the data"""
		if not hasattr(self, 'filter_column') or not self.filter_column:
			return
			
		filter_col = self.filter_column
		
		if filter_col == "None" or not self.filter_values:
			self.filtered_data = self.data.copy()
		else:
			self.filtered_data = []
			for row in self.data:
				row_value = str(row.get(filter_col, ''))
				if row_value in self.filter_values:
					self.filtered_data.append(row)
		
		# Update status
		self.filter_status_label.configure(
			text=f"📊 {len(self.filtered_data):,} records after filter"
		)
		self._update_info(f"🔍 Filter applied: {len(self.filtered_data):,} records selected")
	
	def _on_group_column_change(self, value):
		"""Handle group column selection change"""
		self.group_column = value
		# Reset group selection when column changes
		self.group_values = []
		self._update_group_status()
	
	def _on_aggregate_change(self, value):
		"""Handle aggregation function change"""
		self.aggregate_function = value
		agg_descriptions = {
			"mean": "Average values within each group",
			"sum": "Sum values within each group", 
			"count": "Count records in each group",
			"min": "Minimum values within each group",
			"max": "Maximum values within each group",
			"median": "Median values within each group"
		}
		self._update_info(f"📊 Aggregation: {agg_descriptions.get(value, '')}")
	
	def _open_group_selector(self):
		"""Open a dialog to select which groups to include"""
		if not hasattr(self, 'group_column') or not self.group_column:
			self._update_info("❌ Please select a group column first")
			return
		
		# Get unique values for the group column
		unique_groups = set()
		for row in self.data:
			val = str(row.get(self.group_column, ''))
			if val.strip():
				unique_groups.add(val)
		
		if not unique_groups:
			self._update_info("❌ No valid groups found in selected column")
			return
		
		# Create a simple selection dialog
		self._create_group_selection_dialog(sorted(list(unique_groups)))
	
	def _create_group_selection_dialog(self, available_groups):
		"""Create a dialog for group selection"""
		import tkinter as tk
		from tkinter import ttk
		
		# Create dialog window
		dialog = tk.Toplevel()
		dialog.title("Select Groups to Plot")
		dialog.geometry("400x500")
		dialog.configure(bg='#2b2b2b')
		
		# Make it modal
		dialog.transient(self.parent)
		dialog.grab_set()
		
		# Title
		title_label = tk.Label(dialog, text="Select Groups to Include in Plot", 
							  bg='#2b2b2b', fg='white', font=('Arial', 14, 'bold'))
		title_label.pack(pady=10)
		
		# Instruction
		instruction = tk.Label(dialog, text=f"Column: {self.group_column}", 
							  bg='#2b2b2b', fg='#888888', font=('Arial', 10))
		instruction.pack(pady=(0, 10))
		
		# Scrollable frame for checkboxes
		canvas = tk.Canvas(dialog, bg='#404040', highlightthickness=0)
		scrollbar = ttk.Scrollbar(dialog, orient="vertical", command=canvas.yview)
		scrollable_frame = tk.Frame(canvas, bg='#404040')
		
		scrollable_frame.bind(
			"<Configure>",
			lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
		)
		
		canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
		canvas.configure(yscrollcommand=scrollbar.set)
		
		# Create checkboxes for each group
		group_vars = {}
		for group in available_groups:
			var = tk.BooleanVar(value=group in self.group_values)
			group_vars[group] = var
			
			cb = tk.Checkbutton(scrollable_frame, text=group, variable=var,
							   bg='#404040', fg='white', selectcolor='#404040',
							   activebackground='#555555', activeforeground='white',
							   font=('Arial', 10))
			cb.pack(anchor='w', padx=10, pady=2)
		
		canvas.pack(side="left", fill="both", expand=True, padx=(10, 0), pady=10)
		scrollbar.pack(side="right", fill="y", pady=10)
		
		# Buttons frame
		button_frame = tk.Frame(dialog, bg='#2b2b2b')
		button_frame.pack(fill='x', pady=10)
		
		def select_all():
			for var in group_vars.values():
				var.set(True)
		
		def select_none():
			for var in group_vars.values():
				var.set(False)
		
		def apply_selection():
			self.group_values = [group for group, var in group_vars.items() if var.get()]
			self._update_group_status()
			dialog.destroy()
		
		# Buttons
		tk.Button(button_frame, text="Select All", command=select_all,
				 bg=Theme.COLOR_PRIMARY, fg='white', font=('Arial', 10)).pack(side='left', padx=10)
		
		tk.Button(button_frame, text="Select None", command=select_none,
				 bg=Theme.COLOR_SURFACE_LIGHT, fg='white', font=('Arial', 10)).pack(side='left', padx=5)
		
		tk.Button(button_frame, text="Apply", command=apply_selection,
				 bg=Theme.COLOR_PRIMARY, fg='white', font=('Arial', 10, 'bold')).pack(side='right', padx=10)
		
		tk.Button(button_frame, text="Cancel", command=dialog.destroy,
				 bg=Theme.COLOR_PRIMARY, fg='white', font=('Arial', 10)).pack(side='right', padx=5)
	
	def _update_group_status(self):
		"""Update the group status label"""
		if hasattr(self, 'group_status_label'):
			if self.group_values:
				status_text = f"📊 {len(self.group_values)} groups selected"
			else:
				status_text = "📊 No groups selected"
			
			self.group_status_label.configure(text=status_text)
	
	def _clean_data(self, x_data, y_data, x_col, y_col):
		"""Clean and prepare data for plotting"""
		cleaned_x = []
		cleaned_y = []
		
		# Get data types
		x_type = self._get_var_value(self.x_type_var) if hasattr(self, 'x_type_var') else DataType.AUTO
		y_type = self._get_var_value(self.y_type_var) if hasattr(self, 'y_type_var') else DataType.AUTO
		
		# Auto-detect data types if needed
		if x_type == DataType.AUTO:
			x_type = self._detect_data_type(x_data)
		if y_type == DataType.AUTO:
			y_type = self._detect_data_type(y_data)
		
		# Process pairs of data
		for i in range(min(len(x_data), len(y_data))):
			x_val = x_data[i]
			y_val = y_data[i]
			
			try:
				# Convert X data
				if x_type == DataType.NUMERIC:
					x_converted = float(str(x_val).replace(',', ''))
				elif x_type == DataType.CATEGORICAL or x_type == DataType.STRING:
					x_converted = str(x_val)
				elif x_type == DataType.DATETIME:
					x_converted = self._parse_datetime(x_val)
				else:
					x_converted = x_val
				
				# Convert Y data
				if y_type == DataType.NUMERIC:
					y_converted = float(str(y_val).replace(',', ''))
				elif y_type == DataType.CATEGORICAL or y_type == DataType.STRING:
					y_converted = str(y_val)
				elif y_type == DataType.DATETIME:
					y_converted = self._parse_datetime(y_val)
				else:
					y_converted = y_val
				
				# Only add if both conversions successful
				cleaned_x.append(x_converted)
				cleaned_y.append(y_converted)
				
			except (ValueError, TypeError):
				continue  # Skip invalid data points
		
		return cleaned_x, cleaned_y
	
	def _detect_data_type(self, data_list):
		"""Auto-detect the data type of a column"""
		if not data_list:
			return DataType.STRING
		
		# Sample first few values
		sample = data_list[:min(100, len(data_list))]
		numeric_count = 0
		datetime_count = 0
		
		for val in sample:
			val_str = str(val).strip()
			if not val_str:
				continue
				
			# Check if numeric
			try:
				float(val_str.replace(',', ''))
				numeric_count += 1
				continue
			except ValueError:
				pass
			
			# Check if datetime
			if self._is_datetime_string(val_str):
				datetime_count += 1
		
		total_valid = len([v for v in sample if str(v).strip()])
		if total_valid == 0:
			return DataType.STRING
			
		numeric_ratio = numeric_count / total_valid
		datetime_ratio = datetime_count / total_valid
		
		if numeric_ratio > 0.8:
			return DataType.NUMERIC
		elif datetime_ratio > 0.5:
			return DataType.DATETIME
		else:
			return DataType.CATEGORICAL
	
	def _is_datetime_string(self, s):
		"""Check if string looks like a datetime"""
		datetime_patterns = [
			r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
			r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
			r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
			r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO format
		]
		import re
		for pattern in datetime_patterns:
			if re.search(pattern, s):
				return True
		return False
	
	def _parse_datetime(self, val):
		"""Parse datetime value"""
		try:
			from datetime import datetime
			if isinstance(val, str):
				# Try common datetime formats
				formats = [
					'%Y-%m-%d',
					'%Y-%m-%dT%H:%M:%SZ',
					'%Y-%m-%dT%H:%M:%S',
					'%m/%d/%Y',
					'%Y/%m/%d',
					'%d/%m/%Y'
				]
				for fmt in formats:
					try:
						return datetime.strptime(val, fmt)
					except ValueError:
						continue
			return val
		except Exception:
			return val
	
	def _create_plot(self, ax, x_data, y_data, x_col, y_col, plot_type):
		"""Create the actual plot"""
		try:
			if plot_type == PlotType.SCATTER:
				ax.scatter(x_data, y_data, alpha=0.6, s=50, c='#1f77b4', edgecolors='white', linewidth=0.5)
				
			elif plot_type == PlotType.LINE:
				ax.plot(x_data, y_data, marker='o', markersize=4, linewidth=2, alpha=0.8)
				
			elif plot_type == PlotType.BAR:
				if all(isinstance(x, str) for x in x_data[:5]):  # Categorical X
					ax.bar(x_data, y_data, alpha=0.7, color='#1f77b4')
					ax.tick_params(axis='x', rotation=45)
				else:
					ax.bar(x_data, y_data, alpha=0.7, color='#1f77b4')
					
			elif plot_type == PlotType.HISTOGRAM:
				# Use Y data for histogram
				if all(isinstance(y, (int, float)) for y in y_data):
					ax.hist(y_data, bins=min(30, len(set(y_data))), alpha=0.7, color='#1f77b4', edgecolor='white')
					ax.set_xlabel(y_col)
					ax.set_ylabel('Frequency')
				else:
					ax.text(0.5, 0.5, 'Histogram requires numeric data', 
							ha='center', va='center', transform=ax.transAxes, color='white')
					
			elif plot_type == PlotType.BOX:
				if all(isinstance(y, (int, float)) for y in y_data):
					ax.boxplot(y_data, patch_artist=True, 
							  boxprops=dict(facecolor='#1f77b4', alpha=0.7),
							  medianprops=dict(color='white', linewidth=2))
					ax.set_ylabel(y_col)
				else:
					ax.text(0.5, 0.5, 'Box plot requires numeric data', 
							ha='center', va='center', transform=ax.transAxes, color='white')
					
			elif plot_type == PlotType.CORRELATION:
				# Create correlation matrix for all numeric columns
				numeric_data = {}
				for col in self.columns:
					col_data = []
					for row in self.data:
						try:
							val = float(str(row.get(col, '')).replace(',', ''))
							col_data.append(val)
						except (ValueError, TypeError):
							col_data.append(None)
					
					# Only include if more than 50% valid numeric data
					valid_count = len([v for v in col_data if v is not None])
					if valid_count > len(col_data) * 0.5:
						numeric_data[col] = [v for v in col_data if v is not None]
				
				if len(numeric_data) >= 2:
					df = pd.DataFrame({k: pd.Series(v) for k, v in numeric_data.items()})
					corr_matrix = df.corr()
					
					im = ax.imshow(corr_matrix.values, cmap='coolwarm', aspect='auto', vmin=-1, vmax=1)
					ax.set_xticks(range(len(corr_matrix.columns)))
					ax.set_yticks(range(len(corr_matrix.columns)))
					ax.set_xticklabels(corr_matrix.columns, rotation=45, ha='right')
					ax.set_yticklabels(corr_matrix.columns)
					
					# Add colorbar
					cbar = self.figure.colorbar(im, ax=ax)
					cbar.set_label('Correlation', color='white')
					cbar.ax.tick_params(colors='white')
				else:
					ax.text(0.5, 0.5, 'Correlation requires multiple numeric columns', 
							ha='center', va='center', transform=ax.transAxes, color='white')
			
			# Set labels and title (except for histogram and correlation which set their own)
			if plot_type not in [PlotType.HISTOGRAM, PlotType.CORRELATION, PlotType.BOX]:
				ax.set_xlabel(x_col, color='white', fontsize=12)
				ax.set_ylabel(y_col, color='white', fontsize=12)
			
			ax.set_title(f'{plot_type.title()} Plot: {x_col} vs {y_col}', color='white', fontsize=14, pad=20)
			
			# Style the plot
			ax.tick_params(colors='white')
			ax.spines['bottom'].set_color('white')
			ax.spines['top'].set_color('white')
			ax.spines['right'].set_color('white')
			ax.spines['left'].set_color('white')
			ax.grid(True, alpha=0.3, color='white')
			
		except Exception as e:
			ax.text(0.5, 0.5, f'Error creating plot:\n{str(e)}', 
					ha='center', va='center', transform=ax.transAxes, color='white')
	
	def _clear_plot(self):
		"""Clear the current plot"""
		if self.figure:
			self.figure.clear()
			self._create_empty_plot()
			self._update_info("🗑️ Plot cleared")
	
	def _update_info(self, message):
		"""Update the info label"""
		if hasattr(self, 'info_label'):
			self.info_label.configure(text=message)
	
	def _on_x_column_change(self, value):
		"""Handle X column selection change"""
		self.x_column = value
		if hasattr(self, 'x_type_var'):
			# Auto-detect data type for new column
			if self.data and value in self.columns:
				col_data = [row.get(value) for row in self.data[:100] if row.get(value) is not None]
				detected_type = self._detect_data_type(col_data)
				self.x_type_var.set(detected_type)
	
	def _on_y_column_change(self, value):
		"""Handle Y column selection change"""
		self.y_column = value
		if hasattr(self, 'y_type_var'):
			# Auto-detect data type for new column
			if self.data and value in self.columns:
				col_data = [row.get(value) for row in self.data[:100] if row.get(value) is not None]
				detected_type = self._detect_data_type(col_data)
				self.y_type_var.set(detected_type)
	
	def _on_x_type_change(self, value):
		"""Handle X data type change"""
		self.x_data_type = value
	
	def _on_y_type_change(self, value):
		"""Handle Y data type change"""
		self.y_data_type = value
	
	def _on_plot_type_change(self, value):
		"""Handle plot type change"""
		self.plot_type = value
		
		# Update info based on plot type
		info_messages = {
			PlotType.SCATTER: "💡 Scatter plot shows relationship between two variables",
			PlotType.LINE: "💡 Line plot connects data points with lines",
			PlotType.BAR: "💡 Bar plot compares values across categories",
			PlotType.HISTOGRAM: "💡 Histogram shows distribution of values (uses Y column)",
			PlotType.BOX: "💡 Box plot shows data distribution and outliers (uses Y column)",
			PlotType.CORRELATION: "💡 Correlation matrix shows relationships between all numeric columns"
		}
		self._update_info(info_messages.get(value, "💡 Generate plot to visualize your data"))
	
	def _get_var_value(self, var) -> str:
		"""Get value from a variable (handles both CTK and TK vars)"""
		try:
			if hasattr(var, 'get'):
				return var.get()
			elif hasattr(var, '_v'):
				return var._v
			else:
				return str(var) if var is not None else ""
		except Exception:
			return ""
	
	def _get_string_var(self, value: str):
		"""Create a string variable compatible with the current UI framework"""
		try:
			if HAS_CTK:
				from customtkinter import StringVar as CTkStringVar
				return CTkStringVar(value=value)
			else:
				import tkinter as tk
				return tk.StringVar(value=value)
		except Exception:
			# Fallback simple variable
			class _SimpleVar:
				def __init__(self, v):
					self._v = v
				def get(self):
					return self._v
				def set(self, value):
					self._v = value
			return _SimpleVar(value)
	
	def update_data(self, new_data):
		"""Update the plot explorer with new data"""
		self.data = new_data or []
		self.columns = list(new_data[0].keys()) if new_data else []
		
		# Update column options
		if hasattr(self, 'x_column_menu'):
			self.x_column_menu.configure(values=self.columns if self.columns else ["No data"])
		if hasattr(self, 'y_column_menu'):
			self.y_column_menu.configure(values=self.columns if self.columns else ["No data"])
		
		# Reset selections
		if self.columns:
			if hasattr(self, 'x_column_var'):
				self.x_column_var.set(self.columns[0])
			if hasattr(self, 'y_column_var'):
				self.y_column_var.set(self.columns[1] if len(self.columns) > 1 else self.columns[0])
		
		self._update_info(f"📊 Data updated: {len(self.data)} records, {len(self.columns)} columns")


# ---------------------------
# Data Exploration Components
# ---------------------------

class FilterType(Enum):
	CONTAINS = "contains"
	EQUALS = "equals" 
	GREATER = "greater"
	LESS = "less"
	NOT_EMPTY = "not_empty"
	IS_EMPTY = "is_empty"

@dataclass
class ColumnFilter:
	column: str
	filter_type: FilterType
	value: str = ""

class DataExplorer:
	"""Enhanced data exploration widget with efficient pagination for large datasets"""
	
	def __init__(self, parent, data=None):
		self.parent = parent
		self.original_data = data or []
		self.filtered_data = self.original_data.copy()
		self.current_filters = []
		self.sort_column = None
		self.sort_ascending = True
		
		# Enhanced pagination settings
		self.page_size = 50  # Smaller default for better performance
		self.current_page = 0
		self.max_memory_rows = 10000  # Maximum rows to keep in memory
		
		# Performance optimizations
		self.search_index = {}  # For faster text searching
		self.column_cache = {}  # Cache for column statistics
		self.last_search_term = ""
		self.lazy_loading = True  # Enable lazy loading for very large datasets
		
		# Initialize data type detector for intelligent column analysis
		self.data_type_detector = DataTypeDetector()
		
		# Get column names and analyze data types
		self.columns = list(data[0].keys()) if data else []
		self.column_types = self._analyze_column_types() if data else {}
		
		# Build search index for better performance
		self._build_search_index()
		
		self._build_explorer()
	
	def _analyze_column_types(self) -> Dict[str, Dict[str, Any]]:
		"""
		Analyze column data types using the DataTypeDetector.
		
		Returns:
			Dictionary mapping column names to type analysis results
		"""
		column_types = {}
		
		try:
			for column in self.columns:
				# Extract column values
				column_values = [row.get(column) for row in self.original_data[:1000]]  # Sample first 1000
				
				# Get type analysis
				type_summary = self.data_type_detector.get_type_summary(column_values)
				column_types[column] = type_summary
				
			self._log_performance(f"Column type analysis completed for {len(column_types)} columns")
			
		except Exception as e:
			print(f"Warning: Could not analyze column types: {e}")
			# Fallback to basic type detection
			for column in self.columns:
				column_types[column] = {
					'detected_type': 'string',
					'confidence': 0.5,
					'total_count': len(self.original_data),
					'null_count': 0
				}
		
		return column_types
	
	def _build_search_index(self):
		"""Build search index for faster text searching"""
		try:
			if len(self.original_data) > self.max_memory_rows:
				# For very large datasets, use sample-based indexing
				sample_size = min(1000, len(self.original_data))
				sample_data = self.original_data[:sample_size]
				self._log_performance(f"Building search index from {sample_size} sample records (dataset too large)")
			else:
				sample_data = self.original_data
			
			self.search_index = {}
			for idx, row in enumerate(sample_data):
				# Create searchable text for each row
				searchable_text = " ".join(str(row.get(col, "")).lower() for col in self.columns)
				self.search_index[idx] = searchable_text
			
			self._log_performance(f"Search index built for {len(self.search_index)} records")
		except Exception as e:
			print(f"Warning: Could not build search index: {e}")
			self.search_index = {}
	
	def _build_explorer(self):
		"""Build the complete data explorer interface"""
		# Main container
		self.main_frame = ctk.CTkFrame(self.parent)
		self.main_frame.pack(fill="both", expand=True)
		
		# Top toolbar
		self._build_toolbar()
		
		# Filter panel (collapsible)
		self._build_filter_panel()
		
		# Data table
		self._build_table()
		
		# Bottom pagination and stats
		self._build_bottom_panel()
		
		# Initial data load
		self._refresh_table()
	
	def _build_toolbar(self):
		"""Build the top toolbar with main actions"""
		toolbar = ctk.CTkFrame(self.main_frame)
		toolbar.pack(fill="x", padx=10, pady=(10, 5))
		
		# Left side - main actions
		left_frame = ctk.CTkFrame(toolbar)
		left_frame.pack(side="left", fill="x", expand=True)
		
		# Refresh data button
		refresh_btn = ctk.CTkButton(
			left_frame,
			text="🔄 Refresh Data",
			command=self._refresh_data,
			width=120,
			height=32
		)
		refresh_btn.pack(side="left", padx=(10, 5), pady=8)
		
		# Export filtered data
		export_btn = ctk.CTkButton(
			left_frame,
			text="📁 Export Filtered",
			command=self._export_filtered,
			width=120,
			height=32
		)
		export_btn.pack(side="left", padx=5, pady=8)
		
		# Quick search
		ctk.CTkLabel(left_frame, text="🔍 Quick Search:").pack(side="left", padx=(15, 5), pady=8)
		self.quick_search = ctk.CTkEntry(left_frame, placeholder_text="Search all columns...", width=200)
		self.quick_search.pack(side="left", padx=5, pady=8)
		self.quick_search.bind("<KeyRelease>", self._on_quick_search)
		
		# Right side - view options
		right_frame = ctk.CTkFrame(toolbar)
		right_frame.pack(side="right")
		
		# Toggle filters panel
		self.filters_visible = True
		self.toggle_filters_btn = ctk.CTkButton(
			right_frame,
			text="🔽 Filters",
			command=self._toggle_filters,
			width=100,
			height=32
		)
		self.toggle_filters_btn.pack(side="right", padx=10, pady=8)
	
	def _build_filter_panel(self):
		"""Build the advanced filters panel"""
		self.filter_frame = ctk.CTkFrame(self.main_frame)
		self.filter_frame.pack(fill="x", padx=10, pady=5)
		
		# Filter header
		filter_header = ctk.CTkFrame(self.filter_frame)
		filter_header.pack(fill="x", padx=10, pady=(10, 5))
		
		ctk.CTkLabel(
			filter_header,
			text="🎯 Advanced Filters",
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		).pack(side="left", pady=5)
		
		# Clear all filters
		clear_btn = ctk.CTkButton(
			filter_header,
			text="🗑️ Clear All",
			command=self._clear_all_filters,
			width=80,
			height=25
		)
		clear_btn.pack(side="right", pady=2)
		
		# Add filter button
		add_filter_btn = ctk.CTkButton(
			filter_header,
			text="➕ Add Filter",
			command=self._add_filter_row,
			width=100,
			height=25
		)
		add_filter_btn.pack(side="right", padx=(0, 10), pady=2)
		
		# Scrollable filter rows container
		self.filters_container = ctk.CTkScrollableFrame(self.filter_frame, height=150)
		self.filters_container.pack(fill="x", padx=10, pady=(0, 10))
		
		self.filter_rows = []
	
	def _build_table(self):
		"""Build the main data table with enhanced pagination for large datasets"""
		table_frame = ctk.CTkFrame(self.main_frame)
		table_frame.pack(fill="both", expand=True, padx=10, pady=5)
		
		# Table header with performance info
		header_frame = ctk.CTkFrame(table_frame)
		header_frame.pack(fill="x", padx=10, pady=(10, 0))
		
		ctk.CTkLabel(
			header_frame,
			text="📊 Data Table (Enhanced Pagination)",
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		).pack(side="left", pady=5)
		
		# Performance indicator
		self.perf_label = ctk.CTkLabel(
			header_frame,
			text="⚡ Ready",
			font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8),
			text_color="#00AA00"
		)
		self.perf_label.pack(side="right", pady=5)
		
		# Create the table area
		self.table_container = ctk.CTkFrame(table_frame)
		self.table_container.pack(fill="both", expand=True, padx=10, pady=10)
		
		# Use optimized Treeview with performance enhancements
		try:
			import tkinter.ttk as ttk
			
			# Create frame for treeview and scrollbars
			tree_frame = self._get_tk_frame(self.table_container)
			tree_frame.pack(fill="both", expand=True)
			
			# Create treeview with performance optimizations
			self.tree = ttk.Treeview(tree_frame, show='tree headings')
			
			# Performance: Disable automatic column width calculation
			self.tree.configure(selectmode='browse')  # Single selection for better performance
			
			# Scrollbars
			v_scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
			h_scrollbar = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
			
			self.tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
			
			# Pack scrollbars and treeview
			v_scrollbar.pack(side="right", fill="y")
			h_scrollbar.pack(side="bottom", fill="x")
			self.tree.pack(side="left", fill="both", expand=True)
			
			# Configure columns with optimized settings
			if self.columns:
				self.tree["columns"] = self.columns
				self.tree.column("#0", width=0, stretch=False)  # Hide tree column
				
				# Calculate optimal column widths once
				column_widths = self._calculate_optimal_column_widths()
				
				for col in self.columns:
					width = column_widths.get(col, 120)
					self.tree.column(col, width=width, anchor="w", stretch=False)  # stretch=False for performance
					self.tree.heading(col, text=col, command=lambda c=col: self._sort_by_column(c))
			
			# Configure treeview styling for dark theme with performance optimizations
			style = ttk.Style()
			style.theme_use("clam")
			style.configure("Treeview", 
						  background="#404040",
						  foreground="white",
						  rowheight=22,  # Slightly smaller for more rows visible
						  fieldbackground="#404040",
						  relief="flat")  # Flat relief for better performance
			style.configure("Treeview.Heading",
						  background="#2b2b2b",
						  foreground="white",
						  relief="flat")
			style.map("Treeview.Heading",
					 background=[('active', '#1f538d')])
			
			# Bind events for performance monitoring
			self.tree.bind("<<TreeviewSelect>>", self._on_row_select)
			
		except Exception as e:
			# Fallback to optimized text display
			self._log_performance(f"Treeview failed ({e}), using text fallback")
			self._build_text_fallback()
	
	def _calculate_optimal_column_widths(self):
		"""Calculate optimal column widths based on sample data"""
		try:
			widths = {}
			sample_size = min(50, len(self.filtered_data))  # Sample only 50 rows for speed
			
			for col in self.columns:
				# Start with header width
				header_width = len(col) * 8 + 40
				max_content_width = header_width
				
				# Check sample data for optimal width
				for i in range(sample_size):
					if i < len(self.filtered_data):
						value = str(self.filtered_data[i].get(col, ""))
						content_width = min(len(value) * 8 + 20, 300)  # Cap at 300px
						max_content_width = max(max_content_width, content_width)
				
				# Set reasonable bounds
				widths[col] = max(80, min(max_content_width, 250))
			
			return widths
		except Exception:
			# Fallback to uniform widths
			return {col: 120 for col in self.columns}
	
	def _build_text_fallback(self):
		"""Build optimized text-based table fallback"""
		self.tree = ctk.CTkTextbox(self.table_container)
		self.tree.pack(fill="both", expand=True)
		
		# Configure for better performance
		try:
			if hasattr(self.tree, 'configure'):
				self.tree.configure(wrap="none", state="normal")
		except Exception:
			pass
	
	def _on_row_select(self, event):
		"""Handle row selection events"""
		try:
			selection = self.tree.selection()
			if selection:
				item = self.tree.item(selection[0])
				self._log_performance(f"Row selected: {item.get('text', 'Unknown')}")
		except Exception:
			pass
	
	def _build_bottom_panel(self):
		"""Build enhanced pagination and statistics panel"""
		bottom_frame = ctk.CTkFrame(self.main_frame)
		bottom_frame.pack(fill="x", padx=10, pady=(5, 10))
		
		# Left side - enhanced statistics
		stats_frame = ctk.CTkFrame(bottom_frame)
		stats_frame.pack(side="left", fill="x", expand=True)
		
		self.stats_label = ctk.CTkLabel(
			stats_frame,
			text="No data loaded",
			font=ctk.CTkFont(size=12) if HAS_CTK else ("Arial", 10)
		)
		self.stats_label.pack(side="left", padx=10, pady=8)
		
		# Performance indicator
		self.load_time_label = ctk.CTkLabel(
			stats_frame,
			text="",
			font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8),
			text_color="#888888"
		)
		self.load_time_label.pack(side="left", padx=(20, 0), pady=8)
		
		# Right side - enhanced pagination
		pagination_frame = ctk.CTkFrame(bottom_frame)
		pagination_frame.pack(side="right")
		
		# First page button
		self.first_btn = ctk.CTkButton(
			pagination_frame,
			text="⏮",
			command=self._first_page,
			width=40,
			height=30
		)
		self.first_btn.pack(side="left", padx=(10, 2), pady=5)
		
		# Previous page button
		self.prev_btn = ctk.CTkButton(
			pagination_frame,
			text="◀",
			command=self._prev_page,
			width=40,
			height=30
		)
		self.prev_btn.pack(side="left", padx=2, pady=5)
		
		# Page info with jump capability
		page_info_frame = ctk.CTkFrame(pagination_frame)
		page_info_frame.pack(side="left", padx=5, pady=5)
		
		# Page jump entry
		self.page_jump_var = self._get_string_var("1")
		self.page_jump_entry = ctk.CTkEntry(
			page_info_frame,
			textvariable=self.page_jump_var,
			width=50,
			height=25
		)
		self.page_jump_entry.pack(side="left", padx=2, pady=2)
		self.page_jump_entry.bind("<Return>", self._jump_to_page)
		
		self.page_label = ctk.CTkLabel(
			page_info_frame,
			text="of 1",
			font=ctk.CTkFont(size=12) if HAS_CTK else ("Arial", 10)
		)
		self.page_label.pack(side="left", padx=(2, 5), pady=2)
		
		# Next page button
		self.next_btn = ctk.CTkButton(
			pagination_frame,
			text="▶",
			command=self._next_page,
			width=40,
			height=30
		)
		self.next_btn.pack(side="left", padx=2, pady=5)
		
		# Last page button
		self.last_btn = ctk.CTkButton(
			pagination_frame,
			text="⏭",
			command=self._last_page,
			width=40,
			height=30
		)
		self.last_btn.pack(side="left", padx=(2, 10), pady=5)
		
		# Page size selector with smart defaults
		size_frame = ctk.CTkFrame(pagination_frame)
		size_frame.pack(side="left", padx=(15, 10), pady=5)
		
		ctk.CTkLabel(size_frame, text="Rows:", font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8)).pack(side="left", padx=(5, 2))
		
		try:
			self.page_size_var = self._get_string_var(str(self.page_size))
			# Smart page sizes based on dataset size
			size_options = self._get_smart_page_sizes()
			page_size_menu = ctk.CTkOptionMenu(
				size_frame,
				values=size_options,
				variable=self.page_size_var,
				command=self._on_page_size_change,
				width=80,
				height=25
			)
			page_size_menu.pack(side="left", padx=2, pady=2)
		except Exception:
			# Fallback entry
			self.page_size_entry = ctk.CTkEntry(size_frame, width=60, height=25)
			self.page_size_entry.pack(side="left", padx=2, pady=2)
			self._entry_set(self.page_size_entry, str(self.page_size))
	
	def _get_smart_page_sizes(self):
		"""Get smart page size options based on dataset size"""
		total_rows = len(self.filtered_data)
		
		if total_rows <= 100:
			return ["25", "50", "100"]
		elif total_rows <= 1000:
			return ["25", "50", "100", "200"]
		elif total_rows <= 10000:
			return ["50", "100", "200", "500"]
		elif total_rows <= 100000:
			return ["50", "100", "200", "500", "1000"]
		else:
			return ["25", "50", "100", "200", "500"]  # Smaller sizes for very large datasets
	
	def _jump_to_page(self, event=None):
		"""Jump to a specific page"""
		try:
			page_num = int(self._get_var_value(self.page_jump_var))
			total_pages = self._get_total_pages()
			
			if 1 <= page_num <= total_pages:
				self.current_page = page_num - 1  # Convert to 0-based
				self._refresh_table()
			else:
				self._log_performance(f"Invalid page number: {page_num} (max: {total_pages})")
				# Reset to current page
				self.page_jump_var.set(str(self.current_page + 1))
		except ValueError:
			# Reset to current page if invalid input
			self.page_jump_var.set(str(self.current_page + 1))
		except Exception as e:
			print(f"Error jumping to page: {e}")
	
	def _first_page(self):
		"""Go to first page"""
		if self.current_page > 0:
			self.current_page = 0
			self._refresh_table()
	
	def _last_page(self):
		"""Go to last page"""
		total_pages = self._get_total_pages()
		last_page = max(0, total_pages - 1)
		if self.current_page != last_page:
			self.current_page = last_page
			self._refresh_table()
	
	def _add_filter_row(self):
		"""Add a new filter row"""
		row_frame = ctk.CTkFrame(self.filters_container)
		row_frame.pack(fill="x", pady=2)
		
		# Column selector
		try:
			column_var = self._get_string_var(self.columns[0] if self.columns else "")
			column_menu = ctk.CTkOptionMenu(
				row_frame,
				values=self.columns,
				variable=column_var,
				width=150
			)
			column_menu.pack(side="left", padx=5, pady=5)
		except Exception:
			column_var = None
			column_menu = ctk.CTkEntry(row_frame, placeholder_text="Column", width=150)
			column_menu.pack(side="left", padx=5, pady=5)
		
		# Filter type selector
		try:
			filter_type_var = self._get_string_var("contains")
			filter_type_menu = ctk.CTkOptionMenu(
				row_frame,
				values=["contains", "equals", "greater", "less", "not_empty", "is_empty"],
				variable=filter_type_var,
				width=120
			)
			filter_type_menu.pack(side="left", padx=5, pady=5)
		except Exception:
			filter_type_var = None
			filter_type_menu = ctk.CTkEntry(row_frame, placeholder_text="Type", width=120)
			filter_type_menu.pack(side="left", padx=5, pady=5)
		
		# Filter value
		value_entry = ctk.CTkEntry(row_frame, placeholder_text="Filter value...", width=200)
		value_entry.pack(side="left", padx=5, pady=5)
		
		# Apply filter button
		apply_btn = ctk.CTkButton(
			row_frame,
			text="Apply",
			command=lambda: self._apply_filter_row(column_var, filter_type_var, value_entry),
			width=60,
			height=30
		)
		apply_btn.pack(side="left", padx=5, pady=5)
		
		# Remove filter button
		remove_btn = ctk.CTkButton(
			row_frame,
			text="✕",
			command=lambda: self._remove_filter_row(row_frame),
			width=30,
			height=30
		)
		remove_btn.pack(side="left", padx=5, pady=5)
		
		# Store reference
		self.filter_rows.append({
			'frame': row_frame,
			'column_var': column_var,
			'filter_type_var': filter_type_var,
			'value_entry': value_entry
		})
	
	def _apply_filter_row(self, column_var, filter_type_var, value_entry):
		"""Apply filter from a specific row"""
		try:
			column = self._get_var_value(column_var)
			filter_type = self._get_var_value(filter_type_var)
			value = self._entry_get(value_entry)
			
			if column and filter_type:
				# Remove existing filter for this column
				self.current_filters = [f for f in self.current_filters if f.column != column]
				
				# Add new filter
				new_filter = ColumnFilter(column, FilterType(filter_type), value)
				self.current_filters.append(new_filter)
				
				self._apply_filters()
		except Exception as e:
			print(f"Error applying filter: {e}")
	
	def _remove_filter_row(self, row_frame):
		"""Remove a filter row"""
		try:
			# Find and remove from filter_rows
			self.filter_rows = [row for row in self.filter_rows if row['frame'] != row_frame]
			
			# Destroy the frame
			row_frame.destroy()
			
			# Reapply filters
			self._apply_filters()
		except Exception as e:
			print(f"Error removing filter row: {e}")
	
	def _apply_single_filter(self, data, filter_obj: ColumnFilter):
		"""Apply a single filter to the data"""
		try:
			filtered = []
			
			for row in data:
				value = str(row.get(filter_obj.column, ""))
				
				if filter_obj.filter_type == FilterType.CONTAINS:
					if filter_obj.value.lower() in value.lower():
						filtered.append(row)
				elif filter_obj.filter_type == FilterType.EQUALS:
					if value.lower() == filter_obj.value.lower():
						filtered.append(row)
				elif filter_obj.filter_type == FilterType.GREATER:
					try:
						if float(value) > float(filter_obj.value):
							filtered.append(row)
					except ValueError:
						pass
				elif filter_obj.filter_type == FilterType.LESS:
					try:
						if float(value) < float(filter_obj.value):
							filtered.append(row)
					except ValueError:
						pass
				elif filter_obj.filter_type == FilterType.NOT_EMPTY:
					if value.strip():
						filtered.append(row)
				elif filter_obj.filter_type == FilterType.IS_EMPTY:
					if not value.strip():
						filtered.append(row)
			
			return filtered
			
		except Exception as e:
			print(f"Error in single filter: {e}")
			return data
	
	def _refresh_table(self):
		"""Refresh the table display with current filtered data - OPTIMIZED"""
		import time
		start_time = time.perf_counter()
		
		try:
			if hasattr(self.tree, 'delete'):
				# PERFORMANCE: Clear existing items efficiently
				self.tree.delete(*self.tree.get_children())
				
				# Calculate pagination efficiently
				total_rows = len(self.filtered_data)
				start_idx = self.current_page * self.page_size
				end_idx = min(start_idx + self.page_size, total_rows)
				page_data = self.filtered_data[start_idx:end_idx]
				
				# PERFORMANCE: Batch insert for better speed
				items_to_insert = []
				for i, row in enumerate(page_data):
					values = [str(row.get(col, ""))[:100] for col in self.columns]  # Truncate long values
					row_id = self.tree.insert("", "end", text=str(start_idx + i + 1), values=values)
					items_to_insert.append(row_id)
				
				# Update pagination info efficiently
				total_pages = self._get_total_pages()
				self.page_label.configure(text=f"of {total_pages}")
				self.page_jump_var.set(str(self.current_page + 1))
				
				# Update navigation buttons efficiently
				self._update_button_state(self.first_btn, "normal" if self.current_page > 0 else "disabled")
				self._update_button_state(self.prev_btn, "normal" if self.current_page > 0 else "disabled")
				self._update_button_state(self.next_btn, "normal" if self.current_page < total_pages - 1 else "disabled")
				self._update_button_state(self.last_btn, "normal" if self.current_page < total_pages - 1 else "disabled")
				
			else:
				# Fallback text display
				self._refresh_text_table()
			
			# Update statistics and performance info
			load_time = time.perf_counter() - start_time
			self._update_stats()
			self.load_time_label.configure(text=f"⚡ Loaded in {load_time*1000:.1f}ms")
			
			# Update performance indicator
			if hasattr(self, 'perf_label'):
				if load_time < 0.1:
					self.perf_label.configure(text="⚡ Excellent", text_color="#00AA00")
				elif load_time < 0.5:
					self.perf_label.configure(text="⚡ Good", text_color="#FFAA00")
				else:
					self.perf_label.configure(text="⚡ Slow", text_color="#FF6600")
			
		except Exception as e:
			print(f"Error refreshing table: {e}")
			if hasattr(self, 'perf_label'):
				self.perf_label.configure(text="❌ Error", text_color="#FF0000")
	
	def _apply_filters(self):
		"""Apply all current filters to the data - OPTIMIZED"""
		import time
		start_time = time.perf_counter()
		
		try:
			# Start with original data
			self.filtered_data = self.original_data.copy()
			
			# PERFORMANCE: Early exit if no filters
			quick_search = self._entry_get(self.quick_search) if hasattr(self, 'quick_search') else ""
			if not quick_search and not self.current_filters:
				self.current_page = 0
				self._refresh_table()
				return
			
			# Apply quick search first (most common filter)
			if quick_search and quick_search != self.last_search_term:
				quick_search_lower = quick_search.lower()
				
				# PERFORMANCE: Use search index if available
				if self.search_index and len(self.original_data) <= self.max_memory_rows:
					matching_indices = [
						idx for idx, searchable_text in self.search_index.items()
						if quick_search_lower in searchable_text
					]
					self.filtered_data = [self.original_data[idx] for idx in matching_indices if idx < len(self.original_data)]
				else:
					# Fallback to linear search for large datasets
					self.filtered_data = [
						row for row in self.filtered_data
						if any(quick_search_lower in str(row.get(col, "")).lower() for col in self.columns)
					]
				
				self.last_search_term = quick_search
			
			# Apply column filters
			for filter_obj in self.current_filters:
				self.filtered_data = self._apply_single_filter(self.filtered_data, filter_obj)
			
			# Reset to first page when filters change
			self.current_page = 0
			
			# Refresh display
			filter_time = time.perf_counter() - start_time
			self._refresh_table()
			
			# Log performance for large filter operations
			if filter_time > 0.5:
				self._log_performance(f"Filter applied in {filter_time:.2f}s ({len(self.filtered_data):,} results)")
			
		except Exception as e:
			print(f"Error applying filters: {e}")
	
	def _log_performance(self, message: str):
		"""Log performance information"""
		try:
			print(f"[PERF] {message}")
		except Exception:
			pass
	
	def _get_total_pages(self) -> int:
		"""Get total number of pages"""
		return max(1, (len(self.filtered_data) + self.page_size - 1) // self.page_size)
	
	def _update_button_state(self, button, state: str):
		"""Update button state with proper handling"""
		try:
			if hasattr(button, 'configure'):
				button.configure(state=state)
		except Exception:
			pass
	
	def _on_first_page(self):
		"""Navigate to first page"""
		try:
			self.current_page = 0
			self._refresh_table()
		except Exception as e:
			print(f"Error navigating to first page: {e}")
	
	def _on_last_page(self):
		"""Navigate to last page"""
		try:
			total_pages = self._get_total_pages()
			self.current_page = max(0, total_pages - 1)
			self._refresh_table()
		except Exception as e:
			print(f"Error navigating to last page: {e}")
	
	def _on_jump_to_page(self, event=None):
		"""Jump to specific page"""
		try:
			page_str = self._entry_get(self.page_jump_entry) if hasattr(self, 'page_jump_entry') else ""
			if page_str.isdigit():
				page_num = int(page_str) - 1  # Convert to 0-based
				total_pages = self._get_total_pages()
				if 0 <= page_num < total_pages:
					self.current_page = page_num
					self._refresh_table()
				else:
					# Reset to current page if invalid
					self._entry_set(self.page_jump_entry, str(self.current_page + 1))
		except Exception as e:
			print(f"Error jumping to page: {e}")
	
	def _calculate_optimal_page_size(self, total_rows: int) -> int:
		"""Calculate optimal page size based on dataset size"""
		if total_rows <= 1000:
			return 100
		elif total_rows <= 10000:
			return 250
		elif total_rows <= 100000:
			return 500
		else:
			return 1000
	
	def _calculate_column_widths(self, sample_data: list) -> dict:
		"""Calculate optimal column widths based on sample data"""
		widths = {}
		try:
			for col in self.columns:
				# Start with header width
				max_width = len(col) * 8
				
				# Check sample data
				for row in sample_data[:min(100, len(sample_data))]:  # Sample first 100 rows
					value_str = str(row.get(col, ""))
					max_width = max(max_width, len(value_str) * 8)
				
				# Set reasonable bounds
				widths[col] = max(80, min(max_width, 300))
		except Exception:
			# Fallback to default widths
			for col in self.columns:
				widths[col] = 120
		
		return widths
	
	def _entry_set(self, entry, value: str):
		"""Set the value of an entry widget"""
		try:
			if hasattr(entry, 'delete') and hasattr(entry, 'insert'):
				entry.delete(0, "end")
				entry.insert(0, value)
		except Exception:
			pass
	
	def _refresh_text_table(self):
		"""Fallback method for text-based table display"""
		try:
			# Clear existing items
			for item in self.tree.get_children():
				self.tree.delete(item)
			
			# Calculate pagination
			total_rows = len(self.filtered_data)
			start_idx = self.current_page * self.page_size
			end_idx = min(start_idx + self.page_size, total_rows)
			page_data = self.filtered_data[start_idx:end_idx]
			
			# Create table data
			if not page_data:
				self.tree.insert("", "end", text="No data to display", values=[""] * len(self.columns))
				return
			
			# Data rows
			for i, row in enumerate(page_data):
				values = [str(row.get(col, ""))[:50] for col in self.columns]  # Limit for readability
				self.tree.insert("", "end", text=str(start_idx + i + 1), values=values)
			
		except Exception as e:
			print(f"Error in text table refresh: {e}")
	
	def _update_stats(self):
		"""Update the statistics display"""
		try:
			total_original = len(self.original_data)
			total_filtered = len(self.filtered_data)
			
			if total_filtered != total_original:
				stats_text = f"Showing {total_filtered:,} of {total_original:,} records ({len(self.current_filters)} filters active)"
			else:
				stats_text = f"Showing {total_original:,} records"
			
			self.stats_label.configure(text=stats_text)
			
		except Exception as e:
			print(f"Error updating stats: {e}")
	
	def _sort_by_column(self, column):
		"""Sort data by the specified column"""
		try:
			if self.sort_column == column:
				self.sort_ascending = not self.sort_ascending
			else:
				self.sort_column = column
				self.sort_ascending = True
			
			# Sort the filtered data
			def sort_key(row):
				value = row.get(column, "")
				# Try to convert to number for proper numeric sorting
				try:
					return float(value)
				except (ValueError, TypeError):
					return str(value).lower()
			
			self.filtered_data.sort(key=sort_key, reverse=not self.sort_ascending)
			
			# Update column header to show sort direction
			if hasattr(self.tree, 'heading'):
				sort_indicator = " ↑" if self.sort_ascending else " ↓"
				self.tree.heading(column, text=f"{column}{sort_indicator}")
				
				# Clear indicators from other columns
				for col in self.columns:
					if col != column:
						self.tree.heading(col, text=col)
			
			# Refresh display
			self._refresh_table()
			
		except Exception as e:
			print(f"Error sorting: {e}")
	
	def _on_quick_search(self, event=None):
		"""Handle quick search input"""
		# Debounce the search
		if hasattr(self, '_search_timer'):
			self.parent.after_cancel(self._search_timer)
		
		self._search_timer = self.parent.after(300, self._apply_filters)
	
	def _toggle_filters(self):
		"""Toggle the visibility of the filters panel"""
		try:
			if self.filters_visible:
				self.filter_frame.pack_forget()
				self.toggle_filters_btn.configure(text="🔽 Show Filters")
				self.filters_visible = False
			else:
				self.filter_frame.pack(fill="x", padx=10, pady=5, before=self.table_container.master)
				self.toggle_filters_btn.configure(text="🔼 Hide Filters")
				self.filters_visible = True
		except Exception as e:
			print(f"Error toggling filters: {e}")
	
	def _clear_all_filters(self):
		"""Clear all active filters"""
		try:
			# Clear filter objects
			self.current_filters.clear()
			
			# Clear quick search
			if hasattr(self, 'quick_search'):
				self.quick_search.delete(0, "end")
			
			# Clear filter rows
			for row in self.filter_rows:
				row['frame'].destroy()
			self.filter_rows.clear()
			
			# Reset sort
			self.sort_column = None
			self.sort_ascending = True
			
			# Refresh display
			self._apply_filters()
			
		except Exception as e:
			print(f"Error clearing filters: {e}")
	
	def _prev_page(self):
		"""Go to previous page"""
		if self.current_page > 0:
			self.current_page -= 1
			self._refresh_table()
	
	def _next_page(self):
		"""Go to next page"""
		total_pages = (len(self.filtered_data) + self.page_size - 1) // self.page_size
		if self.current_page < total_pages - 1:
			self.current_page += 1
			self._refresh_table()
	
	def _on_page_size_change(self, new_size):
		"""Handle page size change"""
		try:
			self.page_size = int(new_size)
			self.current_page = 0  # Reset to first page
			self._refresh_table()
		except Exception as e:
			print(f"Error changing page size: {e}")
	
	def _refresh_data(self):
		"""Refresh data from the server"""
		# This will be implemented to call the parent's data refresh method
		if hasattr(self.parent, '_refresh_exploration_data'):
			self.parent._refresh_exploration_data()
	
	def _export_filtered(self):
		"""Export the currently filtered data"""
		# Try to access the app instance through the app_instance attribute
		app_instance = getattr(self, 'app_instance', None)
		if app_instance and hasattr(app_instance, '_export_exploration_data'):
			app_instance._export_exploration_data(self.filtered_data)
		elif hasattr(self.parent, '_export_exploration_data'):
			self.parent._export_exploration_data(self.filtered_data)
		else:
			# Fallback: implement direct export here
			self._export_filtered_direct()
	
	def _export_filtered_direct(self):
		"""Direct export method as fallback"""
		try:
			if not self.filtered_data:
				print("❌ No data to export")
				return
			
			# Try to import file dialog
			try:
				if HAS_CTK and hasattr(ctk, "filedialog") and ctk.filedialog:
					from tkinter import filedialog as fd
				else:
					from tkinter import filedialog as fd
			except Exception:
				import tkinter.filedialog as fd
			
			# Ask for save location
			file_path = fd.asksaveasfilename(
				defaultextension=".csv",
				filetypes=[("CSV Files", "*.csv"), ("JSON Files", "*.json"), ("All Files", "*.*")],
				initialfile=f"exploration_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
			)
			
			if file_path:
				save_path = Path(file_path)
				
				if save_path.suffix.lower() == '.json':
					# Export as JSON
					import json
					save_path.write_text(json.dumps(self.filtered_data, indent=2, ensure_ascii=False), encoding='utf-8')
				else:
					# Export as CSV
					if self.filtered_data:
						import csv
						with open(save_path, 'w', newline='', encoding='utf-8') as csvfile:
							writer = csv.DictWriter(csvfile, fieldnames=self.filtered_data[0].keys())
							writer.writeheader()
							writer.writerows(self.filtered_data)
				
				print(f"✅ Exported {len(self.filtered_data):,} records to: {save_path}")
			
		except Exception as e:
			print(f"❌ Export failed: {e}")

	def _entry_get(self, entry) -> str:
		"""Get value from entry widget"""
		try:
			return entry.get() if hasattr(entry, 'get') else ""
		except Exception:
			return ""
	
	def _get_var_value(self, var) -> str:
		"""Get value from a variable (handles both CTK and TK vars)"""
		try:
			if hasattr(var, 'get'):
				return var.get()
			elif hasattr(var, '_v'):
				return var._v
			else:
				return str(var) if var is not None else ""
		except Exception:
			return ""
	
	def _get_string_var(self, value: str):
		"""Create a string variable compatible with the current UI framework"""
		try:
			if HAS_CTK:
				from customtkinter import StringVar as CTkStringVar
				return CTkStringVar(value=value)
			else:
				import tkinter as tk
				return tk.StringVar(value=value)
		except Exception:
			# Fallback simple variable
			class _SimpleVar:
				def __init__(self, v):
					self._v = v
				def get(self):
					return self._v
				def set(self, value):
					self._v = value
			return _SimpleVar(value)
	
	def _get_tk_frame(self, parent):
		"""Get a tkinter Frame for embedding ttk widgets"""
		try:
			import tkinter as tk
			frame = tk.Frame(parent, bg='#2b2b2b')
			return frame
		except Exception:
			return parent
	
	def update_data(self, new_data):
		"""Update the explorer with new data"""
		self.original_data = new_data or []
		self.columns = list(new_data[0].keys()) if new_data else []
		self.current_page = 0
		self._apply_filters()


# ---------------------------
# UI Helpers
# ---------------------------


def _format_json(obj: Any) -> str:
	try:
		return json.dumps(obj, indent=2, ensure_ascii=False)
	except Exception:
		return str(obj)


class FloatingDataExplorer:
	"""Floating data explorer with pagination support"""
	
	def __init__(self, parent_window, data, filter_info=""):
		self.parent_window = parent_window
		self.data = data or []
		self.filter_info = filter_info
		
		# Pagination settings
		self.page_size = 100  # Default page size for floating table
		self.current_page = 0
		
		# Get column names
		self.columns = list(data[0].keys()) if data else []
		
		self._build_floating_explorer()
	
	def _build_floating_explorer(self):
		"""Build the floating table explorer interface"""
		import tkinter as tk
		from tkinter import ttk
		
		# Window header
		header_frame = tk.Frame(self.parent_window, bg='#2b2b2b', height=50)
		header_frame.pack(fill='x', padx=10, pady=(10, 0))
		header_frame.pack_propagate(False)
		
		# Title and info
		title_label = tk.Label(header_frame, text="📊 Data Table (Floating)", 
							  bg='#2b2b2b', fg='white', font=('Arial', 14, 'bold'))
		title_label.pack(side='left', pady=15)
		
		# Data info with filter status
		data_info = f"Records: {len(self.data):,}{self.filter_info} | Columns: {len(self.columns)}"
		info_label = tk.Label(header_frame, text=data_info, 
							 bg='#2b2b2b', fg='#888888', font=('Arial', 10))
		info_label.pack(side='right', pady=15)
		
		# Main content area
		content_frame = tk.Frame(self.parent_window, bg='#2b2b2b')
		content_frame.pack(fill='both', expand=True, padx=10, pady=(0, 10))
		
		# Table frame
		table_frame = tk.Frame(content_frame, bg='#2b2b2b')
		table_frame.pack(fill='both', expand=True, pady=(0, 10))
		
		self._build_table(table_frame)
		
		# Pagination and controls
		controls_frame = tk.Frame(content_frame, bg='#2b2b2b', height=50)
		controls_frame.pack(fill='x')
		controls_frame.pack_propagate(False)
		
		self._build_pagination_controls(controls_frame)
	
	def _build_table(self, parent):
		"""Build the data table with scrollbars"""
		import tkinter as tk
		from tkinter import ttk
		
		# Create frame for treeview and scrollbars
		tree_frame = tk.Frame(parent, bg='#2b2b2b')
		tree_frame.pack(fill="both", expand=True)
		
		# Create treeview
		self.tree = ttk.Treeview(tree_frame, columns=self.columns, show='tree headings')
		
		# Configure columns
		self.tree.column("#0", width=60, stretch=False)  # Row number column
		for col in self.columns:
			self.tree.column(col, width=120, anchor="w")
			self.tree.heading(col, text=col)
		
		# Add scrollbars
		v_scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
		h_scrollbar = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
		self.tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
		
		# Pack scrollbars and treeview
		v_scrollbar.pack(side="right", fill="y")
		h_scrollbar.pack(side="bottom", fill="x")
		self.tree.pack(side="left", fill="both", expand=True)
		
		# Style the treeview for dark theme
		style = ttk.Style()
		style.theme_use("clam")
		style.configure("Treeview", 
					  background="#404040",
					  foreground="white",
					  rowheight=24,
					  fieldbackground="#404040")
		style.configure("Treeview.Heading",
					  background="#2b2b2b",
					  foreground="white",
					  font=('Arial', 9, 'bold'))
		style.map("Treeview.Heading",
				 background=[('active', '#1f538d')])
		style.map("Treeview",
				 background=[('selected', '#1f538d')])
		
		# Load initial page
		self._load_page()
	
	def _build_pagination_controls(self, parent):
		"""Build pagination controls"""
		import tkinter as tk
		from tkinter import ttk
		
		# Left side - page info
		page_info_frame = tk.Frame(parent, bg='#2b2b2b')
		page_info_frame.pack(side='left', padx=10, pady=10)
		
		self.page_info_label = tk.Label(page_info_frame, text="", 
										bg='#2b2b2b', fg='#888888', font=('Arial', 10))
		self.page_info_label.pack()
		
		# Center - navigation buttons
		nav_frame = tk.Frame(parent, bg='#2b2b2b')
		nav_frame.pack(side='left', padx=20, pady=10)
		
		# First page
		self.first_btn = tk.Button(nav_frame, text="⏮", 
								  command=self._first_page,
								  bg='#1f538d', fg='white', font=('Arial', 10),
								  width=3, state='disabled')
		self.first_btn.pack(side='left', padx=2)
		
		# Previous page
		self.prev_btn = tk.Button(nav_frame, text="◀", 
								 command=self._prev_page,
								 bg='#1f538d', fg='white', font=('Arial', 10),
								 width=3, state='disabled')
		self.prev_btn.pack(side='left', padx=2)
		
		# Page size selector
		size_frame = tk.Frame(nav_frame, bg='#2b2b2b')
		size_frame.pack(side='left', padx=(10, 10))
		
		tk.Label(size_frame, text="Size:", bg='#2b2b2b', fg='white', font=('Arial', 9)).pack(side='left')
		
		self.page_size_var = tk.StringVar(value=str(self.page_size))
		page_size_combo = ttk.Combobox(size_frame, textvariable=self.page_size_var,
									  values=['50', '100', '200', '500'], width=5,
									  state='readonly')
		page_size_combo.pack(side='left', padx=(5, 0))
		page_size_combo.bind('<<ComboboxSelected>>', self._on_page_size_change)
		
		# Next page
		self.next_btn = tk.Button(nav_frame, text="▶", 
								 command=self._next_page,
								 bg='#1f538d', fg='white', font=('Arial', 10),
								 width=3)
		self.next_btn.pack(side='left', padx=2)
		
		# Last page
		self.last_btn = tk.Button(nav_frame, text="⏭", 
								 command=self._last_page,
								 bg='#1f538d', fg='white', font=('Arial', 10),
								 width=3)
		self.last_btn.pack(side='left', padx=2)
		
		# Right side - close button
		close_frame = tk.Frame(parent, bg='#2b2b2b')
		close_frame.pack(side='right', padx=10, pady=10)
		
		close_btn = tk.Button(close_frame, text="✕ Close Window", 
							 command=self.parent_window.destroy,
							 bg='#d32f2f', fg='white', font=('Arial', 10))
		close_btn.pack()
	
	def _load_page(self):
		"""Load current page data into the table"""
		# Clear existing data
		for item in self.tree.get_children():
			self.tree.delete(item)
		
		if not self.data:
			return
		
		# Calculate page boundaries
		total_pages = max(1, (len(self.data) + self.page_size - 1) // self.page_size)
		self.current_page = max(0, min(self.current_page, total_pages - 1))
		
		start_idx = self.current_page * self.page_size
		end_idx = min(start_idx + self.page_size, len(self.data))
		page_data = self.data[start_idx:end_idx]
		
		# Populate table
		for i, row in enumerate(page_data):
			row_num = start_idx + i + 1
			values = []
			for col in self.columns:
				value = str(row.get(col, ""))
				# Truncate very long values for display
				if len(value) > 100:
					value = value[:97] + "..."
				values.append(value)
			
			self.tree.insert("", "end", text=str(row_num), values=values)
		
		# Update pagination info and controls
		self._update_pagination_info()
		self._update_navigation_buttons()
	
	def _update_pagination_info(self):
		"""Update pagination information display"""
		if not self.data:
			self.page_info_label.config(text="No data")
			return
		
		total_pages = max(1, (len(self.data) + self.page_size - 1) // self.page_size)
		start_record = self.current_page * self.page_size + 1
		end_record = min((self.current_page + 1) * self.page_size, len(self.data))
		
		info_text = f"Page {self.current_page + 1} of {total_pages} | "
		info_text += f"Records {start_record:,}-{end_record:,} of {len(self.data):,}"
		
		self.page_info_label.config(text=info_text)
	
	def _update_navigation_buttons(self):
		"""Update navigation button states"""
		total_pages = max(1, (len(self.data) + self.page_size - 1) // self.page_size)
		
		# First and previous buttons
		if self.current_page <= 0:
			self.first_btn.config(state='disabled')
			self.prev_btn.config(state='disabled')
		else:
			self.first_btn.config(state='normal')
			self.prev_btn.config(state='normal')
		
		# Next and last buttons
		if self.current_page >= total_pages - 1:
			self.next_btn.config(state='disabled')
			self.last_btn.config(state='disabled')
		else:
			self.next_btn.config(state='normal')
			self.last_btn.config(state='normal')
	
	def _first_page(self):
		"""Go to first page"""
		self.current_page = 0
		self._load_page()
	
	def _prev_page(self):
		"""Go to previous page"""
		if self.current_page > 0:
			self.current_page -= 1
			self._load_page()
	
	def _next_page(self):
		"""Go to next page"""
		total_pages = max(1, (len(self.data) + self.page_size - 1) // self.page_size)
		if self.current_page < total_pages - 1:
			self.current_page += 1
			self._load_page()
	
	def _last_page(self):
		"""Go to last page"""
		total_pages = max(1, (len(self.data) + self.page_size - 1) // self.page_size)
		self.current_page = total_pages - 1
		self._load_page()
	
	def _on_page_size_change(self, event=None):
		"""Handle page size change"""
		try:
			import tkinter as tk
			new_size = int(self.page_size_var.get())
			old_size = self.page_size
			
			# Calculate new page to maintain roughly the same position
			current_record = self.current_page * old_size
			self.page_size = new_size
			self.current_page = current_record // new_size
			
			self._load_page()
		except ValueError:
			pass  # Invalid page size, ignore


class AsyncRunner:
	def __init__(self, tk_root: Any):
		self.root: Any = tk_root

	def run(self, fn, on_done=None, on_error=None):
		def _target():
			try:
				result = fn()
				if on_done:
					self.root.after(0, lambda: on_done(result))  # type: ignore[attr-defined]
			except Exception as e:  # noqa: BLE001
				if on_error:
					self.root.after(0, lambda: on_error(e))  # type: ignore[attr-defined]

		threading.Thread(target=_target, daemon=True).start()


# ---------------------------
# Main Application UI
# ---------------------------

# Import UI Framework Adapter for consistent UI
from frontend.services.ui_framework_adapter import UIFrameworkAdapter
from frontend.core.container import configure_services, get_service


# Create the appropriate base class
import tkinter as tk
from typing import Type, Any

BaseAppClass: Type[Any] = tk.Tk

if ui_adapter.is_customtkinter_available():
    try:
        import customtkinter as base_ctk
        BaseAppClass = base_ctk.CTk  # type: ignore
    except ImportError:
        pass

class DataForgeApp(BaseAppClass):  # type: ignore
	def __init__(self):
		super().__init__()
		
		# Initialize dependency injection container 
		self.container = configure_services()
		
		# Get services through DI
		self.error_handler = self.container.resolve(ErrorHandler)
		self.string_utils = self.container.resolve(StringUtils)
		self.data_type_detector = self.container.resolve(DataTypeDetector)
		self.ui_adapter = self.container.resolve(UIFrameworkAdapter)
		
		# Initialize plugin manager
		self.plugin_manager = self.container.resolve(PluginManager)
		
		# Initialize error handler with log file
		self.error_handler.initialize(log_file="logs/frontend.log")
		
		# Initialize window controller for professional window management
		self.window_controller = MainWindowController(
			self, 
			"DataForge - Modern Data Exploration Frontend",
			error_handler=self.error_handler
		)
		self.window_controller.setup_window(center=True)
		
		# Initialize UI controller for consistent UI state management
		self.ui_controller = UIController(self, error_handler=self.error_handler)

		# State
		self.schema_var = self._ctk_string(AppConfig.DEFAULT_SCHEMA)
		self.records_var = self._ctk_string(AppConfig.DEFAULT_RECORDS)
		self.compression_var = self._ctk_string(AppConfig.DEFAULT_COMPRESSION)
		self.export_format_var = self._ctk_string("CSV")
		
		# External Fetch state
		self.fetch_url_var = self._ctk_string("")
		self.fetch_username_var = self._ctk_string("")
		self.fetch_password_var = self._ctk_string("")
		self.fetch_type_var = self._ctk_string("generic")

		self.api = ApiClient(AppConfig.API_BASE_URL)
		self.runner = AsyncRunner(self)
		
		# Current active tab
		self.current_tab = "home"

		# Layout
		self._build_layout()

	def _build_layout(self):
		"""Build the modern layout with sidebar navigation"""
		# Main container
		self.main_container = ctk.CTkFrame(self)
		self.main_container.pack(fill="both", expand=True)
		
		# Left sidebar for navigation
		self.sidebar = ctk.CTkFrame(self.main_container, width=200)
		self.sidebar.pack(side="left", fill="y", padx=(10, 0), pady=10)
		self.sidebar.pack_propagate(False)
		
		# Right content area
		self.content_area = ctk.CTkFrame(self.main_container)
		self.content_area.pack(side="right", fill="both", expand=True, padx=10, pady=10)
		
		self._build_sidebar()
		self._build_content_area()
		
		# Initialize plugin system
		self._initialize_plugin_system()
		
		# Show home tab by default
		self._show_tab("home")

	def _build_sidebar(self):
		"""Build the left sidebar with navigation buttons"""
		# App title
		title_label = ctk.CTkLabel(
			self.sidebar, 
			text="DataForge", 
			font=ctk.CTkFont(size=24, weight="bold") if HAS_CTK else ("Arial", 18, "bold")
		)
		title_label.pack(pady=(20, 30))
		
		# Navigation buttons
		self.nav_buttons = {}
		
		nav_items = [
			("home", "🏠 Home"),
			("database", "🗄️ Database"),
			("external", "🌐 External Fetch"),
			("sync", "🔄 Sync"),
			("gateway", "⚡ Features"),
			("exploration", "🔍 Exploration"),
			("plugins", "🔌 Plugins"),
			("help", "❓ Help")
		]
		
		for tab_id, tab_text in nav_items:
			btn = ctk.CTkButton(
				self.sidebar,
				text=tab_text,
				command=lambda t=tab_id: self._show_tab(t),
				height=40,
				font=ctk.CTkFont(size=14) if HAS_CTK else ("Arial", 12),
				fg_color=Colors.PRIMARY,  # Use standardized blue
				text_color=Colors.TEXT_PRIMARY,  # Use standardized white text
				hover_color=Colors.PRIMARY_HOVER,  # Use standardized darker blue hover
				border_width=0,
				corner_radius=6
			)
			btn.pack(fill="x", padx=20, pady=5)
			self.nav_buttons[tab_id] = btn
		
		# Keep track of sub-views for back navigation
		self.view_stack = []
		
		# Status section at bottom
		status_frame = ctk.CTkFrame(self.sidebar)
		status_frame.pack(side="bottom", fill="x", padx=10, pady=20)
		
		ctk.CTkLabel(
			status_frame, 
			text="API Endpoint:", 
			font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8)
		).pack(pady=(10, 0))
		
		ctk.CTkLabel(
			status_frame, 
			text=AppConfig.API_BASE_URL, 
			font=ctk.CTkFont(size=9) if HAS_CTK else ("Arial", 8),
			text_color="gray"
		).pack(pady=(0, 10))
		
		# Back button area (below status section)
		back_button_frame = ctk.CTkFrame(self.sidebar)
		back_button_frame.pack(side="bottom", fill="x", padx=10, pady=(0, 10))
		
		# Back button (initially hidden)
		self.back_button = ctk.CTkButton(
			back_button_frame,
			text="← Back",
			command=self._handle_back_action,
			height=35,
			font=ctk.CTkFont(size=12) if HAS_CTK else ("Arial", 10),
			fg_color=Colors.PRIMARY,  # Use standardized blue
			text_color=Colors.TEXT_PRIMARY,  # Use standardized white text
			hover_color=Colors.PRIMARY_HOVER,  # Use standardized darker blue hover
			border_width=0,
			corner_radius=6
		)
		# Don't pack initially - will be shown when needed

	def _show_back_button(self, text="← Back", action=None):
		"""Show the back button with optional custom text and action."""
		if hasattr(self, 'back_button'):
			self.back_button.configure(text=text)
			if action:
				self.back_button.configure(command=action)
			self.back_button.pack(fill="x", padx=10, pady=5)
	
	def _hide_back_button(self):
		"""Hide the back button."""
		if hasattr(self, 'back_button'):
			self.back_button.pack_forget()
	
	def _handle_back_action(self):
		"""Handle the default back action."""
		if self.view_stack:
			# Pop the last view and go back to it
			previous_view = self.view_stack.pop()
			if previous_view == "exploration":
				self._back_to_exploration_main()
			else:
				self._show_tab(previous_view)
		else:
			# No specific back action, go to home
			self._show_tab("home")

	def _build_content_area(self):
		"""Build the main content area that will show different tabs"""
		# Header with current tab info
		self.header_frame = ctk.CTkFrame(self.content_area, height=60)
		self.header_frame.pack(fill="x", pady=(0, 10))
		self.header_frame.pack_propagate(False)
		
		self.tab_title = ctk.CTkLabel(
			self.header_frame, 
			text="Home", 
			font=ctk.CTkFont(size=20, weight="bold") if HAS_CTK else ("Arial", 16, "bold")
		)
		self.tab_title.pack(side="left", padx=20, pady=15)
		
		# Content frame for tab content
		self.tab_content = ctk.CTkFrame(self.content_area)
		self.tab_content.pack(fill="both", expand=True)
		
		# Log section at bottom
		self._build_log_section()

	def _build_log_section(self):
		"""Build the log section that's always visible"""
		log_frame = ctk.CTkFrame(self.content_area, height=200)
		log_frame.pack(side="bottom", fill="x", pady=(10, 0))
		log_frame.pack_propagate(False)
		
		log_header = ctk.CTkFrame(log_frame, height=30)
		log_header.pack(fill="x", padx=10, pady=(10, 0))
		log_header.pack_propagate(False)
		
		ctk.CTkLabel(
			log_header, 
			text="📋 Application Logs", 
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		).pack(side="left", pady=5)
		
		clear_btn = ctk.CTkButton(
			log_header, 
			text="Clear", 
			command=self._clear_logs,
			width=80,
			height=25
		)
		clear_btn.pack(side="right", pady=2)
		
		self.log = ctk.CTkTextbox(log_frame, height=150)
		self.log.pack(fill="both", expand=True, padx=10, pady=(5, 10))

	def _show_tab(self, tab_id: str):
		"""Show the specified tab content"""
		self.current_tab = tab_id
		
		# Hide back button by default for main tabs
		if tab_id in ["home", "database", "external", "sync", "gateway", "exploration", "plugins", "help"]:
			self._hide_back_button()
		
		# Update button states with proper styling
		for btn_id, btn in self.nav_buttons.items():
			if btn_id == tab_id:
				# Selected/active button - darker blue and disabled interaction
				try:
					btn.configure(
						state="disabled",
						fg_color=Colors.PRIMARY_HOVER,  # Darker blue for selected
						text_color=Colors.TEXT_PRIMARY,  # White text
						hover_color=Colors.PRIMARY_HOVER  # Keep same color on hover when selected
					)
				except Exception:
					pass
			else:
				# Normal button - regular blue with proper hover
				try:
					btn.configure(
						state="normal",
						fg_color=Colors.PRIMARY,  # Regular blue
						text_color=Colors.TEXT_PRIMARY,  # White text  
						hover_color=Colors.PRIMARY_HOVER  # Darker blue on hover
					)
				except Exception:
					pass
		
		# Clear current content
		for widget in self.tab_content.winfo_children():
			widget.destroy()
		
		# Show appropriate content
		if tab_id == "home":
			self._build_home_tab()
		elif tab_id == "database":
			self._build_database_tab()
		elif tab_id == "external":
			self._build_external_fetch_tab()
		elif tab_id == "sync":
			self._build_sync_tab()
		elif tab_id == "gateway":
			self._build_gateway_tab()
		elif tab_id == "exploration":
			self._build_exploration_tab()
		elif tab_id == "plugins":
			self._build_plugins_tab()
		elif tab_id == "help":
			self._build_help_tab()
		
		# Update header
		tab_titles = {
			"home": "🏠 Home",
			"database": "🗄️ Database",
			"external": "🌐 External Fetch",
			"sync": "🔄 Sync",
			"gateway": "⚡ Features",
			"exploration": "🔍 Exploration",
			"plugins": "🔌 Plugins",
			"help": "❓ Help"
		}
		self.tab_title.configure(text=tab_titles.get(tab_id, tab_id.title()))

	def _build_home_tab(self):
		"""Build the home tab content"""
		home_container = ctk.CTkFrame(self.tab_content)
		home_container.pack(fill="both", expand=True, padx=20, pady=20)
		
		# Welcome section
		welcome_frame = ctk.CTkFrame(home_container)
		welcome_frame.pack(fill="x", pady=(0, 20))
		
		welcome_label = ctk.CTkLabel(
			welcome_frame,
			text="👋 Hello! Welcome to DataForge",
			font=ctk.CTkFont(size=28, weight="bold") if HAS_CTK else ("Arial", 22, "bold")
		)
		welcome_label.pack(pady=30)
		
		subtitle_label = ctk.CTkLabel(
			welcome_frame,
			text="Modern data management platform with powerful upload, download, and sync capabilities",
			font=ctk.CTkFont(size=16) if HAS_CTK else ("Arial", 14)
		)
		subtitle_label.pack(pady=(0, 30))
		
		# Quick stats or info cards
		stats_frame = ctk.CTkFrame(home_container)
		stats_frame.pack(fill="both", expand=True)
		
		# Info cards
		cards_data = [
			("🚀", "Fast Processing", "High-performance data operations with Polars"),
			("🔄", "Real-time Sync", "Keep your data synchronized across systems"),
			("📊", "Rich Analytics", "Comprehensive data analysis and visualization"),
			("🔒", "Secure", "Enterprise-grade security for your data")
		]
		
		for i, (icon, title, desc) in enumerate(cards_data):
			row = i // 2
			col = i % 2
			
			card = ctk.CTkFrame(stats_frame)
			card.grid(row=row, column=col, padx=10, pady=10, sticky="ew")
			
			icon_label = ctk.CTkLabel(
				card, 
				text=icon, 
				font=ctk.CTkFont(size=32) if HAS_CTK else ("Arial", 24)
			)
			icon_label.pack(pady=(20, 10))
			
			title_label = ctk.CTkLabel(
				card, 
				text=title, 
				font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
			)
			title_label.pack()
			
			desc_label = ctk.CTkLabel(
				card, 
				text=desc, 
				font=ctk.CTkFont(size=12) if HAS_CTK else ("Arial", 10),
				wraplength=200
			)
			desc_label.pack(pady=(5, 20))
		
		# Configure grid weights
		stats_frame.grid_columnconfigure(0, weight=1)
		stats_frame.grid_columnconfigure(1, weight=1)

	def _build_database_tab(self):
		"""Build the unified database tab with upload and download functionality"""
		database_container = ctk.CTkFrame(self.tab_content)
		database_container.pack(fill="both", expand=True, padx=20, pady=20)
		
		# Create tabview for Upload, Download, and Schema Management sections
		try:
			# Try CustomTkinter tabview
			tabview = ctk.CTkTabview(database_container)
			tabview.pack(fill="both", expand=True)
			
			# Add Upload tab
			upload_tab = tabview.add("📤 Upload")
			self._build_upload_content(upload_tab)
			
			# Add Download tab
			download_tab = tabview.add("📥 Download")
			self._build_download_content(download_tab)
			
			# Add Schema Management tab
			schema_tab = tabview.add("📋 Schemas")
			self._build_schema_management_content(schema_tab)
			
		except Exception:
			# Fallback to simple frame-based layout
			self._log("TabView not available, using frame layout")
			
			# Header
			header_frame = ctk.CTkFrame(database_container)
			header_frame.pack(fill="x", pady=(0, 20))
			
			ctk.CTkLabel(
				header_frame,
				text="🗄️ Database Operations",
				font=ctk.CTkFont(size=20, weight="bold") if HAS_CTK else ("Arial", 16, "bold")
			).pack(pady=20)
			
			# Create three columns
			columns_frame = ctk.CTkFrame(database_container)
			columns_frame.pack(fill="both", expand=True)
			
			# Upload column
			upload_column = ctk.CTkFrame(columns_frame)
			upload_column.pack(side="left", fill="both", expand=True, padx=(0, 5))
			self._build_upload_content(upload_column)
			
			# Download column
			download_column = ctk.CTkFrame(columns_frame)
			download_column.pack(side="left", fill="both", expand=True, padx=5)
			self._build_download_content(download_column)
			
			# Schema Management column
			schema_column = ctk.CTkFrame(columns_frame)
			schema_column.pack(side="right", fill="both", expand=True, padx=(5, 0))
			self._build_schema_management_content(schema_column)

	def _build_upload_content(self, parent):
		"""Build upload content in the given parent frame"""
		# Configuration section
		config_frame = ctk.CTkFrame(parent)
		config_frame.pack(fill="x", pady=(20, 20))
		
		ctk.CTkLabel(
			config_frame, 
			text="⚙️ Upload Configuration", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		# Settings row
		settings_row = ctk.CTkFrame(config_frame)
		settings_row.pack(fill="x", padx=20, pady=(0, 15))
		
		ctk.CTkLabel(settings_row, text="Schema:").pack(side="left", padx=(0, 10))
		self.schema_entry = ctk.CTkEntry(settings_row, width=150)
		self.schema_entry.pack(side="left", padx=(0, 20))
		self._entry_set(self.schema_entry, self.schema_var.get())
		
		ctk.CTkLabel(settings_row, text="Records:").pack(side="left", padx=(0, 10))
		self.records_entry = ctk.CTkEntry(settings_row, width=100)
		self.records_entry.pack(side="left", padx=(0, 20))
		self._entry_set(self.records_entry, self.records_var.get())
		
		ctk.CTkLabel(settings_row, text="Compression:").pack(side="left", padx=(0, 10))
		try:
			self.compression_menu = ctk.CTkOptionMenu(
				settings_row,
				values=["zstd", "gzip", "none"],
				variable=self.compression_var,
				width=100
			)
			self.compression_menu.pack(side="left")
		except Exception:
			self.compression_entry = ctk.CTkEntry(settings_row, width=80)
			self.compression_entry.pack(side="left")
			self._entry_set(self.compression_entry, self.compression_var.get())
		
		# Actions section
		actions_frame = ctk.CTkFrame(parent)
		actions_frame.pack(fill="x", pady=(0, 20))
		
		ctk.CTkLabel(
			actions_frame, 
			text="🎯 Actions", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		buttons_row = ctk.CTkFrame(actions_frame)
		buttons_row.pack(fill="x", padx=20, pady=(0, 15))
		
		gen_btn = ctk.CTkButton(
			buttons_row, 
			text="🎲 Generate Sample Data", 
			command=self._on_generate_data,
			height=40
		)
		gen_btn.pack(side="left", padx=(0, 10))
		
		write_btn = ctk.CTkButton(
			buttons_row, 
			text="📤 Upload to Server", 
			command=self._on_write,
			height=40
		)
		write_btn.pack(side="left", padx=(0, 10))
		
		# Progress bar
		self.progress = ctk.CTkProgressBar(buttons_row)
		try:
			self.progress.pack(side="left", padx=20, fill="x", expand=True)
			if hasattr(self.progress, "set"):
				self.progress.set(0)
		except Exception:
			pass
		
		# Schema management section
		schema_frame = ctk.CTkFrame(parent)
		schema_frame.pack(fill="both", expand=True)
		
		ctk.CTkLabel(
			schema_frame, 
			text="📋 Schema Management", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		schema_buttons = ctk.CTkFrame(schema_frame)
		schema_buttons.pack(fill="x", padx=20, pady=(0, 15))
		
		list_btn = ctk.CTkButton(schema_buttons, text="📋 List Schemas", command=self._on_list_schemas)
		list_btn.pack(side="left", padx=(0, 10))
		
		latest_btn = ctk.CTkButton(schema_buttons, text="🔍 Get Latest Schema", command=self._on_get_latest_schema)
		latest_btn.pack(side="left", padx=(0, 10))
		
		register_btn = ctk.CTkButton(schema_buttons, text="➕ Register Example Schema", command=self._on_register_example)
		register_btn.pack(side="left")

	def _build_download_content(self, parent):
		"""Build download content in the given parent frame"""
		# Read section
		read_frame = ctk.CTkFrame(parent)
		read_frame.pack(fill="x", pady=(20, 20))
		
		ctk.CTkLabel(
			read_frame, 
			text="📥 Data Reading", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		read_buttons = ctk.CTkFrame(read_frame)
		read_buttons.pack(fill="x", padx=20, pady=(0, 15))
		
		read_btn = ctk.CTkButton(
			read_buttons, 
			text="📊 Read Data", 
			command=self._on_read,
			height=40
		)
		read_btn.pack(side="left", padx=(0, 10))
		
		save_arrow_btn = ctk.CTkButton(
			read_buttons, 
			text="💾 Save Arrow Stream", 
			command=self._on_read_and_save,
			height=40
		)
		save_arrow_btn.pack(side="left")
		
		# Export section
		export_frame = ctk.CTkFrame(parent)
		export_frame.pack(fill="both", expand=True)
		
		ctk.CTkLabel(
			export_frame, 
			text="📁 Export Options", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		export_options = ctk.CTkFrame(export_frame)
		export_options.pack(fill="x", padx=20, pady=(0, 15))
		
		ctk.CTkLabel(export_options, text="Format:").pack(side="left", padx=(0, 10))
		try:
			self.export_format_menu = ctk.CTkOptionMenu(
				export_options,
				values=["CSV", "Text (JSONL)"],
				variable=self.export_format_var,
				width=150
			)
			self.export_format_menu.pack(side="left", padx=(0, 20))
		except Exception:
			self.export_format_menu = None
			self._log("OptionMenu not available; defaulting export to CSV")
		
		export_btn = ctk.CTkButton(
			export_options, 
			text="📁 Export Data", 
			command=self._on_export,
			height=40
		)
		export_btn.pack(side="left")

	def _build_schema_management_content(self, parent):
		"""Build schema management content in the given parent frame"""
		# Header section
		header_frame = ctk.CTkFrame(parent)
		header_frame.pack(fill="x", pady=(20, 20))
		
		ctk.CTkLabel(
			header_frame, 
			text="📋 Schema Management", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		# Schema operations section
		operations_frame = ctk.CTkFrame(parent)
		operations_frame.pack(fill="x", pady=(0, 20))
		
		ctk.CTkLabel(
			operations_frame, 
			text="🔧 Operations", 
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		).pack(anchor="w", padx=20, pady=(15, 5))
		
		# Operations buttons row
		ops_buttons = ctk.CTkFrame(operations_frame)
		ops_buttons.pack(fill="x", padx=20, pady=(0, 15))
		
		list_families_btn = ctk.CTkButton(
			ops_buttons, 
			text="📋 List All Schemas", 
			command=self._on_list_schemas,
			height=35,
			width=140
		)
		list_families_btn.pack(side="left", padx=(0, 10))
		
		refresh_btn = ctk.CTkButton(
			ops_buttons, 
			text="🔄 Refresh", 
			command=self._on_refresh_schemas,
			height=35,
			width=100
		)
		refresh_btn.pack(side="left", padx=(0, 10))
		
		register_btn = ctk.CTkButton(
			ops_buttons, 
			text="➕ Register Example", 
			command=self._on_register_example,
			height=35,
			width=140
		)
		register_btn.pack(side="left")
		
		# Schema details section
		details_frame = ctk.CTkFrame(parent)
		details_frame.pack(fill="x", pady=(0, 20))
		
		ctk.CTkLabel(
			details_frame, 
			text="🔍 Schema Details", 
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		).pack(anchor="w", padx=20, pady=(15, 5))
		
		# Schema selection row
		selection_row = ctk.CTkFrame(details_frame)
		selection_row.pack(fill="x", padx=20, pady=(0, 10))
		
		ctk.CTkLabel(selection_row, text="Schema:").pack(side="left", padx=(0, 10))
		self.schema_details_entry = ctk.CTkEntry(selection_row, width=150, placeholder_text="Enter schema name...")
		self.schema_details_entry.pack(side="left", padx=(0, 10))
		
		get_latest_btn = ctk.CTkButton(
			selection_row, 
			text="📄 Get Latest", 
			command=self._on_get_latest_schema,
			height=35,
			width=100
		)
		get_latest_btn.pack(side="left", padx=(0, 10))
		
		get_versions_btn = ctk.CTkButton(
			selection_row, 
			text="📊 List Versions", 
			command=self._on_get_schema_versions,
			height=35,
			width=120
		)
		get_versions_btn.pack(side="left")
		
		# Schema viewer section
		viewer_frame = ctk.CTkFrame(parent)
		viewer_frame.pack(fill="both", expand=True)
		
		ctk.CTkLabel(
			viewer_frame, 
			text="📖 Schema Viewer", 
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		).pack(anchor="w", padx=20, pady=(15, 5))
		
		# Create scrollable text area for schema display
		try:
			self.schema_display = ctk.CTkTextbox(
				viewer_frame, 
				height=200,
				wrap="word"
			)
			self.schema_display.pack(fill="both", expand=True, padx=20, pady=(0, 15))
			self.schema_display.insert("0.0", "Select a schema to view its details...")
		except Exception:
			# Fallback to regular text widget
			import tkinter as tk
			self.schema_display = tk.Text(
				viewer_frame, 
				height=12,
				wrap="word",
				bg="#2b2b2b" if HAS_CTK else "white",
				fg="white" if HAS_CTK else "black"
			)
			self.schema_display.pack(fill="both", expand=True, padx=20, pady=(0, 15))
			self.schema_display.insert("1.0", "Select a schema to view its details...")

	def _build_upload_tab(self):
		"""Build the upload tab content"""
		upload_container = ctk.CTkFrame(self.tab_content)
		upload_container.pack(fill="both", expand=True, padx=20, pady=20)
		
		# Configuration section
		config_frame = ctk.CTkFrame(upload_container)
		config_frame.pack(fill="x", pady=(0, 20))
		
		ctk.CTkLabel(
			config_frame, 
			text="⚙️ Upload Configuration", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		# Settings row
		settings_row = ctk.CTkFrame(config_frame)
		settings_row.pack(fill="x", padx=20, pady=(0, 15))
		
		ctk.CTkLabel(settings_row, text="Schema:").pack(side="left", padx=(0, 10))
		self.schema_entry = ctk.CTkEntry(settings_row, width=150)
		self.schema_entry.pack(side="left", padx=(0, 20))
		self._entry_set(self.schema_entry, self.schema_var.get())
		
		ctk.CTkLabel(settings_row, text="Records:").pack(side="left", padx=(0, 10))
		self.records_entry = ctk.CTkEntry(settings_row, width=100)
		self.records_entry.pack(side="left", padx=(0, 20))
		self._entry_set(self.records_entry, self.records_var.get())
		
		ctk.CTkLabel(settings_row, text="Compression:").pack(side="left", padx=(0, 10))
		try:
			self.compression_menu = ctk.CTkOptionMenu(
				settings_row,
				values=["zstd", "gzip", "none"],
				variable=self.compression_var,
				width=100
			)
			self.compression_menu.pack(side="left")
		except Exception:
			self.compression_entry = ctk.CTkEntry(settings_row, width=80)
			self.compression_entry.pack(side="left")
			self._entry_set(self.compression_entry, self.compression_var.get())
		
		# Actions section
		actions_frame = ctk.CTkFrame(upload_container)
		actions_frame.pack(fill="x", pady=(0, 20))
		
		ctk.CTkLabel(
			actions_frame, 
			text="🎯 Actions", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		buttons_row = ctk.CTkFrame(actions_frame)
		buttons_row.pack(fill="x", padx=20, pady=(0, 15))
		
		gen_btn = ctk.CTkButton(
			buttons_row, 
			text="🎲 Generate Sample Data", 
			command=self._on_generate_data,
			height=40
		)
		gen_btn.pack(side="left", padx=(0, 10))
		
		write_btn = ctk.CTkButton(
			buttons_row, 
			text="📤 Upload to Server", 
			command=self._on_write,
			height=40
		)
		write_btn.pack(side="left", padx=(0, 10))
		
		# Progress bar
		self.progress = ctk.CTkProgressBar(buttons_row)
		try:
			self.progress.pack(side="left", padx=20, fill="x", expand=True)
			if hasattr(self.progress, "set"):
				self.progress.set(0)
		except Exception:
			pass
		
		# Schema management section
		schema_frame = ctk.CTkFrame(upload_container)
		schema_frame.pack(fill="both", expand=True)
		
		ctk.CTkLabel(
			schema_frame, 
			text="📋 Schema Management", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		schema_buttons = ctk.CTkFrame(schema_frame)
		schema_buttons.pack(fill="x", padx=20, pady=(0, 15))
		
		list_btn = ctk.CTkButton(schema_buttons, text="📋 List Schemas", command=self._on_list_schemas)
		list_btn.pack(side="left", padx=(0, 10))
		
		latest_btn = ctk.CTkButton(schema_buttons, text="🔍 Get Latest Schema", command=self._on_get_latest_schema)
		latest_btn.pack(side="left", padx=(0, 10))
		
		register_btn = ctk.CTkButton(schema_buttons, text="➕ Register Example Schema", command=self._on_register_example)
		register_btn.pack(side="left")

	def _build_download_tab(self):
		"""Build the download tab content"""
		download_container = ctk.CTkFrame(self.tab_content)
		download_container.pack(fill="both", expand=True, padx=20, pady=20)
		
		# Read section
		read_frame = ctk.CTkFrame(download_container)
		read_frame.pack(fill="x", pady=(0, 20))
		
		ctk.CTkLabel(
			read_frame, 
			text="📥 Data Reading", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		read_buttons = ctk.CTkFrame(read_frame)
		read_buttons.pack(fill="x", padx=20, pady=(0, 15))
		
		read_btn = ctk.CTkButton(
			read_buttons, 
			text="📊 Read Data", 
			command=self._on_read,
			height=40
		)
		read_btn.pack(side="left", padx=(0, 10))
		
		save_arrow_btn = ctk.CTkButton(
			read_buttons, 
			text="💾 Save Arrow Stream", 
			command=self._on_read_and_save,
			height=40
		)
		save_arrow_btn.pack(side="left")
		
		# Export section
		export_frame = ctk.CTkFrame(download_container)
		export_frame.pack(fill="both", expand=True)
		
		ctk.CTkLabel(
			export_frame, 
			text="📁 Export Options", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		export_options = ctk.CTkFrame(export_frame)
		export_options.pack(fill="x", padx=20, pady=(0, 15))
		
		ctk.CTkLabel(export_options, text="Format:").pack(side="left", padx=(0, 10))
		try:
			self.export_format_menu = ctk.CTkOptionMenu(
				export_options,
				values=["CSV", "Text (JSONL)"],
				variable=self.export_format_var,
				width=150
			)
			self.export_format_menu.pack(side="left", padx=(0, 20))
		except Exception:
			self.export_format_menu = None
			self._log("OptionMenu not available; defaulting export to CSV")
		
		export_btn = ctk.CTkButton(
			export_options, 
			text="📁 Export Data", 
			command=self._on_export,
			height=40
		)
		export_btn.pack(side="left")

	def _build_sync_tab(self):
		"""Build the sync tab content"""
		sync_container = ctk.CTkFrame(self.tab_content)
		sync_container.pack(fill="both", expand=True, padx=20, pady=20)
		
		# Sync info
		info_frame = ctk.CTkFrame(sync_container)
		info_frame.pack(fill="both", expand=True)
		
		icon_label = ctk.CTkLabel(
			info_frame, 
			text="🔄", 
			font=ctk.CTkFont(size=64) if HAS_CTK else ("Arial", 48)
		)
		icon_label.pack(pady=(50, 20))
		
		title_label = ctk.CTkLabel(
			info_frame,
			text="Sync Features",
			font=ctk.CTkFont(size=24, weight="bold") if HAS_CTK else ("Arial", 18, "bold")
		)
		title_label.pack(pady=(0, 10))
		
		desc_label = ctk.CTkLabel(
			info_frame,
			text="Synchronization features are coming soon!\nThis will include real-time data sync, backup management,\nand cross-platform data consistency.",
			font=ctk.CTkFont(size=14) if HAS_CTK else ("Arial", 12),
			justify="center"
		)
		desc_label.pack(pady=(0, 50))

	def _build_external_fetch_tab(self):
		"""Build the external fetch tab content"""
		external_container = ctk.CTkFrame(self.tab_content)
		external_container.pack(fill="both", expand=True, padx=20, pady=20)
		
		# URL and Authentication section
		auth_frame = ctk.CTkFrame(external_container)
		auth_frame.pack(fill="x", pady=(0, 20))
		
		ctk.CTkLabel(
			auth_frame, 
			text="🔗 Connection Details", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		# URL field
		url_row = ctk.CTkFrame(auth_frame)
		url_row.pack(fill="x", padx=20, pady=(0, 10))
		
		ctk.CTkLabel(url_row, text="URL:", width=80).pack(side="left", padx=(0, 10))
		self.fetch_url_entry = ctk.CTkEntry(url_row, placeholder_text="https://example.com/data")
		self.fetch_url_entry.pack(side="left", fill="x", expand=True)
		
		# Username field
		username_row = ctk.CTkFrame(auth_frame)
		username_row.pack(fill="x", padx=20, pady=(0, 10))
		
		ctk.CTkLabel(username_row, text="Username:", width=80).pack(side="left", padx=(0, 10))
		self.fetch_username_entry = ctk.CTkEntry(username_row, placeholder_text="username (optional)")
		self.fetch_username_entry.pack(side="left", fill="x", expand=True, padx=(0, 20))
		
		# Password field
		ctk.CTkLabel(username_row, text="Password:", width=80).pack(side="left", padx=(0, 10))
		self.fetch_password_entry = ctk.CTkEntry(username_row, placeholder_text="password (optional)", show="*")
		self.fetch_password_entry.pack(side="left", fill="x", expand=True)
		
		# Request Type section
		type_frame = ctk.CTkFrame(external_container)
		type_frame.pack(fill="x", pady=(0, 20))
		
		ctk.CTkLabel(
			type_frame, 
			text="📋 Request Type", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		# Checkboxes for request type (using radio button logic)
		checkboxes_frame = ctk.CTkFrame(type_frame)
		checkboxes_frame.pack(fill="x", padx=20, pady=(0, 15))
		
		# Configure grid layout for 2x2 checkboxes
		checkboxes_frame.grid_columnconfigure(0, weight=1)
		checkboxes_frame.grid_columnconfigure(1, weight=1)
		
		self.fetch_type_vars = {}
		
		# Create checkbox variables
		self.fetch_type_vars["odata_raw"] = self._ctk_boolean(False)
		self.fetch_type_vars["odata_csv"] = self._ctk_boolean(False)
		self.fetch_type_vars["csv"] = self._ctk_boolean(False)
		self.fetch_type_vars["generic"] = self._ctk_boolean(True)  # Default selection
		
		# Create checkboxes in 2x2 grid layout
		try:
			if HAS_CTK:
				# Left column - OData options
				self.odata_raw_checkbox = ctk.CTkCheckBox(
					checkboxes_frame,
					text="🔌 OData API (raw)",
					variable=self.fetch_type_vars["odata_raw"],
					command=lambda: self._on_fetch_type_change("odata_raw")
				)
				self.odata_raw_checkbox.grid(row=0, column=0, sticky="w", padx=(0, 10), pady=2)
				
				self.odata_csv_checkbox = ctk.CTkCheckBox(
					checkboxes_frame,
					text="🔌 OData API (csv)",
					variable=self.fetch_type_vars["odata_csv"],
					command=lambda: self._on_fetch_type_change("odata_csv")
				)
				self.odata_csv_checkbox.grid(row=1, column=0, sticky="w", padx=(0, 10), pady=2)
				
				# Right column - Direct options
				self.csv_checkbox = ctk.CTkCheckBox(
					checkboxes_frame,
					text="📊 Direct CSV Download",
					variable=self.fetch_type_vars["csv"],
					command=lambda: self._on_fetch_type_change("csv")
				)
				self.csv_checkbox.grid(row=0, column=1, sticky="w", padx=(10, 0), pady=2)
				
				self.generic_checkbox = ctk.CTkCheckBox(
					checkboxes_frame,
					text="🌐 Generic GET Request (HTML)",
					variable=self.fetch_type_vars["generic"],
					command=lambda: self._on_fetch_type_change("generic")
				)
				self.generic_checkbox.grid(row=1, column=1, sticky="w", padx=(10, 0), pady=2)
			else:
				# Fallback for basic tkinter - same grid layout
				import tkinter as tk
				
				# Left column - OData options
				self.odata_raw_checkbox = tk.Checkbutton(
					checkboxes_frame,
					text="🔌 OData API (raw)",
					variable=self.fetch_type_vars["odata_raw"],
					command=lambda: self._on_fetch_type_change("odata_raw"),
					bg='#2b2b2b', fg='#ffffff', selectcolor='#404040'
				)
				self.odata_raw_checkbox.grid(row=0, column=0, sticky="w", padx=(0, 10), pady=2)
				
				self.odata_csv_checkbox = tk.Checkbutton(
					checkboxes_frame,
					text="🔌 OData API (csv)",
					variable=self.fetch_type_vars["odata_csv"],
					command=lambda: self._on_fetch_type_change("odata_csv"),
					bg='#2b2b2b', fg='#ffffff', selectcolor='#404040'
				)
				self.odata_csv_checkbox.grid(row=1, column=0, sticky="w", padx=(0, 10), pady=2)
				
				# Right column - Direct options
				self.csv_checkbox = tk.Checkbutton(
					checkboxes_frame,
					text="📊 Direct CSV Download",
					variable=self.fetch_type_vars["csv"],
					command=lambda: self._on_fetch_type_change("csv"),
					bg='#2b2b2b', fg='#ffffff', selectcolor='#404040'
				)
				self.csv_checkbox.grid(row=0, column=1, sticky="w", padx=(10, 0), pady=2)
				
				self.generic_checkbox = tk.Checkbutton(
					checkboxes_frame,
					text="🌐 Generic GET Request (HTML)",
					variable=self.fetch_type_vars["generic"],
					command=lambda: self._on_fetch_type_change("generic"),
					bg='#2b2b2b', fg='#ffffff', selectcolor='#404040'
				)
				self.generic_checkbox.grid(row=1, column=1, sticky="w", padx=(10, 0), pady=2)
		except Exception as e:
			self._log(f"Error creating checkboxes: {e}")
		
		# Actions section
		actions_frame = ctk.CTkFrame(external_container)
		actions_frame.pack(fill="x", pady=(0, 20))
		
		ctk.CTkLabel(
			actions_frame, 
			text="🚀 Actions", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		buttons_row = ctk.CTkFrame(actions_frame)
		buttons_row.pack(fill="x", padx=20, pady=(0, 15))
		
		test_btn = ctk.CTkButton(
			buttons_row, 
			text="🔍 Test Connection", 
			command=self._on_test_connection,
			height=40
		)
		test_btn.pack(side="left", padx=(0, 10))
		
		# Combined fetch and save button
		fetch_save_btn = ctk.CTkButton(
			buttons_row, 
			text="🌐 Fetch & Save Data", 
			command=self._on_fetch_and_save,
			height=40
		)
		fetch_save_btn.pack(side="left", padx=(0, 10))
		
		# Progress bar for external fetch
		self.fetch_progress = ctk.CTkProgressBar(buttons_row)
		try:
			self.fetch_progress.pack(side="left", padx=20, fill="x", expand=True)
			if hasattr(self.fetch_progress, "set"):
				self.fetch_progress.set(0)
		except Exception:
			pass
		
		# Response preview section
		preview_frame = ctk.CTkFrame(external_container)
		preview_frame.pack(fill="both", expand=True)
		
		ctk.CTkLabel(
			preview_frame, 
			text="👁️ Response Preview", 
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(anchor="w", padx=20, pady=(15, 10))
		
		self.response_preview = ctk.CTkTextbox(preview_frame, height=200)
		self.response_preview.pack(fill="both", expand=True, padx=20, pady=(0, 15))

	def _build_gateway_tab(self):
		"""Build the features tab content"""
		gateway_container = ctk.CTkFrame(self.tab_content)
		gateway_container.pack(fill="both", expand=True, padx=20, pady=20)
		
		# Features info section
		info_frame = ctk.CTkFrame(gateway_container)
		info_frame.pack(fill="both", expand=True, padx=20, pady=20)
		
		ctk.CTkLabel(
			info_frame,
			text="⚡ Features Hub",
			font=ctk.CTkFont(size=24, weight="bold") if HAS_CTK else ("Arial", 18, "bold")
		).pack(pady=(30, 20))
		
		description = (
			"The Features Hub provides access to advanced DataForge capabilities "
			"including data integration, pipeline automation, and future enhancements.\n\n"
			"This section will expand with new features and tools "
			"for enhanced data processing and workflow automation."
		)
		
		ctk.CTkLabel(
			info_frame,
			text=description,
			font=ctk.CTkFont(size=14) if HAS_CTK else ("Arial", 12),
			wraplength=600,
			justify="center"
		).pack(pady=20)
		
		ctk.CTkLabel(
			info_frame,
			text="🚧 Coming Soon",
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold"),
			text_color="#FFA500"
		).pack(pady=20)

	def _build_exploration_tab(self):
		"""Build the exploration tab content with table and plots options"""
		exploration_container = ctk.CTkFrame(self.tab_content)
		exploration_container.pack(fill="both", expand=True, padx=20, pady=20)
		
		# Check if we're in sub-mode
		if not hasattr(self, 'exploration_mode'):
			self.exploration_mode = 'main'  # 'main', 'table', 'plots'
		
		if self.exploration_mode == 'main':
			self._build_exploration_main(exploration_container)
		elif self.exploration_mode == 'table':
			self._build_exploration_table(exploration_container)
		elif self.exploration_mode == 'plots':
			self._build_exploration_plots(exploration_container)

	def _build_exploration_main(self, container):
		"""Build the main exploration page with table and plots options"""
		# Header
		header_frame = ctk.CTkFrame(container)
		header_frame.pack(fill="x", pady=(0, 30))
		
		icon_label = ctk.CTkLabel(
			header_frame,
			text="🔍",
			font=ctk.CTkFont(size=48) if HAS_CTK else ("Arial", 36)
		)
		icon_label.pack(pady=(20, 10))
		
		title_label = ctk.CTkLabel(
			header_frame,
			text="Data Exploration",
			font=ctk.CTkFont(size=24, weight="bold") if HAS_CTK else ("Arial", 18, "bold")
		)
		title_label.pack(pady=(0, 10))
		
		desc_label = ctk.CTkLabel(
			header_frame,
			text="Choose how you want to explore your data",
			font=ctk.CTkFont(size=14) if HAS_CTK else ("Arial", 12)
		)
		desc_label.pack(pady=(0, 20))
		
		# Options grid
		options_frame = ctk.CTkFrame(container)
		options_frame.pack(fill="both", expand=True, padx=50)
		
		# Configure grid
		options_frame.grid_columnconfigure(0, weight=1)
		options_frame.grid_columnconfigure(1, weight=1)
		
		# Table option
		table_card = ctk.CTkFrame(options_frame)
		table_card.grid(row=0, column=0, padx=20, pady=20, sticky="nsew")
		
		ctk.CTkLabel(
			table_card,
			text="📊",
			font=ctk.CTkFont(size=64) if HAS_CTK else ("Arial", 48)
		).pack(pady=(30, 15))
		
		ctk.CTkLabel(
			table_card,
			text="Table Explorer",
			font=ctk.CTkFont(size=20, weight="bold") if HAS_CTK else ("Arial", 16, "bold")
		).pack(pady=(0, 10))
		
		ctk.CTkLabel(
			table_card,
			text="• Interactive data table\n• Advanced filtering & sorting\n• Column management\n• Export filtered data",
			font=ctk.CTkFont(size=14) if HAS_CTK else ("Arial", 12),
			justify="left"
		).pack(pady=(0, 20))
		
		table_btn = ctk.CTkButton(
			table_card,
			text="🚀 Open Table Explorer",
			command=self._open_table_explorer,
			height=40,
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		)
		table_btn.pack(pady=(0, 30))
		
		# Plots option
		plots_card = ctk.CTkFrame(options_frame)
		plots_card.grid(row=0, column=1, padx=20, pady=20, sticky="nsew")
		
		ctk.CTkLabel(
			plots_card,
			text="📈",
			font=ctk.CTkFont(size=64) if HAS_CTK else ("Arial", 48)
		).pack(pady=(30, 15))
		
		ctk.CTkLabel(
			plots_card,
			text="Visual Analytics",
			font=ctk.CTkFont(size=20, weight="bold") if HAS_CTK else ("Arial", 16, "bold")
		).pack(pady=(0, 10))
		
		ctk.CTkLabel(
			plots_card,
			text="• Interactive charts & plots\n• Statistical analysis\n• Data visualization\n• Export charts",
			font=ctk.CTkFont(size=14) if HAS_CTK else ("Arial", 12),
			justify="left"
		).pack(pady=(0, 20))
		
		plots_btn = ctk.CTkButton(
			plots_card,
			text="📊 Open Visual Analytics",
			command=self._open_plots_explorer,
			height=40,
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		)
		plots_btn.pack(pady=(0, 30))
		
		# Status label for plots
		ctk.CTkLabel(
			plots_card,
			text="✅ Ready",
			font=ctk.CTkFont(size=12) if HAS_CTK else ("Arial", 10),
			text_color="#00AA00"
		).pack(pady=(0, 10))

	def _build_exploration_table(self, container):
		"""Build the table exploration interface"""
		# Show the back button in sidebar
		self._show_back_button("← Back to Exploration", self._back_to_exploration_main)
		
		# Add current view to stack for navigation
		if "exploration" not in self.view_stack:
			self.view_stack.append("exploration")
		
		# Navigation header (simplified without back button)
		nav_frame = ctk.CTkFrame(container)
		nav_frame.pack(fill="x", pady=(0, 10))
		
		# Title
		ctk.CTkLabel(
			nav_frame,
			text="📊 Table Explorer",
			font=ctk.CTkFont(size=18, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(side="left", padx=20, pady=8)
		
		# Check if we have data loaded
		if not hasattr(self, 'exploration_data') or not self.exploration_data:
			# Show data loading interface
			self._build_data_loading_interface(container)
		else:
			# Add "Load New Data" button when data is already loaded
			load_new_btn = ctk.CTkButton(
				nav_frame,
				text="📁 Load New Data",
				command=self._load_new_data_for_exploration,
				width=140,
				height=32
			)
			load_new_btn.pack(side="right", padx=10, pady=8)
			
			# Add "Float Table" button
			float_table_btn = ctk.CTkButton(
				nav_frame,
				text="🪟 Float Table",
				command=self._open_floating_table,
				width=120,
				height=32
			)
			float_table_btn.pack(side="right", padx=(0, 10), pady=8)
			
			# Add data info label
			data_info = f"({len(self.exploration_data):,} records, {len(self.exploration_data[0].keys()) if self.exploration_data else 0} columns)"
			info_label = ctk.CTkLabel(
				nav_frame,
				text=data_info,
				font=ctk.CTkFont(size=12) if HAS_CTK else ("Arial", 10),
				text_color="#888888"
			)
			info_label.pack(side="right", padx=(0, 10), pady=8)
			
			# Always create a new data explorer instance to avoid UI issues
			self.data_explorer = DataExplorer(
				container, 
				self.exploration_data,
				app_log=self._log,  # Pass log function
				export_callback=self._export_exploration_data
			)
			# Set the app instance as the parent for method access
			self.data_explorer.app_instance = self

	def _build_exploration_plots(self, container):
		"""Build the plots exploration interface"""
		# Show the back button in sidebar
		self._show_back_button("← Back to Exploration", self._back_to_exploration_main)
		
		# Add current view to stack for navigation
		if "exploration" not in self.view_stack:
			self.view_stack.append("exploration")
		
		# Navigation header (simplified without back button)
		nav_frame = ctk.CTkFrame(container)
		nav_frame.pack(fill="x", pady=(0, 10))
		
		# Title
		ctk.CTkLabel(
			nav_frame,
			text="📈 Visual Analytics",
			font=ctk.CTkFont(size=18, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(side="left", padx=20, pady=8)
		
		# Check if we have data loaded
		if not hasattr(self, 'exploration_data') or not self.exploration_data:
			# Show data loading interface for plots
			self._build_plots_data_loading_interface(container)
		else:
			# Add "Load New Data" button when data is already loaded
			load_new_btn = ctk.CTkButton(
				nav_frame,
				text="� Load New Data",
				command=self._load_new_data_for_exploration,
				width=140,
				height=32
			)
			load_new_btn.pack(side="right", padx=10, pady=8)
			
			# Add data info label
			data_info = f"({len(self.exploration_data):,} records, {len(self.exploration_data[0].keys()) if self.exploration_data else 0} columns)"
			info_label = ctk.CTkLabel(
				nav_frame,
				text=data_info,
				font=ctk.CTkFont(size=12) if HAS_CTK else ("Arial", 10),
				text_color="#888888"
			)
			info_label.pack(side="right", padx=(0, 10), pady=8)
			
			# Create plot explorer instance
			self.plot_explorer = PlotExplorer(
				container, 
				self.exploration_data,
				app_log=self._log,  # Pass log function
				on_back=self._back_to_exploration_main
			)
			# Set the app instance as the parent for method access
			if hasattr(self.plot_explorer, 'app_instance'):
				self.plot_explorer.app_instance = self

	def _build_plots_data_loading_interface(self, container):
		"""Build interface for loading data into the plots explorer"""
		loading_frame = ctk.CTkFrame(container)
		loading_frame.pack(fill="both", expand=True, padx=20, pady=20)
		
		# Icon and title
		ctk.CTkLabel(
			loading_frame,
			text="📊",
			font=ctk.CTkFont(size=48) if HAS_CTK else ("Arial", 36)
		).pack(pady=(30, 15))
		
		ctk.CTkLabel(
			loading_frame,
			text="Load Data for Visual Analytics",
			font=ctk.CTkFont(size=20, weight="bold") if HAS_CTK else ("Arial", 16, "bold")
		).pack(pady=(0, 20))
		
		# Data source options
		options_frame = ctk.CTkFrame(loading_frame)
		options_frame.pack(pady=20)
		
		# Show current data info if available
		if hasattr(self, 'exploration_data') and self.exploration_data:
			info_text = f"📊 Current Data: {len(self.exploration_data):,} records loaded"
			if self.exploration_data:
				num_cols = len(self.exploration_data[0].keys())
				info_text += f", {num_cols} columns"
			
			current_data_label = ctk.CTkLabel(
				options_frame,
				text=info_text,
				font=ctk.CTkFont(size=12, weight="bold") if HAS_CTK else ("Arial", 10, "bold"),
				text_color="#00AA00"
			)
			current_data_label.pack(pady=(0, 15))
		
		# Current schema button
		current_schema_btn = ctk.CTkButton(
			options_frame,
			text="📥 Load Current Schema Data",
			command=self._load_current_schema_data_for_plots,
			height=40,
			width=250
		)
		current_schema_btn.pack(pady=10)
		
		# Generate sample data button
		sample_data_btn = ctk.CTkButton(
			options_frame,
			text="🎲 Generate Sample Data",
			command=self._load_sample_exploration_data_for_plots,
			height=40,
			width=250
		)
		sample_data_btn.pack(pady=10)
		
		# File upload button
		upload_btn = ctk.CTkButton(
			options_frame,
			text="📁 Upload CSV File",
			command=self._upload_csv_for_plots,
			height=40,
			width=250
		)
		upload_btn.pack(pady=10)
		
		# If data is already loaded, show "Continue to Plots" button
		if hasattr(self, 'exploration_data') and self.exploration_data:
			continue_btn = ctk.CTkButton(
				options_frame,
				text="📈 Continue to Visual Analytics",
				command=self._continue_to_plots_explorer,
				height=40,
				width=250,
				fg_color="#1f538d",
				font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
			)
			continue_btn.pack(pady=15)
			
			# Add "Clear Current Data" button
			clear_data_btn = ctk.CTkButton(
				options_frame,
				text="🗑️ Clear Current Data",
				command=self._clear_exploration_data,
				height=35,
				width=200,
				fg_color="#d32f2f",
				hover_color="#b71c1c",
				font=ctk.CTkFont(size=12) if HAS_CTK else ("Arial", 10)
			)
			clear_data_btn.pack(pady=(5, 0))

	def _load_current_schema_data_for_plots(self):
		"""Load data from the current schema for plots exploration"""
		def task():
			self._apply_settings()  # Apply any form changes first
			table, byte_len = self.api.read_polars(self.schema_var.get())
			return table, byte_len

		def done(result):
			table, byte_len = result
			if table is None:
				self._log("❌ Cannot load data for plots exploration. PyArrow is required.")
				return
			
			# Convert Arrow table to list of dictionaries
			try:
				data_dicts = table.to_pylist()
				self.exploration_data = data_dicts
				self._log(f"✅ Loaded {len(data_dicts):,} records for plots exploration")
				
				# Stay in plots view but refresh to show data
				self.exploration_mode = 'plots'
				self._show_tab('exploration')
				
			except Exception as e:
				self._log(f"❌ Error converting data for plots exploration: {e}")

		def error(e: Exception):
			self._log(f"❌ Failed to load data for plots exploration: {e}")

		self._status("Loading data for plots exploration...")
		self.runner.run(task, on_done=done, on_error=error)

	def _load_sample_exploration_data_for_plots(self):
		"""Generate sample data for plots exploration"""
		try:
			# Generate sample data
			sample_data = DataGenerator.generate_sample_data(500)  # Generate 500 records for exploration
			self.exploration_data = sample_data
			self._log(f"✅ Generated {len(sample_data):,} sample records for plots exploration")
			
			# Stay in plots view but refresh to show data
			self.exploration_mode = 'plots'
			self._show_tab('exploration')

		except Exception as e:
			self._log(f"❌ Error generating sample data: {e}")

	def _upload_csv_for_plots(self):
		"""Upload CSV file for plots exploration"""
		try:
			# Import file dialog
			from tkinter import filedialog as fd
			
			# Open file dialog
			file_path = fd.askopenfilename(
				title="Select CSV File for Plots",
				filetypes=[
					("CSV Files", "*.csv"),
					("Text Files", "*.txt"),
					("All Files", "*.*")
				],
				initialdir=str(Path.home() / "Downloads")  # Default to Downloads folder
			)
			
			if not file_path:
				self._log("💡 File selection cancelled")
				return
			
			# Convert to Path object
			csv_path = Path(file_path)
			
			if not csv_path.exists():
				self._log(f"❌ File not found: {csv_path}")
				return
			
			self._log(f"📁 Loading CSV file for plots: {csv_path.name}")
			self._status("Loading CSV file...")
			
			# Read the CSV file (reuse existing CSV reading logic)
			try:
				import csv
				
				# Read CSV with automatic encoding detection
				encodings_to_try = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']
				csv_data = None
				used_encoding = None
				
				for encoding in encodings_to_try:
					try:
						with open(csv_path, 'r', encoding=encoding, newline='') as csvfile:
							# Detect delimiter
							sample = csvfile.read(1024)
							csvfile.seek(0)
							
							# Use csv.Sniffer to detect format
							sniffer = csv.Sniffer()
							delimiter = ','
							try:
								dialect = sniffer.sniff(sample, delimiters=',;\t|')
								delimiter = dialect.delimiter
							except csv.Error:
								# If sniffing fails, try common delimiters
								for delim in [',', ';', '\t', '|']:
									if sample.count(delim) > sample.count(','):
										delimiter = delim
										break
							
							# Read the CSV
							reader = csv.DictReader(csvfile, delimiter=delimiter)
							csv_data = list(reader)
							used_encoding = encoding
							break
					except (UnicodeDecodeError, UnicodeError):
						continue
					except Exception as e:
						self._log(f"⚠️ Error with encoding {encoding}: {e}")
						continue
				
				if csv_data is None:
					self._log("❌ Could not read CSV file with any supported encoding")
					self._status("CSV load failed")
					return
				
				if not csv_data:
					self._log("❌ CSV file is empty or has no data rows")
					self._status("CSV load failed")
					return
				
				# Clean and validate the data (reuse existing cleaning logic)
				cleaned_data = []
				for i, row in enumerate(csv_data):
					# Skip completely empty rows
					if not any(str(value).strip() for value in row.values()):
						continue
					
					# Clean the row data
					cleaned_row = {}
					for key, value in row.items():
						# Handle None or empty column names
						clean_key = str(key).strip() if key is not None else f"Column_{len(cleaned_row)}"
						if not clean_key:
							clean_key = f"Column_{len(cleaned_row)}"
						
						# Handle None values
						clean_value = str(value).strip() if value is not None else ""
						
						# Try to convert numeric values
						if clean_value:
							# Try to detect and convert numbers
							try:
								# Check if it's an integer
								if '.' not in clean_value and clean_value.replace('-', '').replace('+', '').isdigit():
									cleaned_row[clean_key] = int(clean_value)
								else:
									# Try to convert to float
									float_val = float(clean_value)
									cleaned_row[clean_key] = float_val
							except ValueError:
								# Keep as string if not numeric
								cleaned_row[clean_key] = clean_value
						else:
							cleaned_row[clean_key] = clean_value
					
					if cleaned_row:  # Only add non-empty rows
						cleaned_data.append(cleaned_row)
				
				if not cleaned_data:
					self._log("❌ No valid data found in CSV file")
					self._status("CSV load failed")
					return
				
				# Store the data for exploration
				self.exploration_data = cleaned_data
				
				# Log success info
				file_size = csv_path.stat().st_size
				num_columns = len(cleaned_data[0].keys()) if cleaned_data else 0
				
				self._log(f"✅ Successfully loaded CSV file for plots:")
				self._log(f"   • File: {csv_path.name}")
				self._log(f"   • Records: {len(cleaned_data):,}")
				self._log(f"   • Columns: {num_columns}")
				
				# Stay in plots view but refresh to show data
				self.exploration_mode = 'plots'
				self._show_tab('exploration')
				self._status("CSV loaded successfully")
				self.ui_controller.show_success("CSV file loaded successfully for plots!")
				
			except Exception as e:
				self._log(f"❌ Error reading CSV file: {e}")
				self._status("CSV load failed")
				self.ui_controller.show_error(f"Failed to read CSV file: {str(e)}", "CSV Load Error")
		
		except Exception as e:
			self._log(f"❌ Unexpected error during CSV upload: {e}")
			self._status("CSV upload failed")
			self.ui_controller.show_error(f"Unexpected error during CSV upload: {str(e)}", "Upload Error")

	def _continue_to_plots_explorer(self):
		"""Continue to plots explorer with already loaded data"""
		self.exploration_mode = 'plots'
		self._show_tab('exploration')

	def _open_floating_table(self):
		"""Open a floating table window with pagination and filter support"""
		if not hasattr(self, 'exploration_data') or not self.exploration_data:
			self._log("❌ No data available for floating table")
			return
		
		try:
			import tkinter as tk
			from tkinter import ttk
			
			# Create floating window
			floating_window = tk.Toplevel()
			floating_window.title("DataForge - Floating Table")
			floating_window.geometry("1400x800")
			floating_window.configure(bg='#2b2b2b')
			
			# Set window icon if available
			try:
				floating_window.iconbitmap(str(AppConfig.FAVICON_PATH))
			except:
				pass
			
			# Make it stay on top (optional)
			floating_window.attributes('-topmost', False)
			
			# Get filtered data if available
			display_data = self.exploration_data
			filter_info = ""
			
			# Check if we have an active data explorer with filters applied
			if hasattr(self, 'data_explorer') and self.data_explorer and hasattr(self.data_explorer, 'filtered_data'):
				display_data = self.data_explorer.filtered_data
				total_original = len(self.exploration_data)
				total_filtered = len(display_data)
				if total_filtered < total_original:
					filter_info = f" (Filtered: {total_filtered:,} of {total_original:,})"
			
			# Create floating data explorer with pagination
			floating_explorer = FloatingDataExplorer(floating_window, display_data, filter_info)
			
			self._log(f"🪟 Opened floating table with {len(display_data):,} records{filter_info}")
			
		except Exception as e:
			self._log(f"❌ Error opening floating table: {e}")

	def _build_data_loading_interface(self, container):
		"""Build interface for loading data into the explorer"""
		loading_frame = ctk.CTkFrame(container)
		loading_frame.pack(fill="both", expand=True, padx=20, pady=20)
		
		# Icon and title
		ctk.CTkLabel(
			loading_frame,
			text="📊",
			font=ctk.CTkFont(size=48) if HAS_CTK else ("Arial", 36)
		).pack(pady=(30, 15))
		
		ctk.CTkLabel(
			loading_frame,
			text="Load Data for Exploration",
			font=ctk.CTkFont(size=20, weight="bold") if HAS_CTK else ("Arial", 16, "bold")
		).pack(pady=(0, 20))
		
		# Data source options
		options_frame = ctk.CTkFrame(loading_frame)
		options_frame.pack(pady=20)
		
		# Show current data info if available
		if hasattr(self, 'exploration_data') and self.exploration_data:
			info_text = f"📊 Current Data: {len(self.exploration_data):,} records loaded"
			if self.exploration_data:
				num_cols = len(self.exploration_data[0].keys())
				info_text += f", {num_cols} columns"
			
			current_data_label = ctk.CTkLabel(
				options_frame,
				text=info_text,
				font=ctk.CTkFont(size=12, weight="bold") if HAS_CTK else ("Arial", 10, "bold"),
				text_color="#00AA00"
			)
			current_data_label.pack(pady=(0, 15))
		
		# Current schema button
		current_schema_btn = ctk.CTkButton(
			options_frame,
			text="📥 Load Current Schema Data",
			command=self._load_current_schema_data,
			height=40,
			width=250
		)
		current_schema_btn.pack(pady=10)
		
		# Generate sample data button
		sample_data_btn = ctk.CTkButton(
			options_frame,
			text="🎲 Generate Sample Data",
			command=self._load_sample_exploration_data,
			height=40,
			width=250
		)
		sample_data_btn.pack(pady=10)
		
		# File upload button
		upload_btn = ctk.CTkButton(
			options_frame,
			text="📁 Upload CSV File",
			command=self._upload_csv_for_exploration,
			height=40,
			width=250
		)
		upload_btn.pack(pady=10)
		
		# If data is already loaded, show "Continue to Explorer" button
		if hasattr(self, 'exploration_data') and self.exploration_data:
			continue_btn = ctk.CTkButton(
				options_frame,
				text="🚀 Continue to Table Explorer",
				command=self._continue_to_table_explorer,
				height=40,
				width=250,
				fg_color="#1f538d",
				font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
			)
			continue_btn.pack(pady=15)
			
			# Add "Clear Current Data" button
			clear_data_btn = ctk.CTkButton(
				options_frame,
				text="🗑️ Clear Current Data",
				command=self._clear_exploration_data,
				height=35,
				width=200,
				fg_color="#d32f2f",
				hover_color="#b71c1c",
				font=ctk.CTkFont(size=12) if HAS_CTK else ("Arial", 10)
			)
			clear_data_btn.pack(pady=(5, 0))

	# Add these methods to the DataForgeApp class
	def _open_table_explorer(self):
		"""Open the table explorer"""
		self.exploration_mode = 'table'
		self._show_tab('exploration')  # Refresh the tab

	def _open_plots_explorer(self):
		"""Open the plots explorer"""
		self.exploration_mode = 'plots'
		self._show_tab('exploration')  # Refresh the tab

	def _back_to_exploration_main(self):
		"""Go back to the main exploration page"""
		self.exploration_mode = 'main'
		
		# Clear view stack
		if self.view_stack and self.view_stack[-1] == "exploration":
			self.view_stack.pop()
		
		# Hide the back button
		self._hide_back_button()
		
		# Clear the data explorer instance to prevent UI issues
		if hasattr(self, 'data_explorer'):
			delattr(self, 'data_explorer')
		
		# Clear the plot explorer instance to prevent UI issues
		if hasattr(self, 'plot_explorer'):
			delattr(self, 'plot_explorer')
		
		self._show_tab('exploration')  # Refresh the tab

	def _clear_exploration_data(self):
		"""Clear the current exploration data with confirmation"""
		try:
			# Import messagebox for confirmation
			from tkinter import messagebox
			
			# Show confirmation dialog
			result = messagebox.askyesno(
				"Clear Data",
				"Are you sure you want to clear the current exploration data?\n\nThis action cannot be undone.",
				icon="warning"
			)
			
			if result:  # User clicked "Yes"
				if hasattr(self, 'exploration_data'):
					num_records = len(self.exploration_data)
					delattr(self, 'exploration_data')
					self._log(f"🗑️ Cleared {num_records:,} records from exploration data")
				else:
					self._log("🗑️ No exploration data to clear")
				
				if hasattr(self, 'data_explorer'):
					delattr(self, 'data_explorer')
				
				# Refresh the current tab to show the updated interface
				self._show_tab('exploration')
			else:
				self._log("💡 Clear data operation cancelled")
				
		except Exception as e:
			# Fallback without confirmation if messagebox import fails
			self._log(f"⚠️ Could not show confirmation dialog: {e}")
			
			if hasattr(self, 'exploration_data'):
				num_records = len(self.exploration_data)
				delattr(self, 'exploration_data')
				self._log(f"🗑️ Cleared {num_records:,} records from exploration data")
			
			if hasattr(self, 'data_explorer'):
				delattr(self, 'data_explorer')
			
			# Refresh the current tab to show the updated interface
			self._show_tab('exploration')

	def _load_new_data_for_exploration(self):
		"""Clear current data and show data loading interface"""
		# Clear current exploration data
		if hasattr(self, 'exploration_data'):
			delattr(self, 'exploration_data')
		
		# Clear data explorer instance
		if hasattr(self, 'data_explorer'):
			delattr(self, 'data_explorer')
		
		self._log("🔄 Cleared current data - ready to load new data")
		
		# Refresh the tab to show data loading interface
		self._show_tab('exploration')

	def _continue_to_table_explorer(self):
		"""Continue to table explorer with already loaded data"""
		self.exploration_mode = 'table'
		self._show_tab('exploration')

	def _load_current_schema_data(self):
		"""Load data from the current schema for exploration"""
		def task():
			self._apply_settings()  # Apply any form changes first
			table, byte_len = self.api.read_polars(self.schema_var.get())
			return table, byte_len

		def done(result):
			table, byte_len = result
			if table is None:
				self._log("❌ Cannot load data for exploration. PyArrow is required.")
				return
			
			# Convert Arrow table to list of dictionaries
			try:
				data_dicts = table.to_pylist()
				self.exploration_data = data_dicts
				self._log(f"✅ Loaded {len(data_dicts):,} records for exploration")
				
				# Switch to table view
				self.exploration_mode = 'table'
				self._show_tab('exploration')
				
			except Exception as e:
				self._log(f"❌ Error converting data for exploration: {e}")

		def error(e: Exception):
			self._log(f"❌ Failed to load data for exploration: {e}")

		self._status("Loading data for exploration...")
		self.runner.run(task, on_done=done, on_error=error)

	def _load_sample_exploration_data(self):
		"""Generate sample data for exploration"""
		try:
			# Generate sample data
			sample_data = DataGenerator.generate_sample_data(500)  # Generate 500 records for exploration
			self.exploration_data = sample_data
			self._log(f"✅ Generated {len(sample_data):,} sample records for exploration")
			
			# Switch to table view
			self.exploration_mode = 'table'
			self._show_tab('exploration')
			
		except Exception as e:
			self._log(f"❌ Error generating sample data: {e}")

	def _upload_csv_for_exploration(self):
		"""Upload CSV file for exploration"""
		try:
			# Import file dialog
			if HAS_CTK and hasattr(ctk, "filedialog") and ctk.filedialog:
				from tkinter import filedialog as fd
			else:
				from tkinter import filedialog as fd
			
			# Open file dialog
			file_path = fd.askopenfilename(
				title="Select CSV File to Explore",
				filetypes=[
					("CSV Files", "*.csv"),
					("Text Files", "*.txt"),
					("All Files", "*.*")
				],
				initialdir=str(Path.home() / "Downloads")  # Default to Downloads folder
			)
			
			if not file_path:
				self._log("💡 File selection cancelled")
				return
			
			# Convert to Path object
			csv_path = Path(file_path)
			
			if not csv_path.exists():
				self._log(f"❌ File not found: {csv_path}")
				return
			
			self._log(f"📁 Loading CSV file: {csv_path.name}")
			self._status("Loading CSV file...")
			
			# Read the CSV file
			try:
				import csv
				
				# Read CSV with automatic encoding detection
				encodings_to_try = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']
				csv_data = None
				used_encoding = None
				
				for encoding in encodings_to_try:
					try:
						with open(csv_path, 'r', encoding=encoding, newline='') as csvfile:
							# Detect delimiter
							sample = csvfile.read(1024)
							csvfile.seek(0)
							
							# Use csv.Sniffer to detect format
							sniffer = csv.Sniffer()
							delimiter = ','
							try:
								dialect = sniffer.sniff(sample, delimiters=',;\t|')
								delimiter = dialect.delimiter
							except csv.Error:
								# If sniffing fails, try common delimiters
								for delim in [',', ';', '\t', '|']:
									if sample.count(delim) > sample.count(','):
										delimiter = delim
										break
							
							# Read the CSV
							reader = csv.DictReader(csvfile, delimiter=delimiter)
							csv_data = list(reader)
							used_encoding = encoding
							break
					except (UnicodeDecodeError, UnicodeError):
						continue
					except Exception as e:
						self._log(f"⚠️ Error with encoding {encoding}: {e}")
						continue
				
				if csv_data is None:
					self._log("❌ Could not read CSV file with any supported encoding")
					self._status("CSV load failed")
					return
				
				if not csv_data:
					self._log("❌ CSV file is empty or has no data rows")
					self._status("CSV load failed")
					return
				
				# Clean and validate the data
				cleaned_data = []
				for i, row in enumerate(csv_data):
					# Skip completely empty rows
					if not any(str(value).strip() for value in row.values()):
						continue
					
					# Clean the row data
					cleaned_row = {}
					for key, value in row.items():
						# Handle None or empty column names
						clean_key = str(key).strip() if key is not None else f"Column_{len(cleaned_row)}"
						if not clean_key:
							clean_key = f"Column_{len(cleaned_row)}"
						
						# Handle None values
						clean_value = str(value).strip() if value is not None else ""
						
						# Try to convert numeric values
						if clean_value:
							# Try to detect and convert numbers
							try:
								# Check if it's an integer
								if '.' not in clean_value and clean_value.replace('-', '').replace('+', '').isdigit():
									cleaned_row[clean_key] = int(clean_value)
								else:
									# Try to convert to float
									float_val = float(clean_value)
									cleaned_row[clean_key] = float_val
							except ValueError:
								# Keep as string if not numeric
								cleaned_row[clean_key] = clean_value
						else:
							cleaned_row[clean_key] = clean_value
					
					if cleaned_row:  # Only add non-empty rows
						cleaned_data.append(cleaned_row)
				
				if not cleaned_data:
					self._log("❌ No valid data found in CSV file")
					self._status("CSV load failed")
					return
				
				# Store the data for exploration
				self.exploration_data = cleaned_data
				
				# Log success info
				file_size = csv_path.stat().st_size
				num_columns = len(cleaned_data[0].keys()) if cleaned_data else 0
				
				self._log(f"✅ Successfully loaded CSV file:")
				self._log(f"   • File: {csv_path.name}")
				self._log(f"   • Size: {file_size:,} bytes")
				self._log(f"   • Encoding: {used_encoding}")
				self._log(f"   • Records: {len(cleaned_data):,}")
				self._log(f"   • Columns: {num_columns}")
				
				if num_columns > 0:
					column_names = list(cleaned_data[0].keys())[:5]  # Show first 5 columns
					column_preview = ", ".join(column_names)
					if num_columns > 5:
						column_preview += f", ... and {num_columns - 5} more"
					self._log(f"   • Columns: {column_preview}")
				
				# Switch to table view
				self.exploration_mode = 'table'
				self._show_tab('exploration')
				self._status("CSV loaded successfully")
				
			except Exception as e:
				self._log(f"❌ Error reading CSV file: {e}")
				self._status("CSV load failed")
				
				# Try to give more specific error info
				if "encoding" in str(e).lower():
					self._log("💡 Tip: This might be an encoding issue. Try saving the CSV as UTF-8.")
				elif "permission" in str(e).lower():
					self._log("💡 Tip: Make sure the file is not open in another application.")
		
		except Exception as e:
			self._log(f"❌ Unexpected error during CSV upload: {e}")
			self._status("CSV upload failed")

	def _refresh_exploration_data(self):
		"""Refresh the current exploration data"""
		if hasattr(self, 'exploration_data') and self.exploration_data:
			# If we have data, reload it from the server
			self._load_current_schema_data()
		else:
			self._log("💡 No data loaded. Use 'Load Data' to start exploring.")

	def _export_exploration_data(self, filtered_data):
		"""Export the filtered exploration data"""
		try:
			if not filtered_data:
				self._log("❌ No data to export")
				return
			
			if HAS_CTK and hasattr(ctk, "filedialog") and ctk.filedialog:
				from tkinter import filedialog as fd
			else:
				from tkinter import filedialog as fd
			
			# Ask for save location
			file_path = fd.asksaveasfilename(
				defaultextension=".csv",
				filetypes=[("CSV Files", "*.csv"), ("JSON Files", "*.json"), ("All Files", "*.*")],
				initialfile=f"exploration_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
			)
			
			if file_path:
				save_path = Path(file_path)
				
				if save_path.suffix.lower() == '.json':
					# Export as JSON
					save_path.write_text(json.dumps(filtered_data, indent=2, ensure_ascii=False), encoding='utf-8')
				else:
					# Export as CSV
					if filtered_data:
						import csv
						with open(save_path, 'w', newline='', encoding='utf-8') as csvfile:
							writer = csv.DictWriter(csvfile, fieldnames=filtered_data[0].keys())
							writer.writeheader()
							writer.writerows(filtered_data)
				
				self._log(f"✅ Exported {len(filtered_data):,} records to: {save_path}")
			
		except Exception as e:
			self._log(f"❌ Export failed: {e}")

	def _build_help_tab(self):
		"""Build the help tab content"""
		help_container = ctk.CTkFrame(self.tab_content)
		help_container.pack(fill="both", expand=True, padx=20, pady=20)
		
		# Help info section
		info_frame = ctk.CTkFrame(help_container)
		info_frame.pack(fill="both", expand=True, padx=20, pady=20)
		
		ctk.CTkLabel(
			info_frame,
			text="❓ Help & About",
			font=ctk.CTkFont(size=24, weight="bold") if HAS_CTK else ("Arial", 18, "bold")
		).pack(pady=(30, 20))
		
		# About section
		about_frame = ctk.CTkFrame(info_frame)
		about_frame.pack(fill="x", padx=20, pady=20)
		
		ctk.CTkLabel(
			about_frame,
			text="DataForge Frontend",
			font=ctk.CTkFont(size=18, weight="bold") if HAS_CTK else ("Arial", 16, "bold")
		).pack(pady=(15, 10))
		
		description = (
			"This application was developed by:\n\n"
			"Lucas Rocha\n"
			"Marine Engineer & Software Engineer\n\n"
			"DataForge provides powerful tools for data management, "
			"external data integration, and workflow automation."
		)
		
		ctk.CTkLabel(
			about_frame,
			text=description,
			font=ctk.CTkFont(size=14) if HAS_CTK else ("Arial", 12),
			justify="center"
		).pack(pady=(10, 20))
		
		# Version info
		version_frame = ctk.CTkFrame(info_frame)
		version_frame.pack(fill="x", padx=20, pady=(0, 20))
		
		ctk.CTkLabel(
			version_frame,
			text="Version: 1.0.0 MVP",
			font=ctk.CTkFont(size=12) if HAS_CTK else ("Arial", 10),
			text_color="#888888"
		).pack(pady=15)

	def _clear_logs(self):
		"""Clear the log content"""
		try:
			self.log.delete("1.0", "end")
		except Exception:
			pass

	def _on_fetch_type_change(self, selected_type: str):
		"""Handle fetch type checkbox changes (radio button behavior)"""
		try:
			# Uncheck all others (radio button behavior)
			for type_name, var in self.fetch_type_vars.items():
				if type_name != selected_type:
					if hasattr(var, 'set'):
						var.set(False)
			
			# Ensure the selected one is checked
			if hasattr(self.fetch_type_vars[selected_type], 'set'):
				self.fetch_type_vars[selected_type].set(True)
			
			# Update internal state
			self.fetch_type_var.set(selected_type)
			
			self._log(f"📋 Selected fetch type: {selected_type}")
		except Exception as e:
			self._log(f"Error updating fetch type: {e}")

	def _on_test_connection(self):
		"""Test the connection to the external URL"""
		def task():
			url = self._entry_get(self.fetch_url_entry)
			username = self._entry_get(self.fetch_username_entry)
			password = self._entry_get(self.fetch_password_entry)
			
			if not url:
				raise ValueError("URL is required")
			
			# Basic connection test with HEAD request
			auth = None
			if username and password:
				from requests.auth import HTTPBasicAuth
				auth = HTTPBasicAuth(username, password)
			
			response = requests.head(url, auth=auth, timeout=30)
			return {
				"status_code": response.status_code,
				"headers": dict(response.headers),
				"url": response.url
			}
		
		def done(result):
			self._fetch_progress(0)
			status = result["status_code"]
			if 200 <= status < 300:
				self._log(f"✅ Connection test successful: {status}")
				self.response_preview.delete("1.0", "end")
				self.response_preview.insert("1.0", f"✅ Connection OK\n\nStatus: {status}\nURL: {result['url']}\n\nHeaders:\n{_format_json(result['headers'])}")
			else:
				self._log(f"⚠️ Connection test returned status: {status}")
				self.response_preview.delete("1.0", "end")
				self.response_preview.insert("1.0", f"⚠️ Status: {status}\n\nHeaders:\n{_format_json(result['headers'])}")
			self._status("Connection test completed")
		
		def error(e: Exception):
			self._fetch_progress(0)
			self._log(f"❌ Connection test failed: {e}")
			self.response_preview.delete("1.0", "end")
			self.response_preview.insert("1.0", f"❌ Connection Failed\n\nError: {str(e)}")
			self._status("Connection test failed")
		
		self._fetch_progress(0.3)
		self._status("Testing connection...")
		self.runner.run(task, on_done=done, on_error=error)

	def _on_external_fetch(self):
		"""Fetch data from external URL"""
		def task():
			url = self._entry_get(self.fetch_url_entry)
			username = self._entry_get(self.fetch_username_entry)
			password = self._entry_get(self.fetch_password_entry)
			
			if not url:
				raise ValueError("URL is required")
			
			# Determine the selected fetch type
			fetch_type = "generic"  # default
			try:
				for type_name, var in self.fetch_type_vars.items():
					if hasattr(var, 'get') and var.get():
						fetch_type = type_name
						break
			except Exception:
				pass
			
			# Prepare authentication
			auth = None
			if username and password:
				from requests.auth import HTTPBasicAuth
				auth = HTTPBasicAuth(username, password)
			
			# Configure headers based on fetch type
			headers = {}
			if fetch_type in ["odata_raw", "odata_csv"]:
				headers.update({
					"Accept": "application/json",
					"Content-Type": "application/json"
				})
			elif fetch_type == "csv":
				headers.update({
					"Accept": "text/csv"
				})
			else:  # generic
				headers.update({
					"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
				})
			
			# Make the request
			response = requests.get(url, auth=auth, headers=headers, timeout=300)
			response.raise_for_status()
			
			return {
				"content": response.text[:5000],  # First 5000 chars for preview
				"full_content": response.text,
				"status_code": response.status_code,
				"headers": dict(response.headers),
				"url": response.url,
				"fetch_type": fetch_type,
				"content_length": len(response.text)
			}
		
		def done(result):
			self._fetch_progress(0)
			content_length = result["content_length"]
			fetch_type = result["fetch_type"]
			
			# Process OData CSV conversion if needed
			processed_content = result["full_content"]
			if fetch_type == "odata_csv":
				try:
					# Note: Test connection only shows first page for preview
					# Full pagination is available in the main fetch operation
					csv_content = self._convert_odata_to_csv(result["full_content"])
					if csv_content:
						processed_content = csv_content
						self._log("🔄 Converted OData JSON to CSV format (test preview - first page only)")
					else:
						self._log("⚠️ Could not convert OData to CSV, saving as JSON")
				except Exception as e:
					self._log(f"❌ OData to CSV conversion failed: {e}")
			
			self._log(f"✅ External fetch successful: {result['status_code']} ({content_length:,} chars, type: {fetch_type})")
			
			# Show preview in the text box
			preview_text = f"✅ Fetch Successful ({fetch_type.upper()})\n\n"
			preview_text += f"Status: {result['status_code']}\n"
			preview_text += f"URL: {result['url']}\n"
			preview_text += f"Content Length: {content_length:,} characters\n"
			
			if fetch_type == "odata_csv" and processed_content != result["full_content"]:
				preview_text += f"Processed: Converted to CSV format\n"
			
			preview_text += "\n--- CONTENT PREVIEW (first 5000 chars) ---\n"
			preview_text += processed_content[:5000]
			
			if len(processed_content) > 5000:
				preview_text += f"\n\n... and {len(processed_content) - 5000:,} more characters"
			
			self.response_preview.delete("1.0", "end")
			self.response_preview.insert("1.0", preview_text)
			
			# Store the processed content for saving
			self._last_fetched_data = processed_content
			self._last_fetch_type = fetch_type
			self._last_fetch_url = result["url"]
			
			self._status(f"Fetch completed ({len(processed_content):,} chars) - Ready to save")
		
		def error(e: Exception):
			self._fetch_progress(0)
			self._log(f"❌ External fetch failed: {e}")
			self.response_preview.delete("1.0", "end")
			self.response_preview.insert("1.0", f"❌ Fetch Failed\n\nError: {str(e)}")
			
			self._status("Fetch failed")
		
		self._fetch_progress(0.5)
		self._status("Fetching external data...")
		self.runner.run(task, on_done=done, on_error=error)

	def _on_fetch_and_save(self):
		"""Combined fetch and save operation in one step"""
		def task():
			url = self._entry_get(self.fetch_url_entry)
			username = self._entry_get(self.fetch_username_entry)
			password = self._entry_get(self.fetch_password_entry)
			
			if not url:
				raise ValueError("URL is required")
			
			# Determine the selected fetch type
			fetch_type = "generic"  # default
			try:
				for type_name, var in self.fetch_type_vars.items():
					if hasattr(var, 'get') and var.get():
						fetch_type = type_name
						break
			except Exception:
				pass
			
			# Prepare authentication
			auth = None
			if username and password:
				from requests.auth import HTTPBasicAuth
				auth = HTTPBasicAuth(username, password)
			
			# Configure headers based on fetch type
			headers = {}
			if fetch_type in ["odata_raw", "odata_csv"]:
				headers.update({
					"Accept": "application/json",
					"Content-Type": "application/json"
				})
			elif fetch_type == "csv":
				headers.update({
					"Accept": "text/csv"
				})
			else:  # generic
				headers.update({
					"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
				})
			
			# Make the request
			response = requests.get(url, auth=auth, headers=headers, timeout=300)
			response.raise_for_status()
			
			# Process OData CSV conversion if needed
			processed_content = response.text
			if fetch_type == "odata_csv":
				try:
					# Handle OData pagination - fetch all pages
					all_data = self._fetch_all_odata_pages(url, auth, headers, response.text)
					csv_content = self._convert_odata_to_csv(all_data)
					if csv_content:
						processed_content = csv_content
					else:
						# Keep original content if conversion fails
						pass
				except Exception:
					# Keep original content if conversion fails
					pass
			
			return {
				"content": processed_content[:5000],  # First 5000 chars for preview
				"full_content": processed_content,
				"status_code": response.status_code,
				"headers": dict(response.headers),
				"url": response.url,
				"fetch_type": fetch_type,
				"content_length": len(processed_content)
			}
		
		def done(result):
			self._fetch_progress(0)
			content_length = result["content_length"]
			fetch_type = result["fetch_type"]
			
			self._log(f"✅ External fetch successful: {result['status_code']} ({content_length:,} chars, type: {fetch_type})")
			
			# Show preview in the text box
			preview_text = f"✅ Fetch Successful ({fetch_type.upper()})\n\n"
			preview_text += f"Status: {result['status_code']}\n"
			preview_text += f"URL: {result['url']}\n"
			preview_text += f"Content Length: {content_length:,} characters\n"
			
			if fetch_type == "odata_csv":
				preview_text += f"Processed: Converted to CSV format\n"
			
			# For CSV data, show analysis instead of raw content
			if fetch_type in ["csv", "odata_csv"] and self._looks_like_csv(result["full_content"]):
				preview_text += "\n" + self._analyze_csv_data(result["full_content"])
			else:
				# For non-CSV data, show content preview as before
				preview_text += "\n--- CONTENT PREVIEW ---\n"
				preview_text += result["content"]
				
				if len(result["full_content"]) > 5000:
					preview_text += f"\n\n... and {len(result['full_content']) - 5000:,} more characters"
			
			self.response_preview.delete("1.0", "end")
			self.response_preview.insert("1.0", preview_text)
			
			# Store the processed content
			self._last_fetched_data = result["full_content"]
			self._last_fetch_type = fetch_type
			self._last_fetch_url = result["url"]
			
			# Immediately proceed to save dialog
			self._status("Opening save dialog...")
			try:
				# Import the file dialog
				if HAS_CTK and hasattr(ctk, "filedialog") and ctk.filedialog:
					from tkinter import filedialog as fd
				else:
					from tkinter import filedialog as fd
				
				# Determine file extension and type based on fetch type
				import urllib.parse
				parsed_url = urllib.parse.urlparse(result["url"])
				url_filename = Path(parsed_url.path).name or "fetched_data"
				
				# Remove any existing extension for clean naming
				base_name = Path(url_filename).stem if '.' in url_filename else url_filename
				
				# Set file extension and types based on fetch type
				if fetch_type in ["odata_csv", "csv"]:
					default_extension = ".csv"
					file_types = [("CSV files", "*.csv"), ("All files", "*.*")]
				elif fetch_type == "odata_raw":
					default_extension = ".json"
					file_types = [("JSON files", "*.json"), ("All files", "*.*")]
				else:  # generic
					default_extension = ".html"
					file_types = [("HTML files", "*.html"), ("Text files", "*.txt"), ("All files", "*.*")]
				
				# Show save dialog
				file_path = fd.asksaveasfilename(
					title="Save Fetched Data",
					defaultextension=default_extension,
					initialfile=f"{base_name}{default_extension}",
					filetypes=file_types
				)
				
				if file_path:
					# Convert to Path object for consistent handling
					save_path = Path(file_path)
					
					# Get the content to save
					content_to_save = result["full_content"]
					
					# If it's a CSV file and the content looks like CSV, clean up formatting
					if (fetch_type in ["csv", "odata_csv"] or save_path.suffix.lower() == ".csv") and self._looks_like_csv(content_to_save):
						content_to_save = self._clean_csv_content(content_to_save)
						self._log("🧹 Cleaned CSV formatting (removed extra blank lines)")
					
					# Save the file
					save_path.write_text(content_to_save, encoding="utf-8")
					
					file_size = len(content_to_save.encode('utf-8'))
					self._log(f"💾 Successfully saved {file_size:,} bytes to: {save_path}")
					self._status("Fetch & Save completed successfully")
					
					# Update preview with success message and data analysis
					success_msg = f"✅ Fetch & Save Completed!\n\n"
					success_msg += f"Location: {save_path}\n"
					success_msg += f"Size: {file_size:,} bytes\n"
					success_msg += f"Type: {fetch_type.upper()}\n\n"
					
					# For CSV data, show analysis instead of raw content
					if fetch_type in ["csv", "odata_csv"] and self._looks_like_csv(content_to_save):
						success_msg += self._analyze_csv_data(content_to_save)
					else:
						# For non-CSV data, show content preview as before
						success_msg += "--- CONTENT PREVIEW ---\n"
						success_msg += content_to_save[:3000]
						if len(content_to_save) > 3000:
							success_msg += f"\n\n... and {len(content_to_save) - 3000:,} more characters"
					
					self.response_preview.delete("1.0", "end")
					self.response_preview.insert("1.0", success_msg)
				else:
					self._log("💡 Save operation cancelled by user")
					self._status("Fetch completed - save cancelled")
					
			except Exception as save_error:
				self._log(f"❌ Failed to save file: {save_error}")
				self._status("Fetch completed - save failed")
				
				# Show error in preview
				error_msg = f"❌ Save Failed\n\nError: {str(save_error)}\n\n"
				error_msg += "Data was fetched successfully but saving failed.\n"
				error_msg += "You can still copy the content from below:\n\n"
				error_msg += "--- CONTENT ---\n"
				error_msg += result["full_content"][:2000]
				if len(result["full_content"]) > 2000:
					error_msg += f"\n\n... and {len(result['full_content']) - 2000:,} more characters"
				
				self.response_preview.delete("1.0", "end")
				self.response_preview.insert("1.0", error_msg)
		
		def error(e: Exception):
			self._fetch_progress(0)
			self._log(f"❌ External fetch failed: {e}")
			self.response_preview.delete("1.0", "end")
			self.response_preview.insert("1.0", f"❌ Fetch Failed\n\nError: {str(e)}")
			self._status("Fetch & Save failed")
		
		self._fetch_progress(0.5)
		self._status("Fetching external data...")
		self.runner.run(task, on_done=done, on_error=error)

	def _on_save_fetched_data(self):
		"""Save the fetched data to a file using Windows file dialog"""
		if not hasattr(self, '_last_fetched_data') or not self._last_fetched_data:
			self._log("❌ No data available to save. Please fetch data first.")
			return
		
		try:
			# Import the file dialog
			if HAS_CTK and hasattr(ctk, "filedialog") and ctk.filedialog:
				from tkinter import filedialog as fd
			else:
				from tkinter import filedialog as fd
			
			# Determine file extension and type based on fetch type
			fetch_type = getattr(self, '_last_fetch_type', 'generic')
			url = getattr(self, '_last_fetch_url', 'fetched_data')
			
			# Extract a reasonable filename from URL
			import urllib.parse
			parsed_url = urllib.parse.urlparse(url)
			url_filename = Path(parsed_url.path).name or "fetched_data"
			
			# Remove any existing extension for clean naming
			base_name = Path(url_filename).stem if '.' in url_filename else url_filename
			
			# Set file extension and types based on fetch type
			if fetch_type == "csv" or fetch_type == "odata_csv":
				default_ext = ".csv"
				initial_filename = f"{base_name}.csv"
				filetypes = [
					("CSV Files", "*.csv"),
					("Text Files", "*.txt"), 
					("All Files", "*.*")
				]
			elif fetch_type == "odata_raw":
				default_ext = ".json"
				initial_filename = f"{base_name}.json"
				filetypes = [
					("JSON Files", "*.json"),
					("Text Files", "*.txt"),
					("All Files", "*.*")
				]
			else:  # generic
				default_ext = ".html"
				initial_filename = f"{base_name}.html"
				filetypes = [
					("HTML Files", "*.html"),
					("Text Files", "*.txt"),
					("JSON Files", "*.json"),
					("CSV Files", "*.csv"),
					("All Files", "*.*")
				]
			
			# Open the save dialog
			file_path = fd.asksaveasfilename(
				title="Save Downloaded Data",
				defaultextension=default_ext,
				filetypes=filetypes,
				initialfile=initial_filename,
				initialdir=str(Path.home() / "Downloads")  # Default to Downloads folder using pathlib
			)
			
			if file_path:
				# Convert to Path object for consistent handling
				save_path = Path(file_path)
				
				# Get the content to save
				content_to_save = self._last_fetched_data
				
				# If it's a CSV file and the content looks like CSV, clean up formatting
				if (fetch_type in ["csv", "odata_csv"] or save_path.suffix.lower() == ".csv") and self._looks_like_csv(content_to_save):
					content_to_save = self._clean_csv_content(content_to_save)
					self._log("🧹 Cleaned CSV formatting (removed extra blank lines)")
				
				# Save the file
				save_path.write_text(content_to_save, encoding="utf-8")
				
				file_size = len(content_to_save.encode('utf-8'))
				self._log(f"💾 Successfully saved {file_size:,} bytes to: {save_path}")
				self._status("File saved successfully")
				
				# Show success message in preview
				success_msg = f"✅ File Saved Successfully!\n\n"
				success_msg += f"Location: {save_path}\n"
				success_msg += f"Size: {file_size:,} bytes\n"
				success_msg += f"Type: {fetch_type.upper()}\n\n"
				success_msg += "--- ORIGINAL CONTENT PREVIEW ---\n"
				success_msg += content_to_save[:3000]
				if len(content_to_save) > 3000:
					success_msg += f"\n\n... and {len(content_to_save) - 3000:,} more characters"
				
				self.response_preview.delete("1.0", "end")
				self.response_preview.insert("1.0", success_msg)
			else:
				self._log("💡 Save operation cancelled by user")
				self._status("Save cancelled")
				
		except Exception as e:
			self._log(f"❌ Failed to save file: {e}")
			self._status("Save failed")
			
			# Show error in preview
			error_msg = f"❌ Save Failed\n\nError: {str(e)}\n\n"
			error_msg += "You can still copy the content from below:\n\n"
			error_msg += "--- CONTENT ---\n"
			error_msg += self._last_fetched_data[:2000]
			if len(self._last_fetched_data) > 2000:
				error_msg += f"\n\n... and {len(self._last_fetched_data) - 2000:,} more characters"
			
			self.response_preview.delete("1.0", "end")
			self.response_preview.insert("1.0", error_msg)

	def _offer_save_fetched_data(self):
		"""Offer to save the fetched data to a file"""
		if not hasattr(self, '_last_fetched_data') or not self._last_fetched_data:
			return
		
		def save_data():
			try:
				if HAS_CTK and hasattr(ctk, "filedialog") and ctk.filedialog:
					from tkinter import filedialog as fd
				else:
					from tkinter import filedialog as fd
				
				# Suggest file extension based on fetch type
				fetch_type = getattr(self, '_last_fetch_type', 'generic')
				if fetch_type == "csv":
					default_ext = ".csv"
					filetypes = [("CSV Files", "*.csv"), ("All Files", "*.*")]
				elif fetch_type == "odata":
					default_ext = ".json"
					filetypes = [("JSON Files", "*.json"), ("All Files", "*.*")]
				else:
					default_ext = ".html"
					filetypes = [("HTML Files", "*.html"), ("Text Files", "*.txt"), ("All Files", "*.*")]
				
				path = fd.asksaveasfilename(
					defaultextension=default_ext,
					filetypes=filetypes,
					initialfile=f"fetched_data{default_ext}"
				)
				
				if path:
					# Use pathlib for consistent file handling
					save_path = Path(path)
					save_path.write_text(self._last_fetched_data, encoding="utf-8")
					self._log(f"💾 Saved fetched data to: {save_path}")
					self._status("Data saved successfully")
			except Exception as e:
				self._log(f"❌ Save failed: {e}")
				self._status("Save failed")
		
		# For now, just log that data is available for saving
		self._log("💡 Tip: You can copy the content from the preview. Save functionality available on request.")

	# --- Events ---
	def _apply_settings(self):
		"""Apply settings from the current tab's form fields"""
		try:
			schema = self._entry_get(self.schema_entry) if hasattr(self, 'schema_entry') else None
			records = self._entry_get(self.records_entry) if hasattr(self, 'records_entry') else None
			
			if schema:
				self.schema_var = self._ctk_string(schema)
			if records:
				self.records_var = self._ctk_string(records)
			
			# Get compression from menu or entry
			if hasattr(self, 'compression_menu') and hasattr(self.compression_menu, 'get'):
				try:
					comp = self.compression_menu.get()
					if comp:
						self.compression_var = self._ctk_string(comp)
				except Exception:
					pass
			elif hasattr(self, 'compression_entry'):
				comp = self._entry_get(self.compression_entry)
				if comp:
					self.compression_var = self._ctk_string(comp)
			
			self._log(f"Applied settings: schema={self.schema_var.get()}, records={self.records_var.get()}, compression={self.compression_var.get()}")
		except Exception as e:
			self._log(f"Error applying settings: {e}")

	def _on_generate_data(self):
		"""Generate sample data for upload"""
		try:
			self._apply_settings()  # Apply any form changes first
			n = max(1, int(self.records_var.get()))
		except Exception:
			n = 1000
			self.records_var = self._ctk_string("1000")
		self.generated_data = DataGenerator.generate_sample_data(n)
		self._log(f"✅ Generated {len(self.generated_data):,} sample records.")

	def _on_write(self):
		"""Upload data to server"""
		def task():
			self._apply_settings()  # Apply any form changes first
			if not hasattr(self, "generated_data"):
				n = max(1, int(self.records_var.get()))
				self.generated_data = DataGenerator.generate_sample_data(n)
			t0 = time.perf_counter()
			res = self.api.write_polars(self.schema_var.get(), self.generated_data, self.compression_var.get())
			dt = time.perf_counter() - t0
			return res, dt

		def done(result):
			res, dt = result
			self._progress(0)
			self._log("📤 UPLOAD SUCCESS:\n" + _format_json(res))
			self._status(f"Upload completed in {dt:.2f}s, {res.get('throughput_records_per_second','?')} rps")

		def error(e: Exception):
			self._progress(0)
			self._log(f"❌ UPLOAD ERROR: {e}")
			self._status("Upload failed")

		self._progress(0.3)
		self._status("Uploading data...")
		self.runner.run(task, on_done=done, on_error=error)

	def _on_read(self):
		"""Read data from server"""
		def task():
			self._apply_settings()  # Apply any form changes first
			t0 = time.perf_counter()
			table, byte_len = self.api.read_polars(self.schema_var.get())
			dt = time.perf_counter() - t0
			return table, byte_len, dt

		def done(result):
			table, byte_len, dt = result
			self._progress(0)
			if table is None:
				self._log(f"📥 READ SUCCESS (raw bytes): {byte_len:,} bytes in {dt:.2f}s. Install pyarrow to decode.")
			else:
				rows = len(table)
				cols = len(table.column_names)
				sample = table.slice(0, min(3, rows)).to_pydict() if rows else {}
				self._log(
					f"📥 READ SUCCESS:\n"
					f"Records: {rows:,}\n"
					f"Columns: {cols}\n"
					f"Bytes: {byte_len:,}\n"
					f"Duration: {dt:.2f}s\n"
					f"Sample:\n{_format_json(sample)}"
				)
			self._status(f"Read completed in {dt:.2f}s")

		def error(e: Exception):
			self._progress(0)
			self._log(f"❌ READ ERROR: {e}")
			self._status("Read failed")

		self._progress(0.2)
		self._status("Reading data...")
		self.runner.run(task, on_done=done, on_error=error)

	def _on_read_and_save(self):
		"""Save the raw Arrow stream to a file for external tools"""
		def task():
			self._apply_settings()  # Apply any form changes first
			url = f"{self.api.base_url}/read/polars/{self.schema_var.get()}"
			resp = requests.get(url, timeout=600)
			resp.raise_for_status()
			return resp.content

		def done(content: bytes):
			path = None
			try:
				if HAS_CTK and hasattr(ctk, "filedialog") and ctk.filedialog:
					from tkinter import filedialog as fd  # type: ignore
				else:
					from tkinter import filedialog as fd  # type: ignore
				path = fd.asksaveasfilename(
					defaultextension=".arrow",
					filetypes=[("Arrow IPC", "*.arrow"), ("All Files", "*.*")],
					initialfile=f"{self.schema_var.get()}_polars.arrow",
				)
			except Exception:
				pass
			if path:
				# Use pathlib for consistent file handling
				save_path = Path(path)
				save_path.write_bytes(content)
				self._log(f"💾 Saved Arrow IPC stream to: {save_path}")
				self._status("File saved successfully")
			else:
				self._log("❌ Save canceled.")
				self._status("Ready")

		def error(e: Exception):
			self._log(f"❌ DOWNLOAD ERROR: {e}")
			self._status("Download failed")

		self._status("Downloading...")
		self.runner.run(task, on_done=done, on_error=error)

	def _on_export(self):
		"""Download Arrow stream, then convert to CSV or JSONL and save"""
		def task():
			self._apply_settings()  # Apply any form changes first
			url = f"{self.api.base_url}/read/polars/{self.schema_var.get()}"
			resp = requests.get(url, timeout=600)
			resp.raise_for_status()
			return resp.content

		def done(content: bytes):
			if pa is None or pa_ipc is None:
				self._log("❌ Export requires pyarrow. Please install pyarrow to export as text or CSV.")
				self._status("Export unavailable (pyarrow missing)")
				return

			selected = None
			try:
				if getattr(self, "export_format_menu", None) is not None and hasattr(self.export_format_menu, "get"):
					selected = (self.export_format_menu).get()  # type: ignore[union-attr]
				elif hasattr(self.export_format_var, "get"):
					selected = self.export_format_var.get()
				else:
					selected = "CSV"
			except Exception:
				selected = "CSV"

			# Ask for path based on selected format
			path = None
			try:
				if HAS_CTK and hasattr(ctk, "filedialog") and ctk.filedialog:
					from tkinter import filedialog as fd  # type: ignore
				else:
					from tkinter import filedialog as fd  # type: ignore
				ext = ".jsonl" if "JSONL" in selected else ".csv"
				initname = f"{self.schema_var.get()}_export{ext}"
				filetypes = [("JSON Lines", "*.jsonl"), ("CSV", "*.csv"), ("All Files", "*.*")]
				path = fd.asksaveasfilename(defaultextension=ext, filetypes=filetypes, initialfile=initname)
			except Exception:
				pass
			if not path:
				self._log("❌ Export canceled.")
				self._status("Ready")
				return

			try:
				# Build a reader from the Arrow IPC content
				reader = pa_ipc.open_stream(pa.BufferReader(content))
				export_path = Path(path)
				
				if "CSV" in selected:
					# Convert to CSV with proper tabular format
					self._export_to_csv(reader, export_path)
				else:
					# JSON Lines (one JSON object per line)
					with export_path.open("w", encoding="utf-8") as f:
						for batch in reader:
							for row in batch.to_pylist():
								f.write(json.dumps(row, ensure_ascii=False))
								f.write("\n")
					self._log(f"📁 Exported JSONL to: {export_path}")
					self._status("Text file saved")
			except Exception as e:  # noqa: BLE001
				self._log(f"❌ EXPORT ERROR: {e}")
				self._status("Export failed")

		self._status("Exporting...")
		self.runner.run(task, on_done=done, on_error=lambda e: (self._log(f"❌ DOWNLOAD ERROR: {e}"), self._status("Download failed")))

	def _export_to_csv(self, reader, path: Path):
		"""Export Arrow data to CSV with proper tabular format."""
		try:
			# First, try using Polars if available (more robust)
			if pl is not None:
				table = reader.read_all()
				pldf = pl.from_arrow(table)  # type: ignore[assignment]
				# Use pathlib for path handling
				pldf.write_csv(str(path))  # type: ignore[attr-defined]
				self._log(f"Exported CSV to: {path} (using Polars)")
				self._status("CSV saved")
				return
		except Exception as e:
			self._log(f"Polars CSV export failed, falling back to manual CSV: {e}")

		# Fallback: Manual CSV export using Python's csv module
		try:
			# Read all data into memory
			all_rows = []
			column_names = None
			
			for batch in reader:
				batch_data = batch.to_pylist()
				if batch_data:
					# Get column names from first batch
					if column_names is None:
						column_names = list(batch_data[0].keys())
					all_rows.extend(batch_data)

			if not all_rows or column_names is None:
				self._log("❌ No data to export")
				self._status("No data")
				return

			# Write CSV with proper formatting
			with open(path, "w", newline="", encoding="utf-8") as csvfile:
				writer = csv.DictWriter(csvfile, fieldnames=column_names)
				
				# Write header row (column names)
				writer.writeheader()
				
				# Write data rows
				for row in all_rows:
					# Ensure all values are properly formatted for CSV
					clean_row = {}
					for key, value in row.items():
						if value is None:
							clean_row[key] = ""
						elif isinstance(value, (dict, list)):
							# Convert complex types to JSON strings
							clean_row[key] = json.dumps(value, ensure_ascii=False)
						else:
							clean_row[key] = str(value)
					writer.writerow(clean_row)

			self._log(f"📁 Exported CSV to: {path} (manual CSV export, {len(all_rows):,} rows)")
			self._status("CSV file saved")
			
		except Exception as e:
			self._log(f"❌ Manual CSV export failed: {e}")
			self._status("CSV export failed")
			raise

	def _on_list_schemas(self):
		"""List available schemas"""
		def task():
			return self.api.list_schema_families()

		def done(families: List[str]):
			self._log("📋 Available Schemas:\n" + "\n".join(f"  • {s}" for s in families))
			self._status("Schemas listed successfully")

		def error(e: Exception):
			self._log(f"❌ SCHEMAS ERROR: {e}")
			self._status("Schema listing failed")

		self._status("Listing schemas...")
		self.runner.run(task, on_done=done, on_error=error)

	def _on_register_example(self):
		"""Register an example schema"""
		# Create a schema definition inspired by scripts/api_bench_full.py
		schema_definition = {
			"description": "Schema for well production data.",
			"table_name": "well_production",
			"primary_key": ["field_code", "well_code", "production_period"],
			"properties": [
				{"name": "field_code", "type": "integer", "db_type": "BIGINT", "required": True, "primary_key": True},
				{"name": "field_name", "type": "string", "db_type": "VARCHAR"},
				{"name": "well_code", "type": "integer", "db_type": "BIGINT", "required": True, "primary_key": True},
				{"name": "well_reference", "type": "string", "db_type": "VARCHAR"},
				{"name": "well_name", "type": "string", "db_type": "VARCHAR"},
				{"name": "production_period", "type": "string", "db_type": "TIMESTAMP", "required": True, "primary_key": True},
				{"name": "days_on_production", "type": "integer", "db_type": "BIGINT"},
				{"name": "oil_production_kbd", "type": "number", "db_type": "DOUBLE"},
				{"name": "gas_production_mmcfd", "type": "number", "db_type": "DOUBLE"},
				{"name": "liquids_production_kbd", "type": "number", "db_type": "DOUBLE"},
				{"name": "water_production_kbd", "type": "number", "db_type": "DOUBLE"},
				{"name": "data_source", "type": "string", "db_type": "VARCHAR"},
				{"name": "source_data", "type": "string", "db_type": "VARCHAR"},
				{"name": "partition_0", "type": "string", "db_type": "VARCHAR"},
			],
		}

		def task():
			self._apply_settings()  # Apply any form changes first
			return self.api.register_schema(self.schema_var.get(), schema_definition)

		def done(res: Dict[str, Any]):
			self._log("➕ REGISTERED SCHEMA:\n" + _format_json(res))
			self._status("Schema registered successfully")

		def error(e: Exception):
			self._log(f"❌ REGISTER SCHEMA ERROR: {e}")
			self._status("Schema registration failed")

		self._status("Registering schema...")
		self.runner.run(task, on_done=done, on_error=error)

	def _on_refresh_schemas(self):
		"""Refresh the schema list"""
		self._log("🔄 Refreshing schema list...")
		self._on_list_schemas()

	def _on_get_schema_versions(self):
		"""Get versions for a specific schema"""
		schema_name = self.schema_details_entry.get().strip() if hasattr(self, 'schema_details_entry') else ""
		if not schema_name:
			self._log("❌ Please enter a schema name first")
			return

		def task():
			return self.api.get_schema_versions(schema_name)

		def done(versions: List[int]):
			versions_str = ", ".join(str(v) for v in sorted(versions))
			self._log(f"📊 Versions for '{schema_name}':\n  {versions_str}")
			self._status(f"Found {len(versions)} versions")
			
			# Update schema display
			if hasattr(self, 'schema_display'):
				self.schema_display.delete("1.0" if hasattr(self.schema_display, 'delete') else "0.0", "end")
				content = f"Schema: {schema_name}\nVersions: {versions_str}\n\nSelect 'Get Latest' to view schema details."
				self.schema_display.insert("1.0" if hasattr(self.schema_display, 'insert') else "0.0", content)

		def error(e: Exception):
			self._log(f"❌ GET VERSIONS ERROR: {e}")
			self._status("Failed to get schema versions")

		self._status(f"Getting versions for {schema_name}...")
		self.runner.run(task, on_done=done, on_error=error)

	def _on_get_schema_details(self):
		"""Get detailed schema information (enhanced version of get latest)"""
		schema_name = self.schema_details_entry.get().strip() if hasattr(self, 'schema_details_entry') else ""
		if not schema_name:
			# Try fallback to main schema entry
			schema_name = self.schema_var.get().strip()
		
		if not schema_name:
			self._log("❌ Please enter a schema name first")
			return

		def task():
			return self.api.get_latest_schema(schema_name)

		def done(schema: Dict[str, Any]):
			# Format schema for display
			formatted_schema = self._format_schema_for_display(schema)
			self._log(f"🔍 Schema Details for '{schema_name}':\n{formatted_schema}")
			self._status("Schema details loaded")
			
			# Update schema display
			if hasattr(self, 'schema_display'):
				self.schema_display.delete("1.0" if hasattr(self.schema_display, 'delete') else "0.0", "end")
				self.schema_display.insert("1.0" if hasattr(self.schema_display, 'insert') else "0.0", formatted_schema)

		def error(e: Exception):
			self._log(f"❌ GET SCHEMA DETAILS ERROR: {e}")
			self._status("Failed to get schema details")

		self._status(f"Getting details for {schema_name}...")
		self.runner.run(task, on_done=done, on_error=error)

	def _format_schema_for_display(self, schema: Dict[str, Any]) -> str:
		"""Format schema data for readable display"""
		lines = []
		lines.append(f"📋 Schema: {schema.get('name', 'Unknown')}")
		lines.append(f"📝 Description: {schema.get('description', 'No description')}")
		lines.append(f"🔢 Version: {schema.get('version', 'Unknown')}")
		lines.append(f"🗂️ Table: {schema.get('table_name', 'Unknown')}")
		
		primary_keys = schema.get('primary_key', [])
		if primary_keys:
			lines.append(f"🔑 Primary Keys: {', '.join(primary_keys)}")
		
		lines.append("\n📊 Properties:")
		lines.append("-" * 50)
		
		properties = schema.get('properties', [])
		for prop in properties:
			name = prop.get('name', 'Unknown')
			prop_type = prop.get('type', 'Unknown')
			db_type = prop.get('db_type', 'Unknown')
			required = prop.get('required', False)
			is_pk = prop.get('primary_key', False)
			
			flags = []
			if required:
				flags.append("Required")
			if is_pk:
				flags.append("PK")
			
			flag_str = f" ({', '.join(flags)})" if flags else ""
			lines.append(f"  • {name}: {prop_type} ({db_type}){flag_str}")
			
			if prop.get('description'):
				lines.append(f"    📝 {prop['description']}")
		
		return "\n".join(lines)

	# Override the existing _on_get_latest_schema to update display
	def _on_get_latest_schema(self):
		"""Get latest schema for the current schema name (enhanced for schema management)"""
		# Check if we're in schema management mode
		if hasattr(self, 'schema_details_entry'):
			self._on_get_schema_details()
			return
		
		# Original implementation for backward compatibility
		def task():
			self._apply_settings()  # Apply any form changes first
			return self.api.get_latest_schema(self.schema_var.get())

		def done(schema: Dict[str, Any]):
			self._log("🔍 Latest Schema:\n" + _format_json(schema))
			self._status("Schema loaded successfully")

		def error(e: Exception):
			self._log(f"❌ GET LATEST SCHEMA ERROR: {e}")
			self._status("Schema loading failed")

		self._status("Getting latest schema...")
		self.runner.run(task, on_done=done, on_error=error)

	# --- Utils ---
	def _ctk_string(self, value: str):
		"""Create a string variable that works with both CustomTkinter and tkinter"""
		try:
			from customtkinter import StringVar as CTkStringVar  # type: ignore
			var = CTkStringVar(value=value)
			return var
		except Exception:
			class _Var:
				def __init__(self, v: str):
					self._v = v

				def get(self):
					return self._v
				
				def set(self, value: str):
					self._v = value

			return _Var(value)

	def _ctk_boolean(self, value: bool):
		"""Create a boolean variable that works with both CustomTkinter and tkinter"""
		try:
			from customtkinter import BooleanVar as CTkBooleanVar
			var = CTkBooleanVar(value=value)
			return var
		except Exception:
			class _BoolVar:
				def __init__(self, v: bool):
					self._v = v

				def get(self):
					return self._v
				
				def set(self, value: bool):
					self._v = value

			return _BoolVar(value)

	def _entry_set(self, entry, value: str):
		"""Set the value of an entry widget"""
		try:
			entry.delete(0, "end")
			entry.insert(0, value)
		except Exception:
			pass

	def _entry_get(self, entry) -> str:
		"""Get the value from an entry widget"""
		try:
			return entry.get()
		except Exception:
			return ""

	def _log(self, text: str):
		"""Add text to the log with timestamp and update UI status"""
		try:
			timestamp = datetime.now().strftime("%H:%M:%S")
			log_text = f"[{timestamp}] {text}\n"
			self.log.insert("end", log_text)
			if hasattr(self.log, "see"):
				self.log.see("end")
				
			# Also update UI status using UIController
			# Extract the main message without emojis and timestamp for status
			clean_message = text.replace("✅", "").replace("❌", "").replace("⚠️", "").replace("📊", "").replace("📁", "").replace("💡", "").strip()
			self.ui_controller.update_status(clean_message)
			
		except Exception:
			pass

	def _status(self, text: str):
		"""Update status using UIController for consistent UI state management"""
		try:
			# Use UIController for centralized status management
			self.ui_controller.update_status(text)
			
			# Keep the debug print for development
			timestamp = datetime.now().strftime("%H:%M:%S")
			print(f"[{timestamp}] Status: {text}")  # For debugging
		except Exception:
			pass

	def _progress(self, value: float):
		"""Update progress bar using UIController"""
		try:
			# Use UIController for centralized progress management
			self.ui_controller.update_progress(value)
			
			# Fallback to direct widget access if needed
			if hasattr(self, 'progress') and hasattr(self.progress, "set"):
				self.progress.set(value)
		except Exception:
			pass

	def _fetch_progress(self, value: float):
		"""Update fetch progress bar if available"""
		try:
			if hasattr(self, 'fetch_progress') and hasattr(self.fetch_progress, "set"):
				self.fetch_progress.set(value)
		except Exception:
			pass

	def _looks_like_csv(self, content: str) -> bool:
		"""Check if content looks like CSV data"""
		if not content.strip():
			return False
		
		lines = content.strip().split('\n')
		if len(lines) < 2:
			return False
		
		# Check if first few lines have commas (simple heuristic)
		first_line = lines[0].strip()
		if ',' not in first_line:
			return False
		
		# Count commas in first line
		comma_count = first_line.count(',')
		if comma_count == 0:
			return False
		
		# Check if at least one more line has similar comma count
		for line in lines[1:min(5, len(lines))]:
			if line.strip() and abs(line.count(',') - comma_count) <= 1:
				return True
		
		return False

	def _clean_csv_content(self, content: str) -> str:
		"""Clean CSV content by removing extra blank lines and normalizing format"""
		try:
			lines = content.split('\n')
			cleaned_lines = []
			
			for line in lines:
				# Keep non-empty lines and lines with just whitespace that might be significant
				stripped = line.strip()
				if stripped:  # Non-empty line
					cleaned_lines.append(line.rstrip())  # Remove trailing whitespace but keep leading
				# Skip completely empty lines
			
			# Rejoin with single newlines
			return '\n'.join(cleaned_lines)
		except Exception as e:
			self._log(f"Warning: Could not clean CSV content: {e}")
			return content

	def _fetch_all_odata_pages(self, initial_url: str, auth, headers: dict, first_response_text: str) -> str:
		"""Fetch all pages from an OData API that supports pagination with rate limiting and retry logic"""
		import time
		
		try:
			all_records = []
			
			# Process the first response
			first_data = json.loads(first_response_text)
			if isinstance(first_data, dict) and 'value' in first_data:
				all_records.extend(first_data['value'])
				self._log(f"📄 Page 1: {len(first_data['value'])} records")
				
				# Check for pagination
				next_link = first_data.get('@odata.nextLink')
				page_count = 1
				
				# Check if we have count information for progress tracking
				total_count = first_data.get('@odata.count')
				if total_count:
					self._log(f"📊 Total records available: {total_count:,}")
				
				# Rate limiting configuration
				base_delay = 4.0  # Base delay between pages (4 seconds)
				max_retries = 5   # Maximum retry attempts per page
				
				while next_link and page_count < 100:  # Safety limit to prevent infinite loops
					page_count += 1
					
					# Add delay between pages to be gentle on the API
					if page_count > 2:  # No delay after first page since we already have it
						self._log(f"⏱️ Waiting {base_delay}s before fetching page {page_count} (API rate limiting)...")
						time.sleep(base_delay)
					
					# Update progress if we know the total count
					if total_count:
						current_progress = min(0.9, len(all_records) / total_count * 0.8 + 0.1)
						self._fetch_progress(current_progress)
					
					self._log(f"🔄 Fetching page {page_count}... ({len(all_records):,} records so far)")
					
					# Retry logic with exponential backoff
					retry_count = 0
					page_success = False
					
					while retry_count < max_retries and not page_success:
						try:
							# Calculate retry delay using formula: time = retry_number * 10 seconds
							if retry_count > 0:
								retry_delay = retry_count * 10  # 10s, 20s, 30s, 40s, 50s
								self._log(f"🔄 Retry attempt {retry_count}/{max_retries} for page {page_count} after {retry_delay}s delay...")
								time.sleep(retry_delay)
							
							# Make the request
							next_response = requests.get(next_link, auth=auth, headers=headers, timeout=300)
							next_response.raise_for_status()
							
							next_data = json.loads(next_response.text)
							if isinstance(next_data, dict) and 'value' in next_data:
								page_records = next_data['value']
								all_records.extend(page_records)
								self._log(f"📄 Page {page_count}: {len(page_records)} records ({len(all_records):,} total)")
								
								# Update next link
								next_link = next_data.get('@odata.nextLink')
								page_success = True
								
								if retry_count > 0:
									self._log(f"✅ Page {page_count} succeeded after {retry_count} retries")
							else:
								self._log(f"⚠️ Page {page_count} has no 'value' array, stopping pagination")
								page_success = True  # Stop pagination, not a retry case
								break
								
						except Exception as e:
							retry_count += 1
							if retry_count < max_retries:
								self._log(f"❌ Error fetching page {page_count} (attempt {retry_count}/{max_retries}): {e}")
							else:
								self._log(f"❌ Failed to fetch page {page_count} after {max_retries} attempts: {e}")
								# Stop pagination on final failure
								next_link = None
								break
				
				if page_count >= 100:
					self._log("⚠️ Reached pagination limit (100 pages) - stopping for safety")
				
				# Create combined response
				combined_data = {
					'value': all_records,
					'@odata.context': first_data.get('@odata.context', ''),
					'@odata.count': len(all_records)
				}
				
				if page_count > 1:
					self._log(f"✅ Pagination complete: {len(all_records):,} total records from {page_count} pages")
				else:
					self._log(f"✅ Single page response: {len(all_records):,} records")
				
				return json.dumps(combined_data)
			else:
				self._log("⚠️ First response doesn't contain OData 'value' array")
				return first_response_text
				
		except Exception as e:
			self._log(f"❌ Error during pagination: {e}")
			return first_response_text

	def _convert_odata_to_csv(self, json_content: str) -> str:
		"""Convert OData JSON response to CSV format by extracting the 'value' array"""
		try:
			# Parse the JSON
			data = json.loads(json_content)
			
			# Check if it's a valid OData response with 'value' array
			if not isinstance(data, dict) or 'value' not in data:
				self._log("⚠️ No 'value' array found in OData response")
				return ""
			
			value_array = data['value']
			if not isinstance(value_array, list) or not value_array:
				self._log("⚠️ 'value' array is empty or not a list")
				return ""
			
			# Get all unique keys from all objects (some objects might have different fields)
			all_keys = set()
			for item in value_array:
				if isinstance(item, dict):
					all_keys.update(item.keys())
			
			if not all_keys:
				self._log("⚠️ No fields found in OData objects")
				return ""
			
			# Sort keys for consistent column order
			sorted_keys = sorted(all_keys)
			
			# Create CSV content
			csv_lines = []
			
			# Add header row - no quotes for clean column names
			csv_lines.append(','.join(sorted_keys))
			
			# Add data rows
			for item in value_array:
				if isinstance(item, dict):
					row_values = []
					for key in sorted_keys:
						value = item.get(key, '')
						
						# Handle different value types with proper CSV quoting
						if value is None:
							# Empty value, no quotes needed
							row_values.append('')
						elif isinstance(value, (int, float)):
							# Numeric values - no quotes
							row_values.append(str(value))
						elif isinstance(value, bool):
							# Boolean values - no quotes, lowercase
							row_values.append(str(value).lower())
						elif isinstance(value, (dict, list)):
							# Complex objects - convert to JSON string and quote
							json_str = json.dumps(value, ensure_ascii=False)
							escaped_json = json_str.replace('"', '""')
							row_values.append(f'"{escaped_json}"')
						elif isinstance(value, str):
							# String values - escape quotes and quote only if necessary
							if ('"' in value or ',' in value or '\n' in value or '\r' in value or 
								value.startswith(' ') or value.endswith(' ')):
								# Needs quoting due to special characters
								escaped_value = value.replace('"', '""')
								row_values.append(f'"{escaped_value}"')
							else:
								# Simple string, no special characters - no quotes needed
								row_values.append(value)
						else:
							# Other types - convert to string, no quotes if numeric-like
							str_value = str(value)
							# Check if it looks like a number
							try:
								float(str_value)
								# It's numeric, no quotes
								row_values.append(str_value)
							except ValueError:
								# Not numeric, quote it
								escaped_value = str_value.replace('"', '""')
								if ('"' in str_value or ',' in str_value or '\n' in str_value or '\r' in str_value or 
									str_value.startswith(' ') or str_value.endswith(' ')):
									row_values.append(f'"{escaped_value}"')
								else:
									row_values.append(str_value)
					
					csv_lines.append(','.join(row_values))
			
			csv_content = '\n'.join(csv_lines)
			self._log(f"✅ Converted OData JSON to CSV: {len(value_array)} records, {len(sorted_keys)} columns")
			return csv_content
			
		except json.JSONDecodeError as e:
			self._log(f"❌ Invalid JSON in OData response: {e}")
			return ""
		except Exception as e:
			self._log(f"❌ Error converting OData to CSV: {e}")
			return ""

	def _analyze_csv_data(self, csv_content: str, max_sample_rows: int = 3) -> str:
		"""Analyze CSV data and return a DataFrame-like summary with quality checks"""
		try:
			import csv
			from io import StringIO
			
			# Parse CSV content
			csv_reader = csv.reader(StringIO(csv_content))
			rows = list(csv_reader)
			
			if not rows:
				return "📊 No data found in CSV"
			
			# Extract headers and data
			headers = rows[0] if rows else []
			data_rows = rows[1:] if len(rows) > 1 else []
			
			if not headers:
				return "📊 No headers found in CSV"
			
			# Basic statistics
			num_columns = len(headers)
			num_rows = len(data_rows)
			
			# Data quality checks
			quality_issues = []
			
			# Check for empty rows
			empty_rows = sum(1 for row in data_rows if not any(cell.strip() for cell in row))
			if empty_rows > 0:
				quality_issues.append(f"{empty_rows} empty rows")
			
			# Check for missing values per column
			missing_counts = {}
			for col_idx, header in enumerate(headers):
				missing = sum(1 for row in data_rows if col_idx >= len(row) or not row[col_idx].strip())
				if missing > 0:
					missing_counts[header] = missing
			
			# Analyze data types for each column (sample-based)
			column_info = {}
			for col_idx, header in enumerate(headers):
				values = [row[col_idx] if col_idx < len(row) else "" for row in data_rows[:100]]  # Sample first 100 rows
				non_empty_values = [v.strip() for v in values if v.strip()]
				
				if not non_empty_values:
					column_info[header] = "empty"
					continue
				
				# Check data type
				numeric_count = 0
				date_count = 0
				boolean_count = 0
				
				for value in non_empty_values[:10]:  # Check first 10 non-empty values
					# Check if numeric
					try:
						float(value)
						numeric_count += 1
					except ValueError:
						pass
					
					# Check if boolean
					if value.lower() in ['true', 'false', '1', '0', 'yes', 'no']:
						boolean_count += 1
					
					# Check if date-like
					if any(char in value for char in ['-', '/', 'T', ':']):
						date_count += 1
				
				# Determine predominant type
				total_checked = min(len(non_empty_values), 10)
				if numeric_count >= total_checked * 0.8:
					column_info[header] = "numeric"
				elif boolean_count >= total_checked * 0.8:
					column_info[header] = "boolean"
				elif date_count >= total_checked * 0.5:
					column_info[header] = "datetime"
				else:
					column_info[header] = "text"
			
			# Build summary report
			summary = "📊 CSV DATA ANALYSIS\n"
			summary += "=" * 50 + "\n\n"
			
			# Basic info
			summary += f"📋 DATASET OVERVIEW\n"
			summary += f"   Rows: {num_rows:,}\n"
			summary += f"   Columns: {num_columns}\n\n"
			
			# Column information
			summary += f"📊 COLUMN INFORMATION\n"
			col_table = ""
			col_table += f"┌{'─' * 25}┬{'─' * 12}┬{'─' * 10}┐\n"
			col_table += f"│{'Column Name':<25}│{'Type':<12}│{'Missing':<10}│\n"
			col_table += f"├{'─' * 25}┼{'─' * 12}┼{'─' * 10}┤\n"
			
			for header in headers:
				col_type = column_info.get(header, "unknown")
				missing = missing_counts.get(header, 0)
				missing_str = f"{missing}" if missing > 0 else "-"
				
				# Truncate long column names
				display_name = header[:23] + ".." if len(header) > 25 else header
				
				col_table += f"│{display_name:<25}│{col_type:<12}│{missing_str:<10}│\n"
			
			col_table += f"└{'─' * 25}┴{'─' * 12}┴{'─' * 10}┘\n"
			summary += col_table + "\n"
			
			# Data quality
			if quality_issues or missing_counts:
				summary += f"⚠️  DATA QUALITY ISSUES\n"
				if empty_rows > 0:
					summary += f"   • {empty_rows} empty rows detected\n"
				if missing_counts:
					summary += f"   • Missing values in {len(missing_counts)} columns\n"
				summary += "\n"
			else:
				summary += f"✅ DATA QUALITY: No issues detected\n\n"
			
			# Sample rows
			if data_rows:
				summary += f"🔍 SAMPLE DATA (showing up to {min(max_sample_rows, len(data_rows))} rows)\n"
				
				# Calculate column widths for pretty printing
				col_widths = []
				for col_idx, header in enumerate(headers):
					max_width = len(header)
					# Check sample data for width
					for row_idx in range(min(max_sample_rows, len(data_rows))):
						if col_idx < len(data_rows[row_idx]):
							max_width = max(max_width, len(str(data_rows[row_idx][col_idx])))
					col_widths.append(min(max_width, 20))  # Cap at 20 chars
				
				# Header row
				header_line = "┌" + "┬".join("─" * (w + 2) for w in col_widths) + "┐\n"
				summary += header_line
				
				header_row = "│"
				for idx, header in enumerate(headers):
					display_header = header[:col_widths[idx]] if len(header) > col_widths[idx] else header
					header_row += f" {display_header:<{col_widths[idx]}} │"
				summary += header_row + "\n"
				
				separator = "├" + "┼".join("─" * (w + 2) for w in col_widths) + "┤\n"
				summary += separator
				
				# Data rows
				for row_idx in range(min(max_sample_rows, len(data_rows))):
					row = data_rows[row_idx]
					data_row = "│"
					for col_idx, width in enumerate(col_widths):
						cell_value = row[col_idx] if col_idx < len(row) else ""
						display_value = str(cell_value)[:width] if len(str(cell_value)) > width else str(cell_value)
						data_row += f" {display_value:<{width}} │"
					summary += data_row + "\n"
				
				footer_line = "└" + "┴".join("─" * (w + 2) for w in col_widths) + "┘\n"
				summary += footer_line
				
				if len(data_rows) > max_sample_rows:
					summary += f"\n   ... and {len(data_rows) - max_sample_rows:,} more rows\n"
			
			return summary
			
		except Exception as e:
			return f"❌ Error analyzing CSV data: {e}"
	
	def _initialize_plugin_system(self):
		"""Initialize the plugin system and discover plugins."""
		try:
			self._log("🔌 Initializing plugin system...")
			
			# Discover plugins
			discovered = self.plugin_manager.discover_plugins()
			self._log(f"📦 Discovered {len(discovered)} plugin(s)")
			
			# Log discovered plugins
			for manifest in discovered:
				self._log(f"   • {manifest.info.name} v{manifest.info.version} ({manifest.info.plugin_type.value})")
			
			self._log("✅ Plugin system initialized successfully")
			
		except Exception as e:
			self.error_handler.handle_error(e, "Failed to initialize plugin system")
			self._log("❌ Plugin system initialization failed")
	
	def _build_plugins_tab(self):
		"""Build the plugins management tab."""
		try:
			from frontend.tabs.plugins_tab import PluginsTab
			
			# Create plugins tab instance
			self.plugins_tab = PluginsTab(
				parent=self.tab_content,
				plugin_manager=self.plugin_manager,
				error_handler=self.error_handler,
				ui_adapter=self.ui_adapter
			)
			
		except Exception as e:
			self.error_handler.handle_error(e, "Failed to build plugins tab")
			
			# Fallback UI
			error_frame = ctk.CTkFrame(self.tab_content)
			error_frame.pack(fill="both", expand=True, padx=20, pady=20)
			
			error_label = ctk.CTkLabel(
				error_frame,
				text="❌ Plugin Management Unavailable\n\nThere was an error loading the plugin management interface.",
				font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
			)
			error_label.pack(expand=True)


def main():
	"""Main entry point for the DataForge frontend application"""
	app = DataForgeApp()
	
	# Log startup message
	app._log("🚀 DataForge Frontend started!")
	app._log(f"📡 API Endpoint: {AppConfig.API_BASE_URL}")
	app._log(f"📋 Default Schema: {AppConfig.DEFAULT_SCHEMA}")
	app._log("Welcome! Use the sidebar to navigate between different features.")
	
	app.mainloop()


if __name__ == "__main__":
	main()
