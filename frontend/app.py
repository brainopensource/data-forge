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
import os
import threading
import time
import uuid
import random
import csv
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

import requests
import importlib
import importlib.util

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


# Try to use CustomTkinter; gracefully fall back to tkinter widgets if not installed
spec = importlib.util.find_spec("customtkinter")
if spec is not None:
	ctk = importlib.import_module("customtkinter")  # type: ignore[assignment]
	HAS_CTK = True
	try:
		ctk.set_appearance_mode("dark")
		ctk.set_default_color_theme("blue")
	except Exception:
		pass
else:  # pragma: no cover
	import tkinter as tk
	from tkinter import ttk
	from tkinter import filedialog
	from tkinter import scrolledtext
	from types import SimpleNamespace

	HAS_CTK = False

	# Minimal shim to approximate CustomTkinter API
	class _CTk(tk.Tk):
		def __init__(self):
			super().__init__()
			self.configure(bg='#212121')  # Dark background

	class _CTkFrame(tk.Frame):
		def __init__(self, master=None, **kw):
			super().__init__(master, bg='#2b2b2b', **kw)

	class _CTkLabel(tk.Label):
		def __init__(self, master=None, text="", **kw):
			super().__init__(master, text=text, bg='#2b2b2b', fg='#ffffff', **kw)

	class _CTkEntry(tk.Entry):
		def __init__(self, master=None, placeholder_text: Optional[str] = None, **kw):
			super().__init__(master, bg='#404040', fg='#ffffff', insertbackground='#ffffff', **kw)
			if placeholder_text:
				self.insert(0, placeholder_text)

	class _CTkButton(tk.Button):
		def __init__(self, master=None, text: str = "", command=lambda: None, **kw):  # noqa: B008
			super().__init__(master, text=text, command=command, bg='#1f538d', fg='#ffffff', 
							activebackground='#14375e', activeforeground='#ffffff', **kw)

	class _CTkTextbox(scrolledtext.ScrolledText):
		def __init__(self, master=None, **kw):
			super().__init__(master, bg='#404040', fg='#ffffff', insertbackground='#ffffff', **kw)
		def insert(self, index, chars, *args):  # type: ignore[override]
			super().insert(index, chars)

	class _CTkTabview(ttk.Notebook):
		def __init__(self, master=None, **kw):
			super().__init__(master, **kw)
			self.style = ttk.Style()
			self.style.theme_use('clam')

	class _CTkProgressBar(ttk.Progressbar):
		def set(self, value: float):
			self["value"] = max(0, min(100, value * 100))

	class _CTkOptionMenu(ttk.Combobox):
		def __init__(self, master=None, values=None, variable=None, **kw):  # noqa: D401
			values = values or []
			super().__init__(master, values=values, **kw)
			if variable is not None:
				self._var = variable
				try:
					self.set(variable.get())
				except Exception:
					pass
			self.state("readonly")

	ctk = SimpleNamespace(
		CTk=_CTk,
		CTkFrame=_CTkFrame,
		CTkLabel=_CTkLabel,
		CTkEntry=_CTkEntry,
		CTkButton=_CTkButton,
		CTkTextbox=_CTkTextbox,
		CTkTabview=_CTkTabview,
		CTkProgressBar=_CTkProgressBar,
		CTkOptionMenu=_CTkOptionMenu,
		set_appearance_mode=lambda *a, **k: None,
		set_default_color_theme=lambda *a, **k: None,
		filedialog=filedialog if "filedialog" in globals() else None,
	)


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
# UI Helpers
# ---------------------------


def _format_json(obj: Any) -> str:
	try:
		return json.dumps(obj, indent=2, ensure_ascii=False)
	except Exception:
		return str(obj)


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


class DataForgeApp(ctk.CTk):
	def __init__(self):
		super().__init__()
		self.title("DataForge - Modern Frontend")
		
		# Set favicon if it exists
		try:
			if AppConfig.FAVICON_PATH.exists():
				self.iconbitmap(str(AppConfig.FAVICON_PATH))
		except Exception:
			pass
		
		try:
			self.geometry("1400x900")
			self.minsize(1200, 700)
		except Exception:
			pass

		# State
		self.schema_var = self._ctk_string(AppConfig.DEFAULT_SCHEMA)
		self.records_var = self._ctk_string(AppConfig.DEFAULT_RECORDS)
		self.compression_var = self._ctk_string(AppConfig.DEFAULT_COMPRESSION)
		self.export_format_var = self._ctk_string("CSV")

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
			("upload", "📤 Upload"),
			("download", "📥 Download"),
			("sync", "🔄 Sync")
		]
		
		for tab_id, tab_text in nav_items:
			btn = ctk.CTkButton(
				self.sidebar,
				text=tab_text,
				command=lambda t=tab_id: self._show_tab(t),
				height=40,
				font=ctk.CTkFont(size=14) if HAS_CTK else ("Arial", 12)
			)
			btn.pack(fill="x", padx=20, pady=5)
			self.nav_buttons[tab_id] = btn
		
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
		
		# Update button states
		for btn_id, btn in self.nav_buttons.items():
			if btn_id == tab_id:
				try:
					btn.configure(state="disabled")
				except Exception:
					pass
			else:
				try:
					btn.configure(state="normal")
				except Exception:
					pass
		
		# Clear current content
		for widget in self.tab_content.winfo_children():
			widget.destroy()
		
		# Show appropriate content
		if tab_id == "home":
			self._build_home_tab()
		elif tab_id == "upload":
			self._build_upload_tab()
		elif tab_id == "download":
			self._build_download_tab()
		elif tab_id == "sync":
			self._build_sync_tab()
		
		# Update header
		tab_titles = {
			"home": "🏠 Home",
			"upload": "📤 Upload Data",
			"download": "📥 Download Data", 
			"sync": "🔄 Sync"
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

	def _clear_logs(self):
		"""Clear the log content"""
		try:
			self.log.delete("1.0", "end")
		except Exception:
			pass

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
				with open(path, "wb") as f:
					f.write(content)
				self._log(f"💾 Saved Arrow IPC stream to: {path}")
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
				if "CSV" in selected:
					# Convert to CSV with proper tabular format
					self._export_to_csv(reader, path)
				else:
					# JSON Lines (one JSON object per line)
					with open(path, "w", encoding="utf-8") as f:
						for batch in reader:
							for row in batch.to_pylist():
								f.write(json.dumps(row, ensure_ascii=False))
								f.write("\n")
					self._log(f"📁 Exported JSONL to: {path}")
					self._status("Text file saved")
			except Exception as e:  # noqa: BLE001
				self._log(f"❌ EXPORT ERROR: {e}")
				self._status("Export failed")

		self._status("Exporting...")
		self.runner.run(task, on_done=done, on_error=lambda e: (self._log(f"❌ DOWNLOAD ERROR: {e}"), self._status("Download failed")))

	def _export_to_csv(self, reader, path: str):
		"""Export Arrow data to CSV with proper tabular format."""
		try:
			# First, try using Polars if available (more robust)
			if pl is not None:
				table = reader.read_all()
				pldf = pl.from_arrow(table)  # type: ignore[assignment]
				pldf.write_csv(path)  # type: ignore[attr-defined]
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

	def _on_get_latest_schema(self):
		"""Get latest schema for the current schema name"""
		def task():
			self._apply_settings()  # Apply any form changes first
			return self.api.get_latest_schema(self.schema_var.get())

		def done(schema: Dict[str, Any]):
			self._log("🔍 Latest Schema:\n" + _format_json(schema))
			self._status("Schema loaded successfully")

		def error(e: Exception):
			self._log(f"❌ LATEST SCHEMA ERROR: {e}")
			self._status("Schema load failed")

		self._status("Fetching latest schema...")
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
		"""Add text to the log with timestamp"""
		try:
			timestamp = datetime.now().strftime("%H:%M:%S")
			log_text = f"[{timestamp}] {text}\n"
			self.log.insert("end", log_text)
			if hasattr(self.log, "see"):
				self.log.see("end")
		except Exception:
			pass

	def _status(self, text: str):
		"""Update status - for now just log it since we removed the status bar"""
		try:
			# Since we removed the status bar, we can just log status updates
			# or we could add a status area to the sidebar if needed
			timestamp = datetime.now().strftime("%H:%M:%S")
			print(f"[{timestamp}] Status: {text}")  # For debugging
		except Exception:
			pass

	def _progress(self, value: float):
		"""Update progress bar if available"""
		try:
			if hasattr(self, 'progress') and hasattr(self.progress, "set"):
				self.progress.set(value)
		except Exception:
			pass


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
