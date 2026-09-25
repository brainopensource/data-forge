"""
Plot Explorer Component - Refactored with BaseComponent inheritance
"""
import tkinter as tk
from tkinter import messagebox

try:
    import customtkinter as ctk
    HAS_CTK = True
except ImportError:
    HAS_CTK = False

import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
try:
    from matplotlib.backends.backend_tkagg import NavigationToolbar2Tk
except ImportError:
    try:
        from matplotlib.backends._backend_tk import NavigationToolbar2Tk
    except ImportError:
        NavigationToolbar2Tk = None
from matplotlib.figure import Figure
import pandas as pd
from datetime import datetime

# Import BaseComponent
from .base_component import BaseComponent

# Configure matplotlib for dark theme
plt.style.use('dark_background')

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

class Colors:
	"""Standardized color palette for DataForge UI"""
	BACKGROUND = '#000000'
	SURFACE = '#1a1a1a'
	SURFACE_LIGHT = '#2b2b2b'
	PRIMARY = '#1f538d'
	PRIMARY_HOVER = '#14375e'
	TEXT_PRIMARY = '#ffffff'
	TEXT_SECONDARY = '#888888'
	SUCCESS = '#1f538d'
	WARNING = '#1f538d'
	ERROR = '#1f538d'
	GRAY_LIGHT = '#404040'
	ACCENT_GREEN = PRIMARY
	ACCENT_RED = PRIMARY

class PlotExplorer(BaseComponent):
	"""Advanced plotting component with matplotlib integration"""
	
	def __init__(self, parent, data=None, app_log=lambda x: print(x), on_back=None, component_id=None):
		super().__init__(parent, component_id)
		self.data = data or []
		self.columns = list(data[0].keys()) if data else []
		self.app_log = app_log
		self.on_back = on_back  # Callback for back button
		
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
		
		# Group plotting options
		self.group_aggregation_mode = "aggregated"  # "aggregated" or "individual"
		self.group_stacked = False
		
		# Current figure
		self.figure = None
		self.canvas = None
		self.toolbar = None
		
		# Initialize component using BaseComponent pattern
		self.initialize()
		
	def get_component_type(self) -> str:
		"""Get component type for identification"""
		return "plot_explorer"
		
	def build_ui(self):
		"""Build the component's UI - required by BaseComponent"""
		self._build_plot_explorer()
		# Register the main frame widget
		self.register_widget('main_frame', self.main_frame)
		
	def get_main_widget(self):
		"""Get the main widget for this component"""
		return getattr(self, 'main_frame', None)
	
	def _on_back_button(self):
		"""Handle back button click"""
		if self.on_back:
			self.on_back()
		else:
			# Default behavior: destroy the main frame to go back
			if hasattr(self, 'main_frame'):
				self.main_frame.destroy()
	
	def _build_plot_explorer(self):
		"""Build the complete plotting interface"""
		self.main_frame = ctk.CTkFrame(self.parent)
		self.main_frame.pack(fill="both", expand=True)
		
		self._build_control_panel()
		self._build_plot_area()
		self._build_info_panel()
	
	def _build_control_panel(self):
		"""Build the plot configuration control panel"""
		control_frame = ctk.CTkFrame(self.main_frame)
		control_frame.pack(fill="x", padx=10, pady=(10, 5))
		
		title_frame = ctk.CTkFrame(control_frame)
		title_frame.pack(fill="x", padx=10, pady=(10, 5))
		
		# Add back button to the left
		# Removed - now handled by sidebar back button
		
		ctk.CTkLabel(
			title_frame,
			text="📊 Plot Configuration",
			font=ctk.CTkFont(size=16, weight="bold") if HAS_CTK else ("Arial", 14, "bold")
		).pack(side="left", padx=10, pady=8)
		
		mode_frame = ctk.CTkFrame(title_frame)
		mode_frame.pack(side="right", padx=10, pady=5)
		
		ctk.CTkLabel(mode_frame, text="Mode:", font=ctk.CTkFont(size=12, weight="bold") if HAS_CTK else ("Arial", 10, "bold")).pack(side="left", padx=(5, 10))
		
		self.individual_btn = ctk.CTkButton(
			mode_frame,
			text="👤 Individual Plots",
			command=lambda: self._switch_mode('individual'),
			width=140,
			height=30,
			hover_color="#8A2BE2"
		)
		self.individual_btn.pack(side="left", padx=2)
		
		self.group_btn = ctk.CTkButton(
			mode_frame,
			text="👥 Group Plots",
			command=lambda: self._switch_mode('group'),
			width=140,
			height=30,
			hover_color="#8A2BE2"
		)
		self.group_btn.pack(side="left", padx=2)
		
		self._update_mode_buttons()
		
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
			self.individual_btn.configure(fg_color=Colors.PRIMARY)
			self.group_btn.configure(fg_color="transparent")
		else:
			self.individual_btn.configure(fg_color="transparent")
			self.group_btn.configure(fg_color=Colors.PRIMARY)
	
	def _build_mode_specific_config(self):
		"""Build configuration UI based on current mode"""
		for widget in self.config_container.winfo_children():
			widget.destroy()
		
		if self.plot_mode == 'individual':
			self._build_individual_config()
		else:
			self._build_group_config()
	
	def _build_individual_config(self):
		"""Build configuration for individual plots with filtering"""
		filter_frame = ctk.CTkFrame(self.config_container)
		filter_frame.pack(fill="x", padx=10, pady=(10, 5))
		
		ctk.CTkLabel(
			filter_frame,
			text="🔍 Data Filter (Optional)",
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		).pack(anchor="w", padx=10, pady=(8, 5))
		
		filter_controls = ctk.CTkFrame(filter_frame)
		filter_controls.pack(fill="x", padx=10, pady=(0, 8))
		
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
		
		filter_val_frame = ctk.CTkFrame(filter_controls)
		filter_val_frame.pack(side="left", padx=(0, 10))
		
		ctk.CTkLabel(filter_val_frame, text="Filter Values:", font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8)).pack(anchor="w", padx=5, pady=(2, 0))
		
		self.filter_values = []
		
		self.filter_select_btn = ctk.CTkButton(
			filter_val_frame,
			text="📋 Select Values",
			command=self._open_filter_selector,
			width=120,
			height=25
		)
		self.filter_select_btn.pack(padx=5, pady=(0, 5))
		
		self.filter_selection_label = ctk.CTkLabel(
			filter_val_frame,
			text="No filters selected",
			font=ctk.CTkFont(size=9) if HAS_CTK else ("Arial", 7),
			text_color="#888888"
		)
		self.filter_selection_label.pack(padx=5, pady=(0, 2))
		
		apply_filter_btn = ctk.CTkButton(
			filter_controls,
			text="🔍 Apply Filter",
			command=self._apply_individual_filter,
			width=100,
			height=30
		)
		apply_filter_btn.pack(side="left", padx=(10, 0), pady=12)
		
		self.filter_status_label = ctk.CTkLabel(
			filter_controls,
			text=f"📊 {len(self.data):,} records available",
			font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8),
			text_color="#888888"
		)
		self.filter_status_label.pack(side="left", padx=(15, 0), pady=12)
		
		self._build_column_selection()
	
	def _build_group_config(self):
		"""Build configuration for group plots with aggregation"""
		group_frame = ctk.CTkFrame(self.config_container)
		group_frame.pack(fill="x", padx=10, pady=(10, 5))
		
		ctk.CTkLabel(
			group_frame,
			text="👥 Group Configuration",
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		).pack(anchor="w", padx=10, pady=(8, 5))
		
		group_controls = ctk.CTkFrame(group_frame)
		group_controls.pack(fill="x", padx=10, pady=(0, 8))
		
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
		
		self.group_status_label = ctk.CTkLabel(
			group_controls,
			text="📊 Select grouping column",
			font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8),
			text_color="#888888"
		)
		self.group_status_label.pack(side="left", padx=(15, 0), pady=12)
		
		apply_groups_btn = ctk.CTkButton(
			group_controls,
			text="✓ Apply Groups",
			command=self._apply_group_settings,
			width=100,
			height=25,
			fg_color=Colors.PRIMARY,
			hover_color=Colors.PRIMARY_HOVER
		)
		apply_groups_btn.pack(side="right", padx=10, pady=12)
		
		# Add group plotting options
		options_frame = ctk.CTkFrame(self.config_container)
		options_frame.pack(fill="x", padx=10, pady=5)
		
		ctk.CTkLabel(
			options_frame,
			text="📊 Group Plot Options",
			font=ctk.CTkFont(size=14, weight="bold") if HAS_CTK else ("Arial", 12, "bold")
		).pack(anchor="w", padx=10, pady=(8, 5))
		
		options_controls = ctk.CTkFrame(options_frame)
		options_controls.pack(fill="x", padx=10, pady=(0, 8))
		
		# Aggregation mode selection
		mode_frame = ctk.CTkFrame(options_controls)
		mode_frame.pack(side="left", padx=(0, 20))
		
		ctk.CTkLabel(mode_frame, text="Plot Mode:", font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8)).pack(anchor="w", padx=5, pady=(2, 0))
		
		self.agg_mode_var = self._get_string_var("aggregated")
		self.agg_mode_menu = ctk.CTkOptionMenu(
			mode_frame,
			values=["aggregated", "individual"],
			variable=self.agg_mode_var,
			command=self._on_agg_mode_change,
			width=120
		)
		self.agg_mode_menu.pack(padx=5, pady=(0, 5))
		
		# Stacked option
		stacked_frame = ctk.CTkFrame(options_controls)
		stacked_frame.pack(side="left", padx=(0, 20))
		
		ctk.CTkLabel(stacked_frame, text="Stacked:", font=ctk.CTkFont(size=10) if HAS_CTK else ("Arial", 8)).pack(anchor="w", padx=5, pady=(2, 0))
		
		self.stacked_var = self._get_string_var("False")
		self.stacked_menu = ctk.CTkOptionMenu(
			stacked_frame,
			values=["True", "False"],
			variable=self.stacked_var,
			command=self._on_stacked_change,
			width=80
		)
		self.stacked_menu.pack(padx=5, pady=(0, 5))

		self._build_column_selection()
	
	def _apply_group_settings(self):
		"""Apply the current group settings"""
		if not hasattr(self, 'group_values') or not self.group_values:
			messagebox.showwarning("No Groups Selected", "Please select at least one group to plot.")
			return
		
		if not self.group_column:
			messagebox.showwarning("No Group Column", "Please select a column to group by.")
			return
		
		self._update_info(f"✅ Group settings applied: {len(self.group_values)} groups selected")
		self._log(f"📊 Group plotting configured: {self.group_column} with {len(self.group_values)} groups")
	
	def _build_column_selection(self):
		"""Build the common column selection interface"""
		columns_frame = ctk.CTkFrame(self.config_container)
		columns_frame.pack(fill="x", padx=10, pady=5)
		
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

		plot_type_group = ctk.CTkFrame(columns_frame)
		plot_type_group.pack(side="left", padx=5, pady=8)

		ctk.CTkLabel(plot_type_group, text="Plot Type:", font=ctk.CTkFont(size=12, weight="bold") if HAS_CTK else ("Arial", 10, "bold")).pack(anchor="w", padx=5, pady=(5, 2))
		self.plot_type_var = self._get_string_var(PlotType.SCATTER)
		self.plot_type_menu = ctk.CTkOptionMenu(
			plot_type_group,
			values=[PlotType.SCATTER, PlotType.LINE, PlotType.BAR, PlotType.HISTOGRAM, PlotType.BOX, PlotType.HEATMAP, PlotType.CORRELATION],
			variable=self.plot_type_var,
			command=self._on_plot_type_change,
			width=150
		)
		self.plot_type_menu.pack(padx=5, pady=(0, 5))

		generate_btn = ctk.CTkButton(
			columns_frame,
			text="📈 Generate Plot",
			command=self._generate_plot,
			width=150,
			height=40,
			hover_color="#8A2BE2"
		)
		generate_btn.pack(side="right", padx=10, pady=20)

	def _build_plot_area(self):
		"""Build the area where the plot will be displayed"""
		self.plot_frame = ctk.CTkFrame(self.main_frame, fg_color=Colors.SURFACE)
		self.plot_frame.pack(fill="both", expand=True, padx=10, pady=5)
		self._create_empty_plot()

	def _build_info_panel(self):
		"""Build the bottom info panel"""
		self.info_frame = ctk.CTkFrame(self.main_frame, height=30)
		self.info_frame.pack(fill="x", padx=10, pady=(5, 10))
		self.info_label = ctk.CTkLabel(self.info_frame, text="ℹ️ Welcome to the Plot Explorer. Select data and options to generate a plot.", anchor="w")
		self.info_label.pack(fill="x", padx=10, pady=5)

	def _create_empty_plot(self):
		"""Create a placeholder plot"""
		self._clear_plot()
		self.figure = Figure(figsize=(8, 6), dpi=100, facecolor=Colors.SURFACE)
		ax = self.figure.add_subplot(111)
		ax.set_facecolor(Colors.SURFACE_LIGHT)
		ax.text(0.5, 0.5, "Select data and click 'Generate Plot'",
				horizontalalignment='center', verticalalignment='center',
				fontsize=14, color=Colors.TEXT_SECONDARY)
		ax.tick_params(axis='x', colors=Colors.TEXT_SECONDARY)
		ax.tick_params(axis='y', colors=Colors.TEXT_SECONDARY)
		self.canvas = FigureCanvasTkAgg(self.figure, self.plot_frame)
		self.canvas.draw()
		self.canvas.get_tk_widget().pack(fill="both", expand=True)

	def _generate_plot(self):
		"""Generate and display the plot based on current settings"""
		x_col = self._get_var_value(self.x_column_var)
		y_col = self._get_var_value(self.y_column_var)
		plot_type = self._get_var_value(self.plot_type_var)

		if not x_col or not y_col:
			self._update_info("❌ Error: X-axis and Y-axis columns must be selected.")
			return

		self._clear_plot()
		self.figure = Figure(figsize=(8, 6), dpi=100, facecolor=Colors.SURFACE)
		ax = self.figure.add_subplot(111)
		ax.set_facecolor(Colors.SURFACE_LIGHT)

		try:
			if self.plot_mode == 'individual':
				plot_data = self.filtered_data
				self._create_individual_plot(ax, plot_data, x_col, y_col, plot_type)
			else: # group mode
				plot_data = self._prepare_group_data(x_col, y_col)
				self._create_group_plot(ax, plot_data, x_col, y_col, plot_type)

			# Only add legend if we have multiple series
			if self.plot_mode == 'group' or (self.plot_mode == 'individual' and len(self.filtered_data) > 1):
				ax.legend()
			
			ax.grid(True, which='both', linestyle='--', linewidth=0.5, color=Colors.GRAY_LIGHT)
			self.canvas = FigureCanvasTkAgg(self.figure, self.plot_frame)
			self.canvas.draw()
			self.canvas.get_tk_widget().pack(fill="both", expand=True)

			if self.toolbar:
				self.toolbar.destroy()
			if NavigationToolbar2Tk:
				self.toolbar = NavigationToolbar2Tk(self.canvas, self.plot_frame)
				self.toolbar.update()
			else:
				self.toolbar = None
			self._update_info(f"✅ Successfully generated {plot_type} plot.")

		except Exception as e:
			self._update_info(f"❌ Error generating plot: {e}")
			self._log(f"Plotting error: {e}")
			self._create_empty_plot()

	def _prepare_group_data(self, x_col, y_col):
		"""Prepare data for group plotting with proper aggregation"""
		if not self.group_column or not self.group_values:
			raise ValueError("Group column and values must be set for group plots.")
		
		df = pd.DataFrame(self.data)
		df = df[df[self.group_column].isin(self.group_values)]
		
		agg_func = self.aggregate_function
		
		# Handle datetime X-axis properly
		x_type = self._get_var_value(self.x_type_var)
		if x_type == DataType.AUTO:
			x_type = self._detect_data_type(df[x_col])
		
		if self.group_aggregation_mode == "aggregated":
			if x_type == DataType.DATETIME:
				# For datetime X, group by both group column and X column (time periods)
				df[x_col] = pd.to_datetime(df[x_col], errors='coerce')
				df.dropna(subset=[x_col], inplace=True)
				
				# Group by both group and time period, then aggregate Y
				grouped = df.groupby([self.group_column, x_col]).agg({
					y_col: agg_func
				}).reset_index()
				
				# Sort by time for proper plotting
				grouped = grouped.sort_values(x_col)
			else:
				# For non-datetime X, group only by group column
				grouped = df.groupby(self.group_column).agg({
					x_col: agg_func if x_col != self.group_column else 'first',
					y_col: agg_func
				}).reset_index()
		else:  # individual mode
			# Don't aggregate, just filter by groups
			grouped = df.copy()
		
		return grouped.to_dict('records')

	def _create_individual_plot(self, ax, plot_data, x_col, y_col, plot_type):
		"""Create a plot for individual data points"""
		df = pd.DataFrame(plot_data)
		if df.empty:
			raise ValueError("No data available for plotting.")

		x_data, y_data = self._clean_data(df[x_col], df[y_col], x_col, y_col)
		
		# Use the updated _create_plot method
		self._create_plot(ax, x_data, y_data, x_col, y_col, plot_type, label_suffix="(individual)")

	def _create_group_plot(self, ax, plot_data, x_col, y_col, plot_type):
		"""Create a plot for group data with proper handling of datetime and aggregation modes"""
		df = pd.DataFrame(plot_data)
		if df.empty:
			raise ValueError("No data available for group plotting.")
		
		if self.group_aggregation_mode == "aggregated":
			self._create_aggregated_group_plot(ax, df, x_col, y_col, plot_type)
		else:  # individual mode
			self._create_individual_group_plot(ax, df, x_col, y_col, plot_type)
	
	def _create_aggregated_group_plot(self, ax, df, x_col, y_col, plot_type):
		"""Create aggregated group plot"""
		unique_groups = df[self.group_column].unique()
		
		# Check if X is datetime
		x_type = self._get_var_value(self.x_type_var)
		if x_type == DataType.AUTO:
			x_type = self._detect_data_type(df[x_col])
		
		if x_type == DataType.DATETIME and x_col != self.group_column:
			# Time series plot: each group is a line/series over time
			for group in unique_groups:
				group_data = df[df[self.group_column] == group]
				if not group_data.empty:
					x_data = pd.to_datetime(group_data[x_col])
					y_data = group_data[y_col]
					
					if plot_type == PlotType.LINE:
						if self.group_stacked:
							# For stacked, we need cumulative data
							y_data = y_data.cumsum()
						label = f"{group} ({self.aggregate_function})"
						ax.plot(x_data, y_data, marker='o', linestyle='-', label=label)
					elif plot_type == PlotType.SCATTER:
						label = f"{group} ({self.aggregate_function})"
						ax.scatter(x_data, y_data, alpha=0.7, label=label)
					elif plot_type == PlotType.BAR:
						if self.group_stacked:
							bottom = 0
							for i, group in enumerate(unique_groups):
								group_data = df[df[self.group_column] == group]
								if not group_data.empty:
									x_data = pd.to_datetime(group_data[x_col])
									y_data = group_data[y_col]
									label = f"{group} ({self.aggregate_function})"
									ax.bar(x_data, y_data, bottom=bottom, label=label, alpha=0.7)
									if i == 0:  # Only add to bottom for first group
										bottom = y_data
						else:
							for group in unique_groups:
								group_data = df[df[self.group_column] == group]
								if not group_data.empty:
									x_data = pd.to_datetime(group_data[x_col])
									y_data = group_data[y_col]
									label = f"{group} ({self.aggregate_function})"
									ax.bar(x_data, y_data, alpha=0.7, label=label)
		else:
			# Categorical X-axis: traditional grouped bar chart
			x_positions = range(len(unique_groups))
			
			if plot_type == PlotType.BAR:
				if self.group_stacked:
					bottom = [0] * len(unique_groups)
					for i, group in enumerate(unique_groups):
						group_data = df[df[self.group_column] == group]
						if not group_data.empty:
							y_val = group_data[y_col].iloc[0]
							label = f"{group} ({self.aggregate_function})"
							ax.bar(x_positions[i], y_val, bottom=bottom[i], label=label, alpha=0.7)
							bottom[i] += y_val
				else:
					for i, group in enumerate(unique_groups):
						group_data = df[df[self.group_column] == group]
						if not group_data.empty:
							y_val = group_data[y_col].iloc[0]
							label = f"{group} ({self.aggregate_function})"
							ax.bar(x_positions[i], y_val, alpha=0.7, label=label)
			
			ax.set_xticks(x_positions)
			ax.set_xticklabels(unique_groups, rotation=45)
	
	def _create_individual_group_plot(self, ax, df, x_col, y_col, plot_type):
		"""Create individual group plot (no aggregation)"""
		unique_groups = df[self.group_column].unique()
		
		for group in unique_groups:
			group_data = df[df[self.group_column] == group]
			if not group_data.empty:
				x_data, y_data = self._clean_data(group_data[x_col], group_data[y_col], x_col, y_col)
				
				label = f"{group} (individual)"
				
				if plot_type == PlotType.SCATTER:
					ax.scatter(x_data, y_data, alpha=0.7, label=label)
				elif plot_type == PlotType.LINE:
					# Sort by x for line plots
					if len(x_data) > 1:
						sort_indices = x_data.argsort()
						x_data = x_data.iloc[sort_indices]
						y_data = y_data.iloc[sort_indices]
					ax.plot(x_data, y_data, marker='o', linestyle='-', label=label)
				elif plot_type == PlotType.BAR:
					if self.group_stacked:
						# For individual stacked, group by x values
						x_unique = sorted(x_data.unique())
						bottom = [0] * len(x_unique)
						
						for i, x_val in enumerate(x_unique):
							y_vals = y_data[x_data == x_val]
							if len(y_vals) > 0:
								y_sum = y_vals.sum()
								ax.bar(i, y_sum, bottom=bottom[i], label=f"{group} at {x_val}", alpha=0.7)
								bottom[i] += y_sum
					else:
						# Individual bars for each point
						for i, (x_val, y_val) in enumerate(zip(x_data, y_data)):
							ax.bar(i, y_val, alpha=0.7, label=f"{group} point {i+1}")
		
		# Set proper axis labels
		x_type = self._get_var_value(self.x_type_var)
		if x_type == DataType.AUTO:
			x_type = self._detect_data_type(df[x_col])
		
		if x_type == DataType.DATETIME:
			ax.set_xlabel(f"{x_col} (datetime)", color=Colors.TEXT_PRIMARY)
		else:
			ax.set_xlabel(x_col, color=Colors.TEXT_PRIMARY)
		
		y_type = self._get_var_value(self.y_type_var)
		if y_type == DataType.AUTO:
			y_type = self._detect_data_type(df[y_col])
		
		if y_type == DataType.DATETIME:
			ax.set_ylabel(f"{y_col} (datetime)", color=Colors.TEXT_PRIMARY)
		else:
			ax.set_ylabel(y_col, color=Colors.TEXT_PRIMARY)
		
		title_suffix = "individual" if self.group_aggregation_mode == "individual" else f"{self.aggregate_function}"
		if self.group_stacked:
			title_suffix += " (stacked)"
		
		ax.set_title(f"{plot_type.title()} Plot: {y_col} vs {x_col} by {self.group_column} ({title_suffix})", color=Colors.TEXT_PRIMARY)

	def _on_filter_column_change(self, value):
		"""Handle filter column change"""
		self.filter_column = value if value != "None" else None
		self.filter_values = []
		self._update_filter_status()
		self._update_info(f"Filter column set to: {value}")

	def _open_filter_selector(self):
		"""Open a dialog to select filter values"""
		if not self.filter_column:
			messagebox.showwarning("No Column Selected", "Please select a filter column first.")
			return
		
		try:
			unique_values = sorted(list(set(d[self.filter_column] for d in self.data if self.filter_column in d)))
			self._create_filter_selection_dialog(unique_values)
		except Exception as e:
			messagebox.showerror("Error", f"Could not get unique values for {self.filter_column}: {e}")

	def _create_filter_selection_dialog(self, available_values):
		"""Create the filter value selection dialog"""
		dialog = ctk.CTkToplevel(self.parent)
		dialog.title(f"Select values for {self.filter_column}")
		dialog.geometry("400x500")

		listbox = tk.Listbox(dialog, selectmode=tk.MULTIPLE)
		for val in available_values:
			listbox.insert(tk.END, val)
		listbox.pack(fill="both", expand=True, padx=10, pady=10)

		def on_ok():
			selected_indices = listbox.curselection()
			self.filter_values = [available_values[i] for i in selected_indices]
			self._update_filter_status()
			dialog.destroy()

		ok_button = ctk.CTkButton(dialog, text="OK", command=on_ok)
		ok_button.pack(pady=10)

	def _update_filter_status(self):
		"""Update the label showing filter status"""
		if not self.filter_values:
			self.filter_selection_label.configure(text="No filters selected")
		else:
			self.filter_selection_label.configure(text=f"{len(self.filter_values)} values selected")

	def _apply_individual_filter(self):
		"""Apply the selected filter to the data"""
		if not self.filter_column or not self.filter_values:
			self.filtered_data = self.data.copy()
			self._update_info("ℹ️ Filter cleared or not fully specified. Showing all data.")
		else:
			self.filtered_data = [
				d for d in self.data 
				if self.filter_column in d and d[self.filter_column] in self.filter_values
			]
			self._update_info(f"🔍 Filter applied. {len(self.filtered_data)} records remaining.")
		
		self.filter_status_label.configure(text=f"📊 {len(self.filtered_data):,} records available")

	def _on_group_column_change(self, value):
		"""Handle group column change"""
		self.group_column = value
		self.group_values = []
		self._update_group_status()
		self._update_info(f"Group column set to: {value}")

	def _on_aggregate_change(self, value):
		"""Handle aggregation function change"""
		self.aggregate_function = value
		self._update_info(f"Aggregation function set to: {value}")

	def _on_agg_mode_change(self, value):
		"""Handle aggregation mode change"""
		self.group_aggregation_mode = value
		self._update_info(f"Group plot mode set to: {value}")
	
	def _on_stacked_change(self, value):
		"""Handle stacked option change"""
		self.group_stacked = value == "True"
		self._update_info(f"Stacked plotting: {value}")

	def _open_group_selector(self):
		"""Open a dialog to select groups"""
		if not self.group_column:
			messagebox.showwarning("No Group Column", "Please select a column to group by first.")
			return
		
		try:
			unique_groups = sorted(list(set(d[self.group_column] for d in self.data if self.group_column in d)))
			self._create_group_selection_dialog(unique_groups)
		except Exception as e:
			messagebox.showerror("Error", f"Could not get unique groups for {self.group_column}: {e}")

	def _create_group_selection_dialog(self, available_groups):
		"""Create the group selection dialog"""
		dialog = ctk.CTkToplevel(self.parent)
		dialog.title(f"Select groups from {self.group_column}")
		dialog.geometry("400x500")

		listbox = tk.Listbox(dialog, selectmode=tk.MULTIPLE)
		for group in available_groups:
			listbox.insert(tk.END, group)
		listbox.pack(fill="both", expand=True, padx=10, pady=10)

		def on_ok():
			selected_indices = listbox.curselection()
			self.group_values = [available_groups[i] for i in selected_indices]
			self._update_group_status()
			dialog.destroy()

		ok_button = ctk.CTkButton(dialog, text="OK", command=on_ok)
		ok_button.pack(pady=10)

	def _update_group_status(self):
		"""Update the label showing group status"""
		if not self.group_values:
			self.group_status_label.configure(text="No groups selected")
		else:
			self.group_status_label.configure(text=f"{len(self.group_values)} groups selected")

	def _clean_data(self, x_data, y_data, x_col, y_col):
		"""Clean and prepare data for plotting"""
		df = pd.DataFrame({x_col: x_data, y_col: y_data})
		df.dropna(inplace=True)
		
		x_type = self._get_var_value(self.x_type_var)
		y_type = self._get_var_value(self.y_type_var)

		if x_type == DataType.AUTO: x_type = self._detect_data_type(df[x_col])
		if y_type == DataType.AUTO: y_type = self._detect_data_type(df[y_col])

		if x_type == DataType.DATETIME:
			df[x_col] = pd.to_datetime(df[x_col], errors='coerce')
		elif x_type == DataType.NUMERIC:
			df[x_col] = pd.to_numeric(df[x_col], errors='coerce')

		if y_type == DataType.DATETIME:
			df[y_col] = pd.to_datetime(df[y_col], errors='coerce')
		elif y_type == DataType.NUMERIC:
			df[y_col] = pd.to_numeric(df[y_col], errors='coerce')
			
		df.dropna(inplace=True)
		return df[x_col], df[y_col]

	def _detect_data_type(self, data_list):
		"""Automatically detect data type from a list of values"""
		if all(isinstance(x, (int, float)) for x in data_list):
			return DataType.NUMERIC
		if all(isinstance(x, str) and self._is_datetime_string(x) for x in data_list):
			return DataType.DATETIME
		if len(set(data_list)) / len(data_list) < 0.5:
			return DataType.CATEGORICAL
		return DataType.STRING

	def _is_datetime_string(self, s):
		"""Check if a string can be parsed as a datetime"""
		try:
			self._parse_datetime(s)
			return True
		except (ValueError, TypeError):
			return False

	def _parse_datetime(self, val):
		"""Parse a datetime string"""
		if isinstance(val, (datetime, pd.Timestamp)):
			return val
		return datetime.fromisoformat(str(val).replace('Z', '+00:00'))

	def _create_plot(self, ax, x_data, y_data, x_col, y_col, plot_type, label_suffix=""):
		"""Main plotting function"""
		label = f"{y_col} vs {x_col} {label_suffix}".strip()
		
		if plot_type == PlotType.SCATTER:
			ax.scatter(x_data, y_data, alpha=0.7, label=label)
		elif plot_type == PlotType.LINE:
			# Sort data for line plots if x is not datetime
			if not isinstance(x_data.iloc[0] if hasattr(x_data, 'iloc') else x_data[0], pd.Timestamp):
				try:
					sort_indices = x_data.argsort()
					x_data = x_data.iloc[sort_indices]
					y_data = y_data.iloc[sort_indices]
				except:
					pass
			ax.plot(x_data, y_data, marker='o', linestyle='-', label=label)
		elif plot_type == PlotType.BAR:
			if hasattr(x_data, 'index'):
				x_positions = range(len(x_data))
				ax.bar(x_positions, y_data, label=label)
				# Set x-tick labels
				if len(x_data) <= 20:  # Only show labels if not too many
					try:
						labels = [str(x) for x in x_data]
						ax.set_xticks(x_positions)
						ax.set_xticklabels(labels, rotation=45)
					except:
						pass
			else:
				x_positions = range(len(x_data))
				ax.bar(x_positions, y_data, label=label)
		elif plot_type == PlotType.HISTOGRAM:
			ax.hist(y_data, bins=30, alpha=0.7, label=f"Distribution of {y_col}")
		elif plot_type == PlotType.BOX:
			if hasattr(y_data, 'values'):
				y_vals = y_data.values
			else:
				y_vals = y_data
			ax.boxplot(y_vals, vert=False)
			ax.set_yticklabels([y_col])
		elif plot_type == PlotType.HEATMAP:
			messagebox.showinfo("Not Implemented", "Heatmap requires specific data format.")
		elif plot_type == PlotType.CORRELATION:
			messagebox.showinfo("Not Implemented", "Correlation plot requires multiple numeric columns.")

		# Set axis labels and title
		x_type = self._get_var_value(self.x_type_var)
		if x_type == DataType.AUTO:
			x_type = self._detect_data_type(x_data)
		
		if x_type == DataType.DATETIME:
			ax.set_xlabel(f"{x_col} (datetime)", color=Colors.TEXT_PRIMARY)
		else:
			ax.set_xlabel(x_col, color=Colors.TEXT_PRIMARY)
		
		y_type = self._get_var_value(self.y_type_var)
		if y_type == DataType.AUTO:
			y_type = self._detect_data_type(y_data)
		
		if y_type == DataType.DATETIME:
			ax.set_ylabel(f"{y_col} (datetime)", color=Colors.TEXT_PRIMARY)
		else:
			ax.set_ylabel(y_col, color=Colors.TEXT_PRIMARY)
		
		if not ax.get_title():
			title_suffix = label_suffix.replace("(", "").replace(")", "")
			if title_suffix:
				title_suffix = f" ({title_suffix})"
			ax.set_title(f"{plot_type.title()} Plot: {y_col} vs {x_col}{title_suffix}", color=Colors.TEXT_PRIMARY)
		
		ax.tick_params(axis='x', colors=Colors.TEXT_SECONDARY, rotation=45)
		ax.tick_params(axis='y', colors=Colors.TEXT_SECONDARY)

	def _clear_plot(self):
		"""Clear the plot area"""
		for widget in self.plot_frame.winfo_children():
			widget.destroy()
		self.figure = None
		self.canvas = None
		self.toolbar = None

	def _update_info(self, message):
		"""Update the info label"""
		self.info_label.configure(text=message)
		self._log(message)

	def _on_x_column_change(self, value):
		self.x_column = value
	
	def _on_y_column_change(self, value):
		self.y_column = value
	
	def _on_x_type_change(self, value):
		self.x_data_type = value
	
	def _on_y_type_change(self, value):
		self.y_data_type = value
	
	def _on_plot_type_change(self, value):
		self.plot_type = value
	
	def _get_var_value(self, var) -> str:
		return var.get()
	
	def _get_string_var(self, value: str):
		if HAS_CTK:
			return tk.StringVar(value=value)
		return tk.StringVar(value=value)
	
	def update_data(self, new_data):
		"""Update the data and refresh the UI"""
		self.data = new_data or []
		self.filtered_data = self.data.copy()
		self.columns = list(self.data[0].keys()) if self.data else []
		
		# Re-build config to update column lists
		self._build_mode_specific_config()
		self.filter_status_label.configure(text=f"📊 {len(self.data):,} records available")
		self._update_info(f"✅ New data loaded with {len(self.data)} records.")

	def _log(self, message: str):
		"""Log a message to the main app's log"""
		if self.app_log:
			self.app_log(f"[PlotExplorer] {message}")

if __name__ == '__main__':
    # Example usage
    root = ctk.CTk()
    root.title("Plot Explorer Demo")
    root.geometry("1000x800")

    sample_data = [
        {'id': i, 'category': f'Cat{i%3}', 'value': i*1.5, 'timestamp': f'2023-01-01T{i:02d}:00:00Z'}
        for i in range(100)
    ]

    plot_explorer = PlotExplorer(root, data=sample_data)
    root.mainloop()
