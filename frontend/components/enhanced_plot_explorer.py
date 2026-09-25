"""
Enhanced Plot Explorer Component with Matplotlib Integration
"""
import tkinter as tk
from typing import Any, Dict, List, Optional
from enum import Enum
from dataclasses import dataclass

try:
    import customtkinter as ctk
    HAS_CTK = True
except ImportError:
    HAS_CTK = False
    ctk = None

# Import plotting libraries
import matplotlib
matplotlib.use('TkAgg')  # Use tkinter backend
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.backends._backend_tk import NavigationToolbar2Tk
from matplotlib.figure import Figure

# Configure matplotlib for dark theme
plt.style.use('dark_background')

from frontend.services.ui_framework_adapter import UIFrameworkAdapter
from .base_component import BaseComponent


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


class PlotExplorer(BaseComponent):
    """Advanced plotting component with matplotlib integration"""
    
    def __init__(self, parent, data=None, component_id=None):
        super().__init__(parent, component_id)
        self.data = data or []
        self.columns = list(data[0].keys()) if data else []
        
        # Initialize UI adapter
        self.ui_adapter = UIFrameworkAdapter()
        
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
        self.main_frame = self.ui_adapter.create_frame(self.parent)
        self.main_frame.pack(fill="both", expand=True)
        
        # Top control panel
        self._build_control_panel()
        
        # Plot area
        self._build_plot_area()
        
        # Bottom info panel
        self._build_info_panel()
    
    def _build_control_panel(self):
        """Build the plot configuration control panel"""
        control_frame = self.ui_adapter.create_frame(self.main_frame)
        control_frame.pack(fill="x", padx=10, pady=(10, 5))
        
        # Title and mode selection
        title_frame = self.ui_adapter.create_frame(control_frame)
        title_frame.pack(fill="x", padx=10, pady=(10, 5))
        
        self.ui_adapter.create_label(
            title_frame,
            text="📊 Plot Configuration",
            font_size=16,
            font_weight="bold"
        ).pack(side="left", padx=10, pady=8)
        
        # Mode selection buttons
        mode_frame = self.ui_adapter.create_frame(title_frame)
        mode_frame.pack(side="right", padx=10, pady=5)
        
        self.ui_adapter.create_label(
            mode_frame,
            text="Mode:",
            font_size=12,
            font_weight="bold"
        ).pack(side="left", padx=(5, 10))
        
        self.individual_btn = self.ui_adapter.create_button(
            mode_frame,
            text="👤 Individual Plots",
            command=lambda: self._switch_mode('individual'),
            width=140,
            height=30
        )
        self.individual_btn.pack(side="left", padx=2)
        
        self.group_btn = self.ui_adapter.create_button(
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
        self.config_container = self.ui_adapter.create_frame(control_frame)
        self.config_container.pack(fill="x", padx=10, pady=5)
        
        self._build_mode_specific_config()
    
    def _switch_mode(self, mode):
        """Switch between individual and group plot modes"""
        self.plot_mode = mode
        self._update_mode_buttons()
        
        # Clear and rebuild configuration
        for widget in self.config_container.winfo_children():
            widget.destroy()
        self._build_mode_specific_config()
    
    def _update_mode_buttons(self):
        """Update button appearance based on current mode"""
        # For now, just update text to indicate active mode
        if self.plot_mode == 'individual':
            self.individual_btn.configure(text="👤 Individual Plots ✓")
            self.group_btn.configure(text="👥 Group Plots")
        else:
            self.individual_btn.configure(text="👤 Individual Plots")
            self.group_btn.configure(text="👥 Group Plots ✓")
    
    def _build_mode_specific_config(self):
        """Build configuration specific to the current mode"""
        if self.plot_mode == 'individual':
            self._build_individual_config()
        else:
            self._build_group_config()
    
    def _build_individual_config(self):
        """Build individual plot configuration"""
        # Column selection
        self._build_column_selection()
        
        # Filter controls
        filter_frame = self.ui_adapter.create_frame(self.config_container)
        filter_frame.pack(fill="x", padx=10, pady=5)
        
        self.ui_adapter.create_label(
            filter_frame,
            text="Filter:",
            font_weight="bold"
        ).pack(side="left", padx=(0, 10))
        
        # Filter column selection
        if self.columns:
            self.filter_column_var = tk.StringVar(value="None")
            filter_col_menu = self.ui_adapter.create_combobox(
                filter_frame,
                values=["None"] + self.columns,
                variable=self.filter_column_var,
                command=self._on_filter_column_change,
                width=120
            )
            filter_col_menu.pack(side="left", padx=5)
            
            # Filter value selection button
            self.filter_btn = self.ui_adapter.create_button(
                filter_frame,
                text="Select Values",
                command=self._open_filter_selector,
                width=100
            )
            self.filter_btn.pack(side="left", padx=5)
            
            # Filter status
            self.filter_status = self.ui_adapter.create_label(
                filter_frame,
                text="No filter applied"
            )
            self.filter_status.pack(side="left", padx=10)
    
    def _build_group_config(self):
        """Build group plot configuration"""
        # Column selection
        self._build_column_selection()
        
        # Group controls
        group_frame = self.ui_adapter.create_frame(self.config_container)
        group_frame.pack(fill="x", padx=10, pady=5)
        
        self.ui_adapter.create_label(
            group_frame,
            text="Group by:",
            font_weight="bold"
        ).pack(side="left", padx=(0, 10))
        
        if self.columns:
            self.group_column_var = tk.StringVar(value="None")
            group_col_menu = self.ui_adapter.create_combobox(
                group_frame,
                values=["None"] + self.columns,
                variable=self.group_column_var,
                command=self._on_group_column_change,
                width=120
            )
            group_col_menu.pack(side="left", padx=5)
            
            # Aggregate function
            self.ui_adapter.create_label(
                group_frame,
                text="Aggregate:",
                font_weight="bold"
            ).pack(side="left", padx=(20, 10))
            
            self.aggregate_var = tk.StringVar(value="mean")
            agg_menu = self.ui_adapter.create_combobox(
                group_frame,
                values=["mean", "sum", "count", "min", "max"],
                variable=self.aggregate_var,
                command=self._on_aggregate_change,
                width=80
            )
            agg_menu.pack(side="left", padx=5)
            
            # Group selection button
            self.group_btn = self.ui_adapter.create_button(
                group_frame,
                text="Select Groups",
                command=self._open_group_selector,
                width=100
            )
            self.group_btn.pack(side="left", padx=5)
            
            # Group status
            self.group_status = self.ui_adapter.create_label(
                group_frame,
                text="No groups selected"
            )
            self.group_status.pack(side="left", padx=10)
    
    def _build_column_selection(self):
        """Build column selection interface"""
        col_frame = self.ui_adapter.create_frame(self.config_container)
        col_frame.pack(fill="x", padx=10, pady=5)
        
        # X-axis controls
        x_frame = self.ui_adapter.create_frame(col_frame)
        x_frame.pack(side="left", padx=(0, 20))
        
        self.ui_adapter.create_label(
            x_frame,
            text="X-axis:",
            font_weight="bold"
        ).pack(side="top", anchor="w")
        
        if self.columns:
            self.x_column_var = tk.StringVar(value=self.columns[0] if self.columns else "")
            x_col_menu = self.ui_adapter.create_combobox(
                x_frame,
                values=self.columns,
                variable=self.x_column_var,
                command=self._on_x_column_change,
                width=120
            )
            x_col_menu.pack(side="top", pady=(5, 0))
            
            self.x_type_var = tk.StringVar(value=DataType.AUTO)
            x_type_menu = self.ui_adapter.create_combobox(
                x_frame,
                values=[DataType.AUTO, DataType.NUMERIC, DataType.CATEGORICAL, DataType.DATETIME],
                variable=self.x_type_var,
                command=self._on_x_type_change,
                width=120
            )
            x_type_menu.pack(side="top", pady=(2, 0))
        
        # Y-axis controls
        y_frame = self.ui_adapter.create_frame(col_frame)
        y_frame.pack(side="left", padx=(0, 20))
        
        self.ui_adapter.create_label(
            y_frame,
            text="Y-axis:",
            font_weight="bold"
        ).pack(side="top", anchor="w")
        
        if self.columns and len(self.columns) > 1:
            self.y_column_var = tk.StringVar(value=self.columns[1])
            y_col_menu = self.ui_adapter.create_combobox(
                y_frame,
                values=self.columns,
                variable=self.y_column_var,
                command=self._on_y_column_change,
                width=120
            )
            y_col_menu.pack(side="top", pady=(5, 0))
            
            self.y_type_var = tk.StringVar(value=DataType.AUTO)
            y_type_menu = self.ui_adapter.create_combobox(
                y_frame,
                values=[DataType.AUTO, DataType.NUMERIC, DataType.CATEGORICAL, DataType.DATETIME],
                variable=self.y_type_var,
                command=self._on_y_type_change,
                width=120
            )
            y_type_menu.pack(side="top", pady=(2, 0))
        
        # Plot type controls
        plot_frame = self.ui_adapter.create_frame(col_frame)
        plot_frame.pack(side="left", padx=(0, 20))
        
        self.ui_adapter.create_label(
            plot_frame,
            text="Plot Type:",
            font_weight="bold"
        ).pack(side="top", anchor="w")
        
        self.plot_type_var = tk.StringVar(value=PlotType.SCATTER)
        plot_type_menu = self.ui_adapter.create_combobox(
            plot_frame,
            values=[PlotType.SCATTER, PlotType.LINE, PlotType.BAR, PlotType.HISTOGRAM, PlotType.BOX],
            variable=self.plot_type_var,
            command=self._on_plot_type_change,
            width=120
        )
        plot_type_menu.pack(side="top", pady=(5, 0))
        
        # Generate button
        self.ui_adapter.create_button(
            plot_frame,
            text="📈 Generate Plot",
            command=self._generate_plot,
            width=120,
            height=35
        ).pack(side="top", pady=(10, 0))
    
    def _build_plot_area(self):
        """Build the matplotlib plot area"""
        plot_frame = self.ui_adapter.create_frame(self.main_frame)
        plot_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Create matplotlib figure
        self.figure = Figure(figsize=(10, 6), dpi=100, facecolor='#2b2b2b')
        self.canvas = FigureCanvasTkAgg(self.figure, plot_frame)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)
        
        # Add toolbar
        toolbar_frame = self.ui_adapter.create_frame(plot_frame)
        toolbar_frame.pack(fill="x", pady=(5, 0))
        self.toolbar = NavigationToolbar2Tk(self.canvas, toolbar_frame)
        
        # Initial empty plot
        self._create_empty_plot()
    
    def _build_info_panel(self):
        """Build the bottom information panel"""
        info_frame = self.ui_adapter.create_frame(self.main_frame)
        info_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        self.info_label = self.ui_adapter.create_label(
            info_frame,
            text="Select columns and click 'Generate Plot' to create visualizations",
            font_size=10
        )
        self.info_label.pack(side="left", padx=10, pady=5)
    
    def _create_empty_plot(self):
        """Create an empty plot with instructions"""
        if self.figure is not None:
            self.figure.clear()
            ax = self.figure.add_subplot(111)
            ax.text(0.5, 0.5, "Select data columns and click 'Generate Plot'\nto create visualizations",
                    ha='center', va='center', transform=ax.transAxes,
                    fontsize=14, color='white', alpha=0.7)
            ax.set_facecolor('#2b2b2b')
            if self.canvas is not None:
                self.canvas.draw()
    
    # Event handlers (placeholder implementations)
    def _generate_plot(self):
        """Generate the plot based on current configuration"""
        # Implementation would go here
        self._update_info("Plot generation not yet implemented")
    
    def _on_filter_column_change(self, value):
        """Handle filter column change"""
        pass
    
    def _open_filter_selector(self):
        """Open filter value selection dialog"""
        pass
    
    def _on_group_column_change(self, value):
        """Handle group column change"""
        pass
    
    def _on_aggregate_change(self, value):
        """Handle aggregate function change"""
        pass
    
    def _open_group_selector(self):
        """Open group selection dialog"""
        pass
    
    def _on_x_column_change(self, value):
        """Handle X column change"""
        pass
    
    def _on_y_column_change(self, value):
        """Handle Y column change"""
        pass
    
    def _on_x_type_change(self, value):
        """Handle X data type change"""
        pass
    
    def _on_y_type_change(self, value):
        """Handle Y data type change"""
        pass
    
    def _on_plot_type_change(self, value):
        """Handle plot type change"""
        pass
    
    def _update_info(self, message):
        """Update info panel message"""
        if hasattr(self, 'info_label'):
            self.info_label.configure(text=message)
    
    def update_data(self, new_data):
        """Update the data for plotting"""
        self.data = new_data or []
        self.columns = list(new_data[0].keys()) if new_data else []
        self.filtered_data = self.data.copy()
        
        # Rebuild interface if needed
        if hasattr(self, 'main_frame'):
            for widget in self.main_frame.winfo_children():
                widget.destroy()
            self._build_plot_explorer()
