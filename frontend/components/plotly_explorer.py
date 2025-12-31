"""
Advanced Plotly Explorer Component for Interactive Visualizations

This component implements Task 12: Advanced Visualization Engine with Plotly Integration
Features:
- Interactive plots with Plotly integration
- 10+ advanced plot types
- Statistical analysis visualizations
- Professional export capabilities
- Graceful fallback when Plotly not available
"""

from typing import List, Dict, Any, Optional, Tuple, Callable
import json
import tempfile
import webbrowser
from pathlib import Path
from dataclasses import dataclass
from enum import Enum

# UI Framework
try:
    import customtkinter as ctk
    HAS_CTK = True
except ImportError:
    HAS_CTK = False

# Always import tkinter for fallback
import tkinter as tk
import tkinter.ttk as ttk

# Plotly imports with graceful fallback
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    import plotly.offline as pyo
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

# Statistical analysis
try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

# Theme imports
from frontend.presentation.styles.theme import Theme
from frontend.utils.error_handler import ErrorHandler
from frontend.components.base_component import BaseComponent


class AdvancedPlotTypes(Enum):
    """Advanced plot types supported by Plotly."""
    SCATTER = "scatter"
    LINE = "line"
    BAR = "bar"
    HISTOGRAM = "histogram"
    BOX = "box"
    VIOLIN = "violin"
    DENSITY = "density"
    HEATMAP = "heatmap"
    TREEMAP = "treemap"
    SUNBURST = "sunburst"
    PARALLEL_COORDINATES = "parallel_coords"
    RADAR = "radar"
    BUBBLE = "bubble"
    CANDLESTICK = "candlestick"
    AREA = "area"
    CORRELATION_MATRIX = "correlation_matrix"


@dataclass
class PlotConfig:
    """Configuration for Plotly plots."""
    plot_type: str
    data_source: List[Dict]
    x_column: Optional[str] = None
    y_column: Optional[str] = None
    color_column: Optional[str] = None
    size_column: Optional[str] = None
    facet_column: Optional[str] = None
    title: str = ""
    theme: str = "plotly_dark"
    animation_column: Optional[str] = None
    custom_params: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.custom_params is None:
            self.custom_params = {}


class PlotlyExplorer(BaseComponent):
    """Advanced interactive plotting component with Plotly integration."""
    
    def __init__(self, parent, data=None, app_log: Optional[Callable[[str], None]] = None):
        super().__init__(parent)
        self.data = data or []
        self.columns = list(data[0].keys()) if data else []
        self.app_log = app_log or print
        self.error_handler = ErrorHandler()
        
        # Current configuration
        self.current_config = None
        self.plot_widget = None
        self.current_figure = None
        
        # Check Plotly availability
        if not HAS_PLOTLY:
            self._show_plotly_missing_message()
            return
            
        self._build_plotly_explorer()
    
    def get_component_type(self) -> str:
        """Get component type for identification."""
        return "plotly_explorer"
    
    def build_ui(self):
        """Build the component's UI - implemented for BaseComponent compatibility."""
        pass  # Already built in constructor
    
    def _create_frame(self, parent):
        """Create frame widget using UI framework adapter."""
        if HAS_CTK:
            return ctk.CTkFrame(parent)
        else:
            return tk.Frame(parent)
    
    def _create_label(self, parent, text="", font_size=12, font_weight="normal", 
                     text_color=None, justify="left", font_family=None):
        """Create label widget using UI framework adapter."""
        if HAS_CTK:
            label = ctk.CTkLabel(parent, text=text)
            if font_size != 12 or font_weight != "normal":
                label.configure(font=ctk.CTkFont(size=font_size, weight=font_weight))
            if text_color:
                label.configure(text_color=text_color)
            if justify != "left":
                label.configure(justify=justify)
            return label
        else:
            label = tk.Label(parent, text=text)
            if font_family:
                label.configure(font=(font_family, font_size, font_weight))
            else:
                label.configure(font=("Arial", font_size, font_weight))
            if text_color:
                label.configure(fg=text_color)
            if justify != "left":
                label.configure(justify=justify)
            return label
    
    def _create_button(self, parent, text="", command=None, style="default"):
        """Create button widget using UI framework adapter."""
        if HAS_CTK:
            return ctk.CTkButton(parent, text=text, command=command)
        else:
            return tk.Button(parent, text=text, command=command or (lambda: None))
    
    def _create_combobox(self, parent, values=None, variable=None, command=None, width=120):
        """Create combobox widget using UI framework adapter."""
        if HAS_CTK:
            combo = ctk.CTkComboBox(parent, values=values or [], command=command, width=width)
            if variable:
                combo.configure(variable=variable)
            return combo
        else:
            combo = ttk.Combobox(parent, values=values or [], width=width//8)
            if variable:
                combo.configure(textvariable=variable)
            if command:
                combo.bind('<<ComboboxSelected>>', lambda e: command(combo.get()))
            return combo
    
    def _create_entry(self, parent, textvariable=None, width=120):
        """Create entry widget using UI framework adapter."""
        if HAS_CTK:
            return ctk.CTkEntry(parent, textvariable=textvariable, width=width)
        else:
            return tk.Entry(parent, textvariable=textvariable or tk.StringVar(), width=width//8)
    
    def _create_string_var(self, value=""):
        """Create string variable using UI framework adapter."""
        return tk.StringVar(value=value)
    
    def _get_var_value(self, var) -> str:
        """Get value from variable."""
        try:
            return var.get() if var else ""
        except:
            return ""
    
    def _build_plotly_explorer(self):
        """Build the main Plotly explorer interface."""
        # Main container
        self.main_frame = self._create_frame(self.parent)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Title
        title_frame = self._create_frame(self.main_frame)
        title_frame.pack(fill="x", padx=5, pady=(5, 10))
        
        title_label = self._create_label(
            title_frame,
            text="🎨 Advanced Interactive Plots",
            font_size=18,
            font_weight="bold"
        )
        title_label.pack(side="left", padx=10, pady=5)
        
        # Control panel
        self._build_control_panel()
        
        # Plot area
        self._build_plot_area()
        
        # Info panel
        self._build_info_panel()
    
    def _build_control_panel(self):
        """Build the plot configuration control panel."""
        control_frame = self._create_frame(self.main_frame)
        control_frame.pack(fill="x", padx=5, pady=5)
        
        # Plot type selection
        plot_type_frame = self._create_frame(control_frame)
        plot_type_frame.pack(side="left", padx=5, pady=5)
        
        type_label = self._create_label(plot_type_frame, text="Plot Type:", font_weight="bold")
        type_label.pack(anchor="w", padx=5, pady=2)
        
        plot_types = [
            AdvancedPlotTypes.SCATTER.value,
            AdvancedPlotTypes.LINE.value,
            AdvancedPlotTypes.BAR.value,
            AdvancedPlotTypes.HISTOGRAM.value,
            AdvancedPlotTypes.BOX.value,
            AdvancedPlotTypes.VIOLIN.value,
            AdvancedPlotTypes.HEATMAP.value,
            AdvancedPlotTypes.TREEMAP.value,
            AdvancedPlotTypes.SUNBURST.value,
            AdvancedPlotTypes.PARALLEL_COORDINATES.value,
            AdvancedPlotTypes.BUBBLE.value,
            AdvancedPlotTypes.CORRELATION_MATRIX.value
        ]
        
        self.plot_type_var = self._create_string_var(AdvancedPlotTypes.SCATTER.value)
        self.plot_type_combo = self._create_combobox(
            plot_type_frame,
            values=plot_types,
            variable=self.plot_type_var,
            command=self._on_plot_type_change,
            width=150
        )
        self.plot_type_combo.pack(padx=5, pady=2)
        
        # Column selection
        self._build_column_selection(control_frame)
        
        # Advanced options
        self._build_advanced_options(control_frame)
        
        # Generate button
        generate_frame = self._create_frame(control_frame)
        generate_frame.pack(side="right", padx=5, pady=5)
        
        self.generate_btn = self._create_button(
            generate_frame,
            text="🎯 Generate Plot",
            command=self._generate_plot,
            style="primary"
        )
        self.generate_btn.pack(padx=5, pady=5)
        
        self.export_btn = self._create_button(
            generate_frame,
            text="💾 Export",
            command=self._export_plot,
            style="secondary"
        )
        self.export_btn.pack(padx=5, pady=2)
    
    def _build_column_selection(self, parent):
        """Build column selection interface."""
        columns_frame = self._create_frame(parent)
        columns_frame.pack(side="left", padx=5, pady=5)
        
        # X column
        x_label = self._create_label(columns_frame, text="X Column:", font_weight="bold")
        x_label.pack(anchor="w", padx=5, pady=2)
        
        self.x_column_var = self._create_string_var()
        self.x_column_combo = self._create_combobox(
            columns_frame,
            values=self.columns,
            variable=self.x_column_var,
            command=self._on_column_change,
            width=120
        )
        self.x_column_combo.pack(padx=5, pady=2)
        
        # Y column
        y_label = self._create_label(columns_frame, text="Y Column:", font_weight="bold")
        y_label.pack(anchor="w", padx=5, pady=2)
        
        self.y_column_var = self._create_string_var()
        self.y_column_combo = self._create_combobox(
            columns_frame,
            values=self.columns,
            variable=self.y_column_var,
            command=self._on_column_change,
            width=120
        )
        self.y_column_combo.pack(padx=5, pady=2)
        
        # Color column (optional)
        color_label = self._create_label(columns_frame, text="Color by:", font_size=10)
        color_label.pack(anchor="w", padx=5, pady=2)
        
        self.color_column_var = self._create_string_var()
        color_options = ["None"] + self.columns
        self.color_column_combo = self._create_combobox(
            columns_frame,
            values=color_options,
            variable=self.color_column_var,
            command=self._on_column_change,
            width=120
        )
        self.color_column_combo.pack(padx=5, pady=2)
    
    def _build_advanced_options(self, parent):
        """Build advanced options panel."""
        options_frame = self._create_frame(parent)
        options_frame.pack(side="left", padx=5, pady=5)
        
        options_label = self._create_label(options_frame, text="Options:", font_weight="bold")
        options_label.pack(anchor="w", padx=5, pady=2)
        
        # Title entry
        title_label = self._create_label(options_frame, text="Title:", font_size=10)
        title_label.pack(anchor="w", padx=5, pady=(2, 0))
        
        self.title_var = self._create_string_var()
        self.title_entry = self._create_entry(
            options_frame,
            textvariable=self.title_var,
            width=150
        )
        self.title_entry.pack(padx=5, pady=2)
        
        # Theme selection
        theme_label = self._create_label(options_frame, text="Theme:", font_size=10)
        theme_label.pack(anchor="w", padx=5, pady=(2, 0))
        
        self.theme_var = self._create_string_var("plotly_dark")
        theme_options = ["plotly_dark", "plotly_white", "ggplot2", "seaborn", "simple_white"]
        self.theme_combo = self._create_combobox(
            options_frame,
            values=theme_options,
            variable=self.theme_var,
            width=120
        )
        self.theme_combo.pack(padx=5, pady=2)
    
    def _build_plot_area(self):
        """Build the plot display area."""
        plot_frame = self._create_frame(self.main_frame)
        plot_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Plot placeholder
        self.plot_placeholder = self._create_label(
            plot_frame,
            text="🎨 Select columns and plot type, then click 'Generate Plot'\n\nSupported plot types:\n• Interactive scatter, line, bar plots\n• Statistical plots (box, violin, histogram)\n• Advanced visualizations (treemap, sunburst)\n• Correlation analysis and heatmaps",
            font_size=14,
            justify="center"
        )
        self.plot_placeholder.pack(expand=True, pady=50)
    
    def _build_info_panel(self):
        """Build information and statistics panel."""
        info_frame = self._create_frame(self.main_frame)
        info_frame.pack(fill="x", padx=5, pady=5)
        
        self.info_label = self._create_label(
            info_frame,
            text="💡 Ready to create interactive plots",
            font_size=12,
            text_color="#888888"
        )
        self.info_label.pack(padx=10, pady=8)
    
    def _get_column_names(self) -> List[str]:
        """Get available column names from data."""
        return self.columns
    
    def _on_plot_type_change(self, value):
        """Handle plot type change."""
        self._update_column_requirements()
        self._update_info(f"Plot type changed to: {value}")
    
    def _on_column_change(self, value):
        """Handle column selection change."""
        self._validate_plot_config()
    
    def _update_column_requirements(self):
        """Update column requirements based on plot type."""
        plot_type = self._get_var_value(self.plot_type_var)
        
        # Update column combo boxes based on plot type requirements
        if plot_type in [AdvancedPlotTypes.HISTOGRAM.value]:
            # Histogram only needs Y column
            self.y_column_combo.configure(state="normal")
            self.x_column_combo.configure(state="disabled")
        elif plot_type in [AdvancedPlotTypes.CORRELATION_MATRIX.value]:
            # Correlation matrix works on all numeric columns
            self.x_column_combo.configure(state="disabled")
            self.y_column_combo.configure(state="disabled")
        else:
            # Most plots need both X and Y
            self.x_column_combo.configure(state="normal")
            self.y_column_combo.configure(state="normal")
    
    def _validate_plot_config(self) -> bool:
        """Validate current plot configuration."""
        plot_type = self._get_var_value(self.plot_type_var)
        x_col = self._get_var_value(self.x_column_var)
        y_col = self._get_var_value(self.y_column_var)
        
        if plot_type == AdvancedPlotTypes.CORRELATION_MATRIX.value:
            return True  # No specific columns needed
        elif plot_type == AdvancedPlotTypes.HISTOGRAM.value:
            return bool(y_col)
        else:
            return bool(x_col and y_col)
    
    def _generate_plot(self):
        """Generate the interactive plot."""
        if not self.data:
            self._update_info("❌ Cannot generate plot: no data available")
            return
        
        if not self._validate_plot_config():
            self._update_info("❌ Please select required columns for the chosen plot type")
            return
        
        try:
            # Create configuration
            plot_type = self._get_var_value(self.plot_type_var)
            config = PlotConfig(
                plot_type=plot_type,
                data_source=self.data,
                x_column=self._get_var_value(self.x_column_var) or None,
                y_column=self._get_var_value(self.y_column_var) or None,
                color_column=self._get_var_value(self.color_column_var) if self._get_var_value(self.color_column_var) != "None" else None,
                title=self._get_var_value(self.title_var) or f"{plot_type.title()} Plot",
                theme=self._get_var_value(self.theme_var)
            )
            
            # Generate figure
            fig = self._create_plotly_figure(config)
            
            # Display plot
            self._display_plot(fig)
            
            self.current_config = config
            self.current_figure = fig
            
            # Update info
            data_count = len(self.data)
            plot_info = f"✅ Generated {config.plot_type} plot with {data_count} data points"
            if config.color_column:
                plot_info += f" (colored by {config.color_column})"
            self._update_info(plot_info)
            
            # Log to app
            self.app_log(f"📊 Created interactive {config.plot_type} plot")
            
        except Exception as e:
            error_msg = self.error_handler.handle_error(e, "generating Plotly plot")
            self._update_info(f"❌ Error: {str(e)}")
            self.app_log(f"❌ Plot generation failed: {error_msg}")
    
    def _create_plotly_figure(self, config: PlotConfig):
        """Create Plotly figure based on configuration."""
        if not HAS_PLOTLY:
            raise ImportError("Plotly is not available")
        
        # Import plotly modules
        import plotly.express as px
        import plotly.graph_objects as go
        
        # Prepare data
        df_data = {col: [row.get(col) for row in config.data_source] for col in self.columns}
        
        try:
            import pandas as pd
            df = pd.DataFrame(df_data)
        except ImportError:
            # Fallback without pandas
            df = df_data
        
        # Create figure based on plot type
        fig = None
        
        if config.plot_type == AdvancedPlotTypes.SCATTER.value:
            fig = px.scatter(
                df, x=config.x_column, y=config.y_column,
                color=config.color_column,
                title=config.title,
                template=config.theme
            )
        
        elif config.plot_type == AdvancedPlotTypes.LINE.value:
            fig = px.line(
                df, x=config.x_column, y=config.y_column,
                color=config.color_column,
                title=config.title,
                template=config.theme
            )
        
        elif config.plot_type == AdvancedPlotTypes.BAR.value:
            fig = px.bar(
                df, x=config.x_column, y=config.y_column,
                color=config.color_column,
                title=config.title,
                template=config.theme
            )
        
        elif config.plot_type == AdvancedPlotTypes.HISTOGRAM.value:
            fig = px.histogram(
                df, x=config.y_column,
                color=config.color_column,
                title=config.title,
                template=config.theme
            )
        
        elif config.plot_type == AdvancedPlotTypes.BOX.value:
            fig = px.box(
                df, x=config.x_column, y=config.y_column,
                color=config.color_column,
                title=config.title,
                template=config.theme
            )
        
        elif config.plot_type == AdvancedPlotTypes.VIOLIN.value:
            fig = px.violin(
                df, x=config.x_column, y=config.y_column,
                color=config.color_column,
                title=config.title,
                template=config.theme
            )
        
        elif config.plot_type == AdvancedPlotTypes.HEATMAP.value:
            # Create correlation heatmap for numeric columns
            numeric_cols = self._get_numeric_columns()
            if len(numeric_cols) < 2:
                raise ValueError("Heatmap requires at least 2 numeric columns")
            
            # Calculate correlation matrix
            corr_matrix = self._calculate_correlation_matrix_from_data(numeric_cols)
            
            if not HAS_PLOTLY:
                raise ValueError("Plotly is required for heatmap visualization")
            
            import plotly.graph_objects as go
            fig = go.Figure(data=go.Heatmap(
                z=corr_matrix,
                x=numeric_cols,
                y=numeric_cols,
                colorscale='RdBu',
                zmid=0
            ))
            fig.update_layout(title=config.title, template=config.theme)
        
        elif config.plot_type == AdvancedPlotTypes.TREEMAP.value:
            if not config.color_column:
                raise ValueError("Treemap requires a color/category column")
            fig = px.treemap(
                df, path=[config.color_column], values=config.y_column,
                title=config.title,
                template=config.theme
            )
        
        elif config.plot_type == AdvancedPlotTypes.SUNBURST.value:
            if not config.color_column:
                raise ValueError("Sunburst requires a color/category column")
            fig = px.sunburst(
                df, path=[config.color_column], values=config.y_column,
                title=config.title,
                template=config.theme
            )
        
        elif config.plot_type == AdvancedPlotTypes.BUBBLE.value:
            fig = px.scatter(
                df, x=config.x_column, y=config.y_column,
                size=config.y_column,  # Use Y as size if no size column specified
                color=config.color_column,
                title=config.title,
                template=config.theme,
                size_max=60
            )
        
        elif config.plot_type == AdvancedPlotTypes.CORRELATION_MATRIX.value:
            numeric_cols = self._get_numeric_columns()
            if len(numeric_cols) < 2:
                raise ValueError("Correlation matrix requires at least 2 numeric columns")
            
            # Calculate correlation matrix
            corr_matrix = self._calculate_correlation_matrix_from_data(numeric_cols)
            
            import plotly.graph_objects as go
            fig = go.Figure(data=go.Heatmap(
                z=corr_matrix,
                x=numeric_cols,
                y=numeric_cols,
                colorscale='RdBu',
                zmid=0,
                text=[[f"{val:.2f}" for val in row] for row in corr_matrix],
                texttemplate="%{text}",
                textfont={"size": 10}
            ))
            fig.update_layout(
                title=config.title or "Correlation Matrix",
                template=config.theme,
                xaxis_title="Features",
                yaxis_title="Features"
            )
        
        else:
            raise ValueError(f"Unsupported plot type: {config.plot_type}")
        
        # Update layout for better appearance
        if fig:
            fig.update_layout(
                height=600,
                margin=dict(l=50, r=50, t=80, b=50),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
        
        return fig
    
    def _get_numeric_columns(self) -> List[str]:
        """Get list of numeric columns from data."""
        numeric_cols = []
        if not self.data:
            return numeric_cols
        
        for col in self.columns:
            # Check if column contains numeric data
            values = [row.get(col) for row in self.data[:100]]  # Sample first 100 rows
            numeric_count = sum(1 for v in values if isinstance(v, (int, float)))
            if numeric_count > len(values) * 0.8:  # 80% numeric
                numeric_cols.append(col)
        
        return numeric_cols
    
    def _calculate_correlation_matrix_from_data(self, numeric_cols: List[str]) -> List[List[float]]:
        """Calculate correlation matrix from data."""
        if not HAS_NUMPY:
            # Simple correlation calculation without numpy
            matrix = []
            for i, col1 in enumerate(numeric_cols):
                row = []
                for j, col2 in enumerate(numeric_cols):
                    if i == j:
                        row.append(1.0)
                    else:
                        # Simple correlation approximation
                        values1 = [row.get(col1) for row in self.data if isinstance(row.get(col1), (int, float))]
                        values2 = [row.get(col2) for row in self.data if isinstance(row.get(col2), (int, float))]
                        correlation = self._simple_correlation(values1, values2)
                        row.append(correlation)
                matrix.append(row)
            return matrix
        else:
            # Use numpy for accurate correlation
            try:
                import numpy as np
                data_matrix = []
                for col in numeric_cols:
                    values = [row.get(col) for row in self.data if isinstance(row.get(col), (int, float))]
                    data_matrix.append(values)
                
                if data_matrix:
                    corr_matrix = np.corrcoef(data_matrix)
                    return corr_matrix.tolist()
                return []
            except ImportError:
                # Fallback to simple correlation if numpy not available
                matrix = []
                for i, col1 in enumerate(numeric_cols):
                    row = []
                    for j, col2 in enumerate(numeric_cols):
                        if i == j:
                            row.append(1.0)
                        else:
                            values1 = [row.get(col1) for row in self.data if isinstance(row.get(col1), (int, float))]
                            values2 = [row.get(col2) for row in self.data if isinstance(row.get(col2), (int, float))]
                            correlation = self._simple_correlation(values1, values2)
                            row.append(correlation)
                    matrix.append(row)
                return matrix
    
    def _simple_correlation(self, x: List[float], y: List[float]) -> float:
        """Calculate simple correlation coefficient."""
        if len(x) != len(y) or len(x) < 2:
            return 0.0
        
        n = len(x)
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(x[i] * y[i] for i in range(n))
        sum_x2 = sum(xi * xi for xi in x)
        sum_y2 = sum(yi * yi for yi in y)
        
        numerator = n * sum_xy - sum_x * sum_y
        denominator = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)) ** 0.5
        
        if denominator == 0:
            return 0.0
        
        return numerator / denominator
    
    def _display_plot(self, fig):
        """Display Plotly figure in the interface."""
        # Hide placeholder
        self.plot_placeholder.pack_forget()
        
        # Create HTML file and open in browser
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
            html_content = fig.to_html(include_plotlyjs=True)
            f.write(html_content)
            html_path = f.name
        
        # Open in default browser
        webbrowser.open(f'file://{html_path}')
        
        # Show a message in the UI
        if hasattr(self, 'plot_display_label'):
            self.plot_display_label.destroy()
        
        self.plot_display_label = self._create_label(
            self.main_frame,
            text="🌐 Interactive plot opened in your web browser\n\n🔧 Features available:\n• Zoom and pan\n• Hover for details\n• Download plot\n• Toggle series",
            font_size=14,
            justify="center"
        )
        self.plot_display_label.pack(expand=True, pady=20)
    
    def _export_plot(self):
        """Export current plot to various formats."""
        if not self.current_figure:
            self._update_info("❌ No plot to export. Generate a plot first.")
            return
        
        try:
            from tkinter import filedialog
            
            # Ask user for export format and location
            file_path = filedialog.asksaveasfilename(
                defaultextension=".html",
                filetypes=[
                    ("HTML files", "*.html"),
                    ("PNG images", "*.png"),
                    ("SVG images", "*.svg"),
                    ("PDF files", "*.pdf")
                ]
            )
            
            if not file_path:
                return
            
            # Export based on file extension
            file_ext = Path(file_path).suffix.lower()
            
            if file_ext == '.html':
                self.current_figure.write_html(file_path)
            elif file_ext == '.png':
                self.current_figure.write_image(file_path, format='png')
            elif file_ext == '.svg':
                self.current_figure.write_image(file_path, format='svg')
            elif file_ext == '.pdf':
                self.current_figure.write_image(file_path, format='pdf')
            else:
                # Default to HTML
                self.current_figure.write_html(file_path)
            
            self._update_info(f"✅ Plot exported to {file_path}")
            self.app_log(f"📁 Plot exported to {file_path}")
            
        except Exception as e:
            error_msg = self.error_handler.handle_error(e, "exporting plot")
            self._update_info(f"❌ Export failed: {str(e)}")
            self.app_log(f"❌ Export failed: {error_msg}")
    
    def _show_plotly_missing_message(self):
        """Show message when Plotly is not available."""
        missing_frame = self._create_frame(self.parent)
        missing_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Icon and title
        icon_label = self._create_label(
            missing_frame,
            text="📦",
            font_size=48
        )
        icon_label.pack(pady=(50, 20))
        
        title_label = self._create_label(
            missing_frame,
            text="Plotly Not Available",
            font_size=20,
            font_weight="bold"
        )
        title_label.pack(pady=(0, 10))
        
        # Message
        message_label = self._create_label(
            missing_frame,
            text="Advanced interactive plotting requires Plotly.\nInstall it to enable these features:",
            font_size=14,
            justify="center"
        )
        message_label.pack(pady=(0, 20))
        
        # Installation instructions
        install_text = "pip install plotly pandas numpy"
        install_label = self._create_label(
            missing_frame,
            text=install_text,
            font_size=12,
            font_family="Courier",
            text_color="#00ff00"
        )
        install_label.pack(pady=(0, 10))
        
        # Copy button
        copy_btn = self._create_button(
            missing_frame,
            text="📋 Copy Command",
            command=lambda: self._copy_to_clipboard(install_text),
            style="primary"
        )
        copy_btn.pack(pady=10)
        
        # Info
        info_label = self._create_label(
            missing_frame,
            text="💡 You can still use the basic matplotlib plots in the 'Visual Analytics' tab",
            font_size=12,
            text_color="#888888"
        )
        info_label.pack(pady=(20, 0))
    
    def _copy_to_clipboard(self, text):
        """Copy text to clipboard."""
        try:
            self.parent.clipboard_clear()
            self.parent.clipboard_append(text)
            self._update_info("✅ Command copied to clipboard")
        except Exception:
            pass
    
    def _update_info(self, message: str):
        """Update information panel."""
        if hasattr(self, 'info_label'):
            self.info_label.configure(text=message)
    
    def update_data(self, new_data):
        """Update data source for plotting."""
        self.data = new_data or []
        self.columns = list(new_data[0].keys()) if new_data else []
        
        # Update column combo boxes
        if hasattr(self, 'x_column_combo'):
            self.x_column_combo.configure(values=self.columns)
        if hasattr(self, 'y_column_combo'):
            self.y_column_combo.configure(values=self.columns)
        if hasattr(self, 'color_column_combo'):
            color_options = ["None"] + self.columns
            self.color_column_combo.configure(values=color_options)
        
        column_names = self.columns
        data_rows = len(new_data)
        self._update_info(f"Data updated: {data_rows} rows, {len(column_names)} columns")
