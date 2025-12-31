"""
Exploration Tab for DataForge Application
"""
from .base_tab import BaseTab
from frontend.components import DataExplorer, EnhancedPlotExplorer, FloatingDataExplorer
from tkinter import filedialog, messagebox


class ExplorationTab(BaseTab):
    """Exploration tab for data analysis and visualization"""
    
    def __init__(self, app):
        super().__init__(app)
        self.current_view = "main"  # main, table, plots
        
    def build_content(self, parent):
        """Build the exploration tab content"""
        self.content_frame = self.ui_adapter.create_frame(parent)
        self.content_frame.pack(fill="both", expand=True)
        
        if self.current_view == "main":
            self._build_main_view()
        elif self.current_view == "table":
            self._build_table_view()
        elif self.current_view == "plots":
            self._build_plots_view()
    
    def _build_main_view(self):
        """Build the main exploration overview"""
        # Header
        header_frame = self.ui_adapter.create_frame(self.content_frame)
        header_frame.pack(fill="x", pady=20)
        
        self.ui_adapter.create_label(
            header_frame,
            text="📊 Data Exploration",
            font_size=20,
            font_weight="bold"
        ).pack()
        
        self.ui_adapter.create_label(
            header_frame,
            text="Analyze and visualize your data with advanced tools",
            font_size=12
        ).pack(pady=(5, 0))
        
        # Data loading section
        data_frame = self.ui_adapter.create_frame(self.content_frame)
        data_frame.pack(fill="x", pady=20, padx=50)
        
        self.ui_adapter.create_label(
            data_frame,
            text="Load Data",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        # Data loading buttons
        load_buttons = self.ui_adapter.create_frame(data_frame)
        load_buttons.pack(fill="x")
        
        self.ui_adapter.create_button(
            load_buttons,
            text="🗄️ Load from Database",
            command=self._load_from_database,
            width=200,
            height=40
        ).pack(side="left", padx=(0, 10))
        
        self.ui_adapter.create_button(
            load_buttons,
            text="📂 Upload CSV File",
            command=self._upload_csv,
            width=200,
            height=40
        ).pack(side="left", padx=(0, 10))
        
        self.ui_adapter.create_button(
            load_buttons,
            text="🎲 Generate Sample",
            command=self._generate_sample,
            width=200,
            height=40
        ).pack(side="left", padx=(0, 10))
        
        # Current data status
        status_frame = self.ui_adapter.create_frame(self.content_frame)
        status_frame.pack(fill="x", pady=20, padx=50)
        
        self.ui_adapter.create_label(
            status_frame,
            text="Current Data",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        # Data info
        data = self.app.get_exploration_data()
        data_info = f"📈 {len(data)} records loaded" if data else "📭 No data loaded"
        
        self.data_status_label = self.ui_adapter.create_label(
            status_frame,
            text=data_info,
            font_size=14
        )
        self.data_status_label.pack(anchor="w", pady=5)
        
        if data:
            # Exploration options
            explore_frame = self.ui_adapter.create_frame(self.content_frame)
            explore_frame.pack(fill="x", pady=20, padx=50)
            
            self.ui_adapter.create_label(
                explore_frame,
                text="Exploration Tools",
                font_size=16,
                font_weight="bold"
            ).pack(anchor="w", pady=(0, 10))
            
            # Exploration buttons
            explore_buttons = self.ui_adapter.create_frame(explore_frame)
            explore_buttons.pack(fill="x")
            
            self.ui_adapter.create_button(
                explore_buttons,
                text="📋 Table Explorer",
                command=self._open_table_explorer,
                width=200,
                height=50
            ).pack(side="left", padx=(0, 10))
            
            self.ui_adapter.create_button(
                explore_buttons,
                text="📊 Plot Explorer",
                command=self._open_plots_explorer,
                width=200,
                height=50
            ).pack(side="left", padx=(0, 10))
    
    def _build_table_view(self):
        """Build the table exploration view"""
        # Back button in app
        self.app._show_back_button("← Back to Exploration", self._back_to_main)
        
        # Header
        header_frame = self.ui_adapter.create_frame(self.content_frame)
        header_frame.pack(fill="x", pady=10)
        
        self.ui_adapter.create_label(
            header_frame,
            text="📋 Table Explorer",
            font_size=18,
            font_weight="bold"
        ).pack()
        
        # Data explorer component
        data = self.app.get_exploration_data()
        if data:
            self.data_explorer = DataExplorer(
                self.content_frame,
                data=data,
                app_log=self.app._log,
                export_callback=self._export_filtered_data
            )
        else:
            self.ui_adapter.create_label(
                self.content_frame,
                text="No data loaded for exploration",
                font_size=14
            ).pack(pady=50)
    
    def _build_plots_view(self):
        """Build the plots exploration view"""
        # Back button in app
        self.app._show_back_button("← Back to Exploration", self._back_to_main)
        
        # Header
        header_frame = self.ui_adapter.create_frame(self.content_frame)
        header_frame.pack(fill="x", pady=10)
        
        self.ui_adapter.create_label(
            header_frame,
            text="📊 Plot Explorer",
            font_size=18,
            font_weight="bold"
        ).pack()
        
        # Plot explorer component
        data = self.app.get_exploration_data()
        if data:
            self.plot_explorer = EnhancedPlotExplorer(
                self.content_frame,
                data=data
            )
        else:
            self.ui_adapter.create_label(
                self.content_frame,
                text="No data loaded for plotting",
                font_size=14
            ).pack(pady=50)
    
    def _load_from_database(self):
        """Load data from database"""
        # This would typically open a schema selection dialog
        # For now, just load from the default schema
        schema_name = "well_production"
        
        def read():
            table, count = self.app.get_api_client().read_polars(schema_name)
            # Convert Arrow table to list of dicts for compatibility
            df = table.to_pandas()
            return df.to_dict('records'), count
        
        def on_done(result):
            data, count = result
            self.app.set_exploration_data(data)
            self._log(f"✅ Loaded {count} records from {schema_name}")
            self._refresh_main_view()
        
        def on_error(error):
            self._log(f"❌ Error loading data: {error}")
            messagebox.showerror("Error", f"Failed to load data: {error}")
        
        self._log(f"Loading data from {schema_name}...")
        self.app.get_async_runner().run(read, on_done, on_error)
    
    def _upload_csv(self):
        """Upload CSV file for exploration"""
        filename = filedialog.askopenfilename(
            title="Select CSV File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        
        if filename:
            def load_csv():
                import csv
                data = []
                with open(filename, 'r', newline='') as f:
                    reader = csv.DictReader(f)
                    data = list(reader)
                return data
            
            def on_done(data):
                self.app.set_exploration_data(data)
                self._log(f"✅ Loaded {len(data)} records from CSV")
                self._refresh_main_view()
            
            def on_error(error):
                self._log(f"❌ Error loading CSV: {error}")
                messagebox.showerror("Error", f"Failed to load CSV: {error}")
            
            self._log(f"Loading CSV file: {filename}")
            self.app.get_async_runner().run(load_csv, on_done, on_error)
    
    def _generate_sample(self):
        """Generate sample data for exploration"""
        def generate():
            return self.app.get_data_generator().generate_sample_data(1000)
        
        def on_done(data):
            self.app.set_exploration_data(data)
            self._log(f"✅ Generated {len(data)} sample records")
            self._refresh_main_view()
        
        def on_error(error):
            self._log(f"❌ Error generating data: {error}")
            messagebox.showerror("Error", f"Failed to generate data: {error}")
        
        self._log("Generating sample data...")
        self.app.get_async_runner().run(generate, on_done, on_error)
    
    def _open_table_explorer(self):
        """Open table explorer view"""
        self.current_view = "table"
        self._refresh_view()
    
    def _open_plots_explorer(self):
        """Open plots explorer view"""
        self.current_view = "plots"
        self._refresh_view()
    
    def _back_to_main(self):
        """Return to main exploration view"""
        self.current_view = "main"
        self.app._hide_back_button()
        self._refresh_view()
    
    def _refresh_view(self):
        """Refresh the current view"""
        # Clear content and rebuild
        for widget in self.content_frame.winfo_children():
            widget.destroy()
        self.build_content(self.content_frame.master)
    
    def _refresh_main_view(self):
        """Refresh the main view with updated data status"""
        if self.current_view == "main":
            self._refresh_view()
    
    def _export_filtered_data(self, filtered_data):
        """Export filtered data from table explorer"""
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("JSON files", "*.json")]
        )
        
        if filename:
            def export():
                import csv
                import json
                from pathlib import Path
                
                path = Path(filename)
                if path.suffix.lower() == '.json':
                    with open(path, 'w') as f:
                        json.dump(filtered_data, f, indent=2)
                else:
                    # Default to CSV
                    if filtered_data:
                        fieldnames = filtered_data[0].keys()
                        with open(path, 'w', newline='') as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(filtered_data)
                return str(path)
            
            def on_done(filepath):
                self._log(f"✅ Filtered data exported to {filepath}")
                messagebox.showinfo("Success", f"Filtered data exported to {filepath}")
            
            def on_error(error):
                self._log(f"❌ Error exporting data: {error}")
                messagebox.showerror("Error", f"Failed to export data: {error}")
            
            self.app.get_async_runner().run(export, on_done, on_error)
