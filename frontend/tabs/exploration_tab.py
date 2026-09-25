"""
Exploration Tab for DataForge
"""
from tkinter import filedialog, messagebox
from pathlib import Path

try:
    import customtkinter as ctk
    HAS_CTK = True
except ImportError:
    HAS_CTK = False

try:
    import polars as pl
except ImportError:
    pl = None

from frontend.components.data_explorer import DataExplorer
from frontend.components.plot_explorer import PlotExplorer
from frontend.components.plotly_explorer import PlotlyExplorer
from frontend.presentation.styles.button_factory import ButtonFactory

class ExplorationTab:
    def __init__(self, parent, app):
        self.parent = parent
        self.app = app
        self.data_explorer = None
        self.plot_explorer = None
        self.plotly_explorer = None
        self.current_data = None
        self.current_mode = 'main'  # 'main', 'table', 'plots', 'more_plots'
        self.build()

    def build(self):
        """Builds the Exploration tab UI."""
        self.main_container = ctk.CTkFrame(self.parent, fg_color="transparent")

        # Configure main container for expansion
        self.main_container.grid_rowconfigure(0, weight=1)
        self.main_container.grid_columnconfigure(0, weight=1)

        self._build_exploration_main(self.main_container)

    def _build_exploration_main(self, container):
        """Builds the main exploration screen with choices."""
        for widget in container.winfo_children():
            widget.destroy()

        main_frame = ctk.CTkFrame(container)
        main_frame.grid(row=0, column=0, sticky="nsew", padx=20, pady=20)

        # Configure main_frame for expansion
        main_frame.grid_rowconfigure(4, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(main_frame, text="Data Exploration", font=ctk.CTkFont(size=20, weight="bold")).grid(row=0, column=0, pady=(20, 10))

        # Center the content with a narrower frame
        content_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        content_frame.grid(row=1, column=0, pady=10)
        content_frame.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(content_frame, text="Choose an exploration tool:").grid(row=0, column=0, pady=(0, 20))

        # Create button container for better spacing
        button_container = ctk.CTkFrame(content_frame, fg_color="transparent")
        button_container.grid(row=1, column=0)

        # Configure button container for centering
        button_container.grid_columnconfigure(0, weight=1)

        # Buttons with compact width (~20% of screen width) and consistent styling
        table_btn = ButtonFactory.create_primary_button(
            button_container,
            "📊 Table Explorer",
            self._open_table_explorer,
            height=45,
            width=200
        )
        table_btn.grid(row=0, column=0, pady=10, padx=10)

        plot_btn = ButtonFactory.create_primary_button(
            button_container,
            "📈 Plot Explorer", 
            self._open_plots_explorer,
            height=45,
            width=200
        )
        plot_btn.grid(row=1, column=0, pady=10, padx=10)

        # Add More Plots button for Plotly integration
        more_plots_btn = ButtonFactory.create_primary_button(
            button_container,
            "🎨 More Plots (Interactive)", 
            self._open_more_plots_explorer,
            height=45,
            width=200
        )
        more_plots_btn.grid(row=2, column=0, pady=10, padx=10)

        # Add empty row for bottom spacing
        main_frame.grid_rowconfigure(2, weight=1)

    def _open_table_explorer(self):
        """Opens the data loading interface for the table explorer."""
        # Track navigation state
        self.app.navigation_history.append({'tab': 'exploration', 'action': 'exploration_main'})
        self.app._update_back_button_state()
        
        self._build_data_loading_interface(self.main_container, self._continue_to_table_explorer)

    def _open_plots_explorer(self):
        """Opens the data loading interface for the plot explorer."""
        # Track navigation state
        self.app.navigation_history.append({'tab': 'exploration', 'action': 'exploration_main'})
        self.app._update_back_button_state()
        
        self._build_data_loading_interface(self.main_container, self._continue_to_plots_explorer)

    def _open_more_plots_explorer(self):
        """Opens the data loading interface for the interactive plots explorer."""
        # Track navigation state
        self.app.navigation_history.append({'tab': 'exploration', 'action': 'exploration_main'})
        self.app._update_back_button_state()
        
        self._build_data_loading_interface(self.main_container, self._continue_to_more_plots_explorer)

    def _build_data_loading_interface(self, container, on_continue_callback):
        """Builds the UI to load data for an exploration tool."""
        for widget in container.winfo_children():
            widget.destroy()

        load_frame = ctk.CTkFrame(container)
        load_frame.grid(row=0, column=0, sticky="nsew", padx=20, pady=20)

        # Configure load_frame for expansion
        load_frame.grid_rowconfigure(4, weight=1)
        load_frame.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(load_frame, text="Load Data for Exploration", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, pady=(20, 10))

        # Center the content with a narrower frame
        content_frame = ctk.CTkFrame(load_frame, fg_color="transparent")
        content_frame.grid(row=1, column=0, pady=10)
        content_frame.grid_columnconfigure(0, weight=1)

        # Data loading buttons with consistent styling
        current_schema_btn = ButtonFactory.create_primary_button(
            content_frame,
            "Load from Current Schema",
            lambda: self._load_current_schema_data(on_continue_callback)
        )
        current_schema_btn.grid(row=0, column=0, pady=8, padx=20, sticky="ew")
        
        sample_data_btn = ButtonFactory.create_secondary_button(
            content_frame,
            "Load Sample Data",
            lambda: self._load_sample_exploration_data(on_continue_callback)
        )
        sample_data_btn.grid(row=1, column=0, pady=8, padx=20, sticky="ew")
        
        upload_csv_btn = ButtonFactory.create_secondary_button(
            content_frame,
            "Upload CSV File",
            lambda: self._upload_csv_for_exploration(on_continue_callback)
        )
        upload_csv_btn.grid(row=2, column=0, pady=8, padx=20, sticky="ew")

        # Remove the back button - now handled by sidebar back button

        # Add empty row for bottom spacing
        load_frame.grid_rowconfigure(2, weight=1)

    def _load_current_schema_data(self, on_continue):
        schema_name = self.app.config.DEFAULT_SCHEMA # Or use an entry
        self.app.log(f"Reading data from schema '{schema_name}' for exploration...")
        def do_read():
            return self.app.api_client.read_polars(schema_name)
        def on_done(result):
            table, num_rows = result
            if num_rows > 0:
                self.app.exploration_data = table.to_dicts()
                self.app.log(f"✅ Loaded {num_rows} records for exploration.")
                on_continue()
            else:
                messagebox.showinfo("No Data", f"No data found in schema '{schema_name}'.")
        def on_error(e):
            messagebox.showerror("Error", f"Failed to load data: {e}")
        self.app.runner.run(do_read, on_done, on_error)

    def _load_sample_exploration_data(self, on_continue):
        self.app.exploration_data = self.app.data_generator.generate_sample_data(500)
        self.app.log("✅ Loaded 500 sample records for exploration.")
        on_continue()

    def _upload_csv_for_exploration(self, on_continue):
        if pl is None:
            messagebox.showerror("Missing Library", "Polars is not installed. Cannot read CSV files.")
            return
        filepath = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if not filepath:
            return
        try:
            df = pl.read_csv(filepath)
            self.app.exploration_data = df.to_dicts()
            self.app.log(f"✅ Loaded {len(self.app.exploration_data)} records from CSV.")
            on_continue()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read CSV file: {e}")

    def _continue_to_table_explorer(self):
        """Builds the table explorer view."""
        if not self.app.exploration_data:
            messagebox.showwarning("No Data", "No data is loaded for exploration.")
            self._build_exploration_main(self.main_container)
            return

        # Track navigation state
        self.app.navigation_history.append({'tab': 'exploration', 'action': 'data_loading'})
        self.app._update_back_button_state()

        for widget in self.main_container.winfo_children():
            widget.destroy()

        # Configure main_container for the explorer
        self.main_container.grid_rowconfigure(0, weight=1)
        self.main_container.grid_columnconfigure(0, weight=1)

        self.data_explorer = DataExplorer(
            self.main_container,
            data=self.app.exploration_data,
            app_log=self.app.log,
            export_callback=self._export_exploration_data
        )

        # Remove the back button - now handled by sidebar

    def _continue_to_plots_explorer(self):
        """Builds the plot explorer view."""
        if not self.app.exploration_data:
            messagebox.showwarning("No Data", "No data is loaded for exploration.")
            self._build_exploration_main(self.main_container)
            return

        # Track navigation state
        self.app.navigation_history.append({'tab': 'exploration', 'action': 'data_loading'})
        self.app._update_back_button_state()

        for widget in self.main_container.winfo_children():
            widget.destroy()

        # Configure main_container for the explorer
        self.main_container.grid_rowconfigure(0, weight=1)
        self.main_container.grid_columnconfigure(0, weight=1)

        self.plot_explorer = PlotExplorer(
            self.main_container,
            data=self.app.exploration_data,
            app_log=self.app.log,
            on_back=self._back_to_exploration_main
        )

    def _continue_to_more_plots_explorer(self):
        """Builds the interactive plots explorer view with Plotly."""
        if not self.app.exploration_data:
            messagebox.showwarning("No Data", "No data is loaded for exploration.")
            self._build_exploration_main(self.main_container)
            return

        # Track navigation state
        self.app.navigation_history.append({'tab': 'exploration', 'action': 'data_loading'})
        self.app._update_back_button_state()

        for widget in self.main_container.winfo_children():
            widget.destroy()

        # Configure main_container for the explorer
        self.main_container.grid_rowconfigure(0, weight=1)
        self.main_container.grid_columnconfigure(0, weight=1)

        self.plotly_explorer = PlotlyExplorer(
            self.main_container,
            data=self.app.exploration_data,
            app_log=self.app.log
        )
        self.current_data = self.app.exploration_data

    def _back_to_exploration_main(self):
        """Clears exploration data and returns to the main selection screen."""
        self.app.exploration_data = []
        self.current_data = None
        self.data_explorer = None
        self.plot_explorer = None
        self.plotly_explorer = None
        self._build_exploration_main(self.main_container)

    def _export_exploration_data(self, filtered_data):
        """Handles exporting data from the DataExplorer."""
        if not filtered_data:
            messagebox.showwarning("No Data", "There is no data to export.")
            return
        if pl is None:
            messagebox.showerror("Missing Library", "Polars is not installed. Cannot export.")
            return

        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if not filepath:
            return

        try:
            df = pl.DataFrame(filtered_data)
            df.write_csv(Path(filepath))
            self.app.log(f"✅ Exported {len(filtered_data)} rows to {filepath}")
            messagebox.showinfo("Export Successful", f"Data exported to {filepath}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export data: {e}")
