"""
Database Tab for DataForge
"""
from tkinter import filedialog
from pathlib import Path

try:
    import customtkinter as ctk
    HAS_CTK = True
except ImportError:
    HAS_CTK = False

import polars as pl

from frontend.utils.ui_helpers import format_json
from frontend.presentation.styles.button_factory import ButtonFactory

class DatabaseTab:
    def __init__(self, parent, app):
        self.parent = parent
        self.app = app
        self.main_frame = ctk.CTkFrame(self.parent, fg_color="transparent")
        self.build()

    def build(self):
        """Builds the Database tab UI."""
        self.main_frame.grid_rowconfigure(0, weight=1)
        self.main_frame.grid_columnconfigure(0, weight=1)
        
        db_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        db_frame.grid(row=0, column=0, sticky="nsew")

        # Create a tab view for different database operations
        db_tab_view = ctk.CTkTabview(db_frame)
        db_tab_view.pack(fill="both", expand=True, padx=10, pady=10)

        db_tab_view.add("Upload")
        db_tab_view.add("Download")
        db_tab_view.add("Schema Management")

        self._build_upload_content(db_tab_view.tab("Upload"))
        self._build_download_content(db_tab_view.tab("Download"))
        self._build_schema_management_content(db_tab_view.tab("Schema Management"))

    def _build_upload_content(self, parent):
        """Content for the 'Upload' tab."""
        upload_frame = ctk.CTkFrame(parent, fg_color="transparent")
        upload_frame.pack(fill="both", expand=True, padx=10, pady=10)

        ctk.CTkLabel(upload_frame, text="Generate and Write Data", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=10)

        # --- Data Generation ---
        gen_frame = ctk.CTkFrame(upload_frame)
        gen_frame.pack(fill="x", pady=5)

        ctk.CTkLabel(gen_frame, text="Schema:").pack(side="left", padx=10)
        self.app.upload_schema_entry = ctk.CTkEntry(gen_frame, placeholder_text="e.g., well_production")
        self.app.upload_schema_entry.pack(side="left", padx=5, fill="x", expand=True)
        self.app.upload_schema_entry.insert(0, self.app.config.DEFAULT_SCHEMA)

        ctk.CTkLabel(gen_frame, text="Records:").pack(side="left", padx=10)
        self.app.num_records_entry = ctk.CTkEntry(gen_frame, placeholder_text="e.g., 10000")
        self.app.num_records_entry.pack(side="left", padx=5)
        self.app.num_records_entry.insert(0, self.app.config.DEFAULT_RECORDS)

        # Generate Sample Data button with consistent styling
        generate_btn = ButtonFactory.create_primary_button(
            upload_frame,
            "Generate Sample Data",
            self._on_generate_data
        )
        generate_btn.pack(pady=10)

        # --- Write Controls ---
        write_frame = ctk.CTkFrame(upload_frame)
        write_frame.pack(fill="x", pady=5)

        ctk.CTkLabel(write_frame, text="Compression:").pack(side="left", padx=10)
        self.app.compression_var = self.app._ctk_string(self.app.config.DEFAULT_COMPRESSION)
        ctk.CTkOptionMenu(write_frame, values=["zstd", "lz4", "uncompressed"], variable=self.app.compression_var).pack(side="left", padx=5)

        # Write to Database button with success styling
        write_btn = ButtonFactory.create_success_button(
            write_frame,
            "Write to Database",
            self._on_write
        )
        write_btn.pack(side="right", padx=10)

    def _build_download_content(self, parent):
        """Content for the 'Download' tab."""
        download_frame = ctk.CTkFrame(parent, fg_color="transparent")
        download_frame.pack(fill="both", expand=True, padx=10, pady=10)

        ctk.CTkLabel(download_frame, text="Read and Export Data", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=10)

        # --- Read Controls ---
        read_frame = ctk.CTkFrame(download_frame)
        read_frame.pack(fill="x", pady=5)

        ctk.CTkLabel(read_frame, text="Schema:").pack(side="left", padx=10)
        self.app.read_schema_entry = ctk.CTkEntry(read_frame, placeholder_text="e.g., well_production")
        self.app.read_schema_entry.pack(side="left", padx=5, fill="x", expand=True)
        self.app.read_schema_entry.insert(0, self.app.config.DEFAULT_SCHEMA)

        # Read buttons with consistent styling
        read_btn = ButtonFactory.create_primary_button(
            read_frame,
            "Read from Database",
            self._on_read
        )
        read_btn.pack(side="left", padx=10)
        
        read_save_btn = ButtonFactory.create_secondary_button(
            read_frame,
            "Read & Save to CSV",
            self._on_read_and_save
        )
        read_save_btn.pack(side="left", padx=10)

    def _build_schema_management_content(self, parent):
        """Content for the 'Schema Management' tab."""
        schema_frame = ctk.CTkFrame(parent, fg_color="transparent")
        schema_frame.pack(fill="both", expand=True, padx=10, pady=10)

        ctk.CTkLabel(schema_frame, text="Manage Data Schemas", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=10)

        # --- Schema Actions ---
        actions_frame = ctk.CTkFrame(schema_frame)
        actions_frame.pack(fill="x", pady=5)

        # Schema management buttons with consistent styling
        list_schemas_btn = ButtonFactory.create_primary_button(
            actions_frame,
            "List All Schemas",
            self._on_list_schemas
        )
        list_schemas_btn.pack(side="left", padx=5)
        
        register_example_btn = ButtonFactory.create_secondary_button(
            actions_frame,
            "Register Example Schema",
            self._on_register_example
        )
        register_example_btn.pack(side="left", padx=5)

        # --- Schema Details ---
        details_frame = ctk.CTkFrame(schema_frame)
        details_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(details_frame, text="Schema Name:").pack(side="left", padx=10)
        self.app.schema_details_entry = ctk.CTkEntry(details_frame, placeholder_text="Enter schema name to get details")
        self.app.schema_details_entry.pack(side="left", fill="x", expand=True, padx=5)
        self.app.schema_details_entry.insert(0, self.app.config.DEFAULT_SCHEMA)

        # Schema details buttons with consistent styling
        get_schema_btn = ButtonFactory.create_primary_button(
            details_frame,
            "Get Latest Schema",
            self._on_get_latest_schema
        )
        get_schema_btn.pack(side="left", padx=5)
        
        get_versions_btn = ButtonFactory.create_secondary_button(
            details_frame,
            "Get Versions",
            self._on_get_schema_versions
        )
        get_versions_btn.pack(side="left", padx=5)

        # --- Schema Display ---
        self.app.schema_display = ctk.CTkTextbox(schema_frame, height=200)
        self.app.schema_display.pack(fill="both", expand=True, pady=10)

    # --- Event Handlers ---
    def _on_generate_data(self):
        try:
            num_records = int(self.app._entry_get(self.app.num_records_entry))
            self.app.log(f"Generating {num_records} sample records...")
            self.app.sample_data = self.app.data_generator.generate_sample_data(num_records)
            self.app.log(f"✅ Generated {len(self.app.sample_data)} records.")
            self.app.status("Sample data generated successfully.")
        except ValueError:
            self.app.log("❌ Error: Invalid number of records. Please enter an integer.")
        except Exception as e:
            self.app.log(f"❌ Error generating data: {e}")

    def _on_write(self):
        if not self.app.sample_data:
            self.app.log("⚠️ No data to write. Please generate data first.")
            return

        schema_name = self.app._entry_get(self.app.upload_schema_entry)
        compression = self.app.compression_var.get()
        self.app.log(f"Writing {len(self.app.sample_data)} records to schema '{schema_name}' with {compression} compression...")
        self.app.progress(0)

        def do_write():
            return self.app.api_client.write_polars(schema_name, self.app.sample_data, compression)

        def on_done(result):
            self.app.log(f"✅ Write successful: {result}")
            self.app.status("Data written successfully.")
            self.app.progress(1)

        def on_error(e):
            self.app.log(f"❌ Write failed: {e}")
            self.app.status("Error writing data.")
            self.app.progress(0)

        self.app.runner.run(do_write, on_done, on_error)

    def _on_read(self):
        schema_name = self.app._entry_get(self.app.read_schema_entry)
        self.app.log(f"Reading data from schema '{schema_name}'...")
        self.app.progress(0)

        def do_read():
            return self.app.api_client.read_polars(schema_name)

        def on_done(result):
            table, num_rows = result
            self.app.log(f"✅ Read successful: Fetched {num_rows} rows.")
            self.app.status(f"Data read successfully. {num_rows} rows fetched.")
            self.app.progress(1)
            # Optionally, display the data in a new window or the exploration tab
            if num_rows > 0:
                self.app.exploration_data = table.to_pydict().values()
                self.app.log("Data loaded into Exploration tab.")
                self.app._open_table_explorer()


        def on_error(e):
            self.app.log(f"❌ Read failed: {e}")
            self.app.status("Error reading data.")
            self.app.progress(0)

        self.app.runner.run(do_read, on_done, on_error)

    def _on_read_and_save(self):
        schema_name = self.app._entry_get(self.app.read_schema_entry)
        self.app.log(f"Reading data from schema '{schema_name}' for saving...")
        self.app.progress(0)

        def do_read():
            return self.app.api_client.read_polars(schema_name)

        def on_done(result):
            table, num_rows = result
            self.app.log(f"✅ Read successful: Fetched {num_rows} rows. Now saving...")
            self.app.status(f"Data read, now saving to CSV...")
            self.app.progress(0.5)
            if num_rows > 0:
                self._export_to_csv(table)
            else:
                self.app.log("No data to save.")
                self.app.status("No data to save.")
                self.app.progress(1)

        def on_error(e):
            self.app.log(f"❌ Read failed: {e}")
            self.app.status("Error reading data.")
            self.app.progress(0)

        self.app.runner.run(do_read, on_done, on_error)

    def _export_to_csv(self, table):
        """Exports a PyArrow table to a CSV file."""
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Save Data as CSV"
        )
        if not path:
            self.app.log("Save cancelled.")
            self.app.status("Save operation cancelled.")
            self.app.progress(1)
            return

        try:
            df = pl.from_arrow(table)
            df.write_csv(Path(path))
            self.app.log(f"✅ Data successfully exported to {path}")
            self.app.status("Export to CSV successful.")
        except Exception as e:
            self.app.log(f"❌ Failed to export to CSV: {e}")
            self.app.status("Error during CSV export.")
        finally:
            self.app.progress(1)

    def _on_list_schemas(self):
        self.app.log("Fetching list of schema families...")
        def do_list():
            return self.app.api_client.list_schema_families()
        def on_done(result):
            self.app.log("✅ Available schema families:")
            for name in result:
                self.app.log(f"  - {name}")
            self.app.status("Schema list fetched.")
        def on_error(e):
            self.app.log(f"❌ Failed to list schemas: {e}")
        self.app.runner.run(do_list, on_done, on_error)

    def _on_register_example(self):
        self.app.log("Registering example 'user_profile' schema...")
        example_schema = {
            "name": "user_profile",
            "fields": [
                {"name": "user_id", "type": "int"},
                {"name": "username", "type": "string"},
                {"name": "email", "type": "string"},
                {"name": "signup_date", "type": "timestamp"}
            ]
        }
        def do_register():
            return self.app.api_client.register_schema("user_profile", example_schema)
        def on_done(result):
            self.app.log(f"✅ Schema 'user_profile' registered: {result}")
            self.app.status("Example schema registered.")
        def on_error(e):
            self.app.log(f"❌ Failed to register schema: {e}")
        self.app.runner.run(do_register, on_done, on_error)

    def _on_get_latest_schema(self):
        schema_name = self.app._entry_get(self.app.schema_details_entry)
        if not schema_name:
            self.app.log("⚠️ Please enter a schema name.")
            return
        self.app.log(f"Fetching latest schema for '{schema_name}'...")
        def do_fetch():
            return self.app.api_client.get_latest_schema(schema_name)
        def on_done(result):
            self.app.log(f"✅ Latest schema for '{schema_name}':")
            formatted_schema = format_json(result)
            self.app.log(formatted_schema)
            self.app.schema_display.delete("1.0", "end")
            self.app.schema_display.insert("1.0", formatted_schema)
            self.app.status(f"Latest schema for '{schema_name}' displayed.")
        def on_error(e):
            self.app.log(f"❌ Failed to get schema: {e}")
            self.app.schema_display.delete("1.0", "end")
            self.app.schema_display.insert("1.0", f"Error: {e}")
        self.app.runner.run(do_fetch, on_done, on_error)

    def _on_get_schema_versions(self):
        schema_name = self.app._entry_get(self.app.schema_details_entry)
        if not schema_name:
            self.app.log("⚠️ Please enter a schema name.")
            return
        self.app.log(f"Fetching versions for schema '{schema_name}'...")
        def do_fetch():
            return self.app.api_client.get_schema_versions(schema_name)
        def on_done(result):
            self.app.log(f"✅ Available versions for '{schema_name}': {result}")
            self.app.status(f"Versions for '{schema_name}' fetched.")
        def on_error(e):
            self.app.log(f"❌ Failed to get schema versions: {e}")
        self.app.runner.run(do_fetch, on_done, on_error)
