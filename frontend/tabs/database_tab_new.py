"""
Database Tab for DataForge Application
"""
from .base_tab import BaseTab
from tkinter import filedialog, messagebox


class DatabaseTab(BaseTab):
    """Database tab for data management operations"""
    
    def build_content(self, parent):
        """Build the database tab content"""
        self.content_frame = self.ui_adapter.create_frame(parent)
        self.content_frame.pack(fill="both", expand=True)
        
        # Header
        header_frame = self.ui_adapter.create_frame(self.content_frame)
        header_frame.pack(fill="x", pady=20)
        
        self.ui_adapter.create_label(
            header_frame,
            text="🗄️ Database Management",
            font_size=20,
            font_weight="bold"
        ).pack()
        
        self.ui_adapter.create_label(
            header_frame,
            text="Manage data operations and schema configurations",
            font_size=12
        ).pack(pady=(5, 0))
        
        # Operations section
        ops_frame = self.ui_adapter.create_frame(self.content_frame)
        ops_frame.pack(fill="x", pady=20, padx=50)
        
        self.ui_adapter.create_label(
            ops_frame,
            text="Data Operations",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        # Operation buttons
        buttons_frame = self.ui_adapter.create_frame(ops_frame)
        buttons_frame.pack(fill="x")
        
        # Row 1: Generate and Write
        row1 = self.ui_adapter.create_frame(buttons_frame)
        row1.pack(fill="x", pady=5)
        
        self.ui_adapter.create_button(
            row1,
            text="🎲 Generate Sample Data",
            command=self._generate_sample_data,
            width=200,
            height=40
        ).pack(side="left", padx=(0, 10))
        
        self.ui_adapter.create_button(
            row1,
            text="💾 Write Data",
            command=self._write_data,
            width=200,
            height=40
        ).pack(side="left", padx=(0, 10))
        
        # Row 2: Read and Export
        row2 = self.ui_adapter.create_frame(buttons_frame)
        row2.pack(fill="x", pady=5)
        
        self.ui_adapter.create_button(
            row2,
            text="📖 Read Data",
            command=self._read_data,
            width=200,
            height=40
        ).pack(side="left", padx=(0, 10))
        
        self.ui_adapter.create_button(
            row2,
            text="📤 Export Data",
            command=self._export_data,
            width=200,
            height=40
        ).pack(side="left", padx=(0, 10))
        
        # Schema section
        schema_frame = self.ui_adapter.create_frame(self.content_frame)
        schema_frame.pack(fill="x", pady=20, padx=50)
        
        self.ui_adapter.create_label(
            schema_frame,
            text="Schema Management",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        # Schema buttons
        schema_buttons = self.ui_adapter.create_frame(schema_frame)
        schema_buttons.pack(fill="x")
        
        self.ui_adapter.create_button(
            schema_buttons,
            text="📋 List Schemas",
            command=self._list_schemas,
            width=150,
            height=35
        ).pack(side="left", padx=(0, 10))
        
        self.ui_adapter.create_button(
            schema_buttons,
            text="🔍 Get Schema Details",
            command=self._get_schema_details,
            width=150,
            height=35
        ).pack(side="left", padx=(0, 10))
        
        self.ui_adapter.create_button(
            schema_buttons,
            text="📝 Register Schema",
            command=self._register_schema,
            width=150,
            height=35
        ).pack(side="left", padx=(0, 10))
        
        # Configuration section
        config_frame = self.ui_adapter.create_frame(self.content_frame)
        config_frame.pack(fill="x", pady=20, padx=50)
        
        self.ui_adapter.create_label(
            config_frame,
            text="Configuration",
            font_size=16,
            font_weight="bold"
        ).pack(anchor="w", pady=(0, 10))
        
        # Basic configuration inputs
        config_inputs = self.ui_adapter.create_frame(config_frame)
        config_inputs.pack(fill="x")
        
        # Schema name
        schema_row = self.ui_adapter.create_frame(config_inputs)
        schema_row.pack(fill="x", pady=5)
        
        self.ui_adapter.create_label(
            schema_row,
            text="Schema:",
            font_weight="bold",
            width=100
        ).pack(side="left")
        
        self.schema_entry = self.ui_adapter.create_entry(
            schema_row,
            placeholder_text="well_production",
            width=200
        )
        self.schema_entry.pack(side="left", padx=(10, 0))
        self.schema_entry.insert(0, "well_production")
        
        # Records count
        records_row = self.ui_adapter.create_frame(config_inputs)
        records_row.pack(fill="x", pady=5)
        
        self.ui_adapter.create_label(
            records_row,
            text="Records:",
            font_weight="bold",
            width=100
        ).pack(side="left")
        
        self.records_entry = self.ui_adapter.create_entry(
            records_row,
            placeholder_text="10000",
            width=200
        )
        self.records_entry.pack(side="left", padx=(10, 0))
        self.records_entry.insert(0, "10000")
    
    def _generate_sample_data(self):
        """Generate sample data"""
        try:
            records_count = int(self.records_entry.get() or "10000")
            self._log(f"Generating {records_count} sample records...")
            
            def generate():
                return self.app.get_data_generator().generate_sample_data(records_count)
            
            def on_done(data):
                self.app.set_exploration_data(data)
                self._log(f"✅ Generated {len(data)} sample records")
                messagebox.showinfo("Success", f"Generated {len(data)} sample records")
            
            def on_error(error):
                self._log(f"❌ Error generating data: {error}")
                messagebox.showerror("Error", f"Failed to generate data: {error}")
            
            self.app.get_async_runner().run(generate, on_done, on_error)
            
        except ValueError:
            messagebox.showerror("Error", "Please enter a valid number of records")
    
    def _write_data(self):
        """Write data to backend"""
        data = self.app.get_exploration_data()
        if not data:
            messagebox.showwarning("Warning", "No data to write. Generate or load data first.")
            return
        
        schema_name = self.schema_entry.get() or "well_production"
        
        def write():
            return self.app.get_api_client().write_polars(schema_name, data)
        
        def on_done(result):
            self._log(f"✅ Data written successfully: {result}")
            messagebox.showinfo("Success", f"Data written to {schema_name}")
        
        def on_error(error):
            self._log(f"❌ Error writing data: {error}")
            messagebox.showerror("Error", f"Failed to write data: {error}")
        
        self._log(f"Writing {len(data)} records to {schema_name}...")
        self.app.get_async_runner().run(write, on_done, on_error)
    
    def _read_data(self):
        """Read data from backend"""
        schema_name = self.schema_entry.get() or "well_production"
        
        def read():
            table, count = self.app.get_api_client().read_polars(schema_name)
            # Convert Arrow table to list of dicts for compatibility
            df = table.to_pandas()
            return df.to_dict('records'), count
        
        def on_done(result):
            data, count = result
            self.app.set_exploration_data(data)
            self._log(f"✅ Read {count} records from {schema_name}")
            messagebox.showinfo("Success", f"Loaded {count} records from {schema_name}")
        
        def on_error(error):
            self._log(f"❌ Error reading data: {error}")
            messagebox.showerror("Error", f"Failed to read data: {error}")
        
        self._log(f"Reading data from {schema_name}...")
        self.app.get_async_runner().run(read, on_done, on_error)
    
    def _export_data(self):
        """Export current data to file"""
        data = self.app.get_exploration_data()
        if not data:
            messagebox.showwarning("Warning", "No data to export. Load data first.")
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("JSON files", "*.json"), ("All files", "*.*")]
        )
        
        if filename:
            def export():
                import csv
                import json
                from pathlib import Path
                
                path = Path(filename)
                if path.suffix.lower() == '.json':
                    with open(path, 'w') as f:
                        json.dump(data, f, indent=2)
                else:
                    # Default to CSV
                    if data:
                        fieldnames = data[0].keys()
                        with open(path, 'w', newline='') as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(data)
                return str(path)
            
            def on_done(filepath):
                self._log(f"✅ Data exported to {filepath}")
                messagebox.showinfo("Success", f"Data exported to {filepath}")
            
            def on_error(error):
                self._log(f"❌ Error exporting data: {error}")
                messagebox.showerror("Error", f"Failed to export data: {error}")
            
            self.app.get_async_runner().run(export, on_done, on_error)
    
    def _list_schemas(self):
        """List available schemas"""
        def list_schemas():
            return self.app.get_api_client().list_schema_families()
        
        def on_done(schemas):
            schema_list = "\n".join(f"• {schema}" for schema in schemas)
            self._log(f"✅ Found {len(schemas)} schemas")
            messagebox.showinfo("Schemas", f"Available schemas:\n\n{schema_list}")
        
        def on_error(error):
            self._log(f"❌ Error listing schemas: {error}")
            messagebox.showerror("Error", f"Failed to list schemas: {error}")
        
        self._log("Listing schemas...")
        self.app.get_async_runner().run(list_schemas, on_done, on_error)
    
    def _get_schema_details(self):
        """Get schema details"""
        schema_name = self.schema_entry.get() or "well_production"
        
        def get_schema():
            return self.app.get_api_client().get_latest_schema(schema_name)
        
        def on_done(schema):
            from frontend.utils.json_utils import format_json
            formatted = format_json(schema)
            self._log(f"✅ Retrieved schema for {schema_name}")
            messagebox.showinfo(f"Schema: {schema_name}", formatted)
        
        def on_error(error):
            self._log(f"❌ Error getting schema: {error}")
            messagebox.showerror("Error", f"Failed to get schema: {error}")
        
        self._log(f"Getting schema details for {schema_name}...")
        self.app.get_async_runner().run(get_schema, on_done, on_error)
    
    def _register_schema(self):
        """Register a new schema"""
        messagebox.showinfo("Info", "Schema registration functionality would be implemented here")
        self._log("Schema registration not yet implemented")
