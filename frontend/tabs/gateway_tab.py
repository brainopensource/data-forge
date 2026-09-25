"""
Gateway Tab for DataForge - Fetching data from external sources.
"""
from tkinter import filedialog, messagebox
from pathlib import Path
import requests
import json

try:
    import customtkinter as ctk
    HAS_CTK = True
except ImportError:
    HAS_CTK = False

try:
    import polars as pl
except ImportError:
    pl = None

class GatewayTab:
    def __init__(self, parent, app):
        self.parent = parent
        self.app = app
        self.fetched_data_content = None
        self.main_frame = ctk.CTkFrame(self.parent, fg_color="transparent")
        self.build()

    def build(self):
        """Builds the Gateway tab UI."""
        gateway_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        gateway_frame.pack(fill="both", expand=True, padx=20, pady=10)

        ctk.CTkLabel(gateway_frame, text="External Data Gateway", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=10)

        # --- Fetch Controls ---
        fetch_frame = ctk.CTkFrame(gateway_frame)
        fetch_frame.pack(fill="x", pady=5)

        ctk.CTkLabel(fetch_frame, text="URL:").pack(side="left", padx=10)
        self.app.external_url_entry = ctk.CTkEntry(fetch_frame, placeholder_text="Enter API endpoint URL")
        self.app.external_url_entry.pack(side="left", fill="x", expand=True, padx=5)

        ctk.CTkButton(fetch_frame, text="Fetch Data", command=self._on_external_fetch).pack(side="left", padx=10)

        # --- Options ---
        options_frame = ctk.CTkFrame(gateway_frame)
        options_frame.pack(fill="x", pady=5)
        
        ctk.CTkLabel(options_frame, text="Fetch Type:").pack(side="left", padx=10)
        self.app.fetch_type_var = self.app._ctk_string("JSON")
        self.app.fetch_type_menu = ctk.CTkOptionMenu(
            options_frame,
            values=["JSON", "OData (JSON)", "CSV"],
            variable=self.app.fetch_type_var,
            command=self._on_fetch_type_change
        )
        self.app.fetch_type_menu.pack(side="left", padx=5)

        self.app.save_fetched_button = ctk.CTkButton(options_frame, text="Save Fetched Data", command=self._on_save_fetched_data, state="disabled")
        self.app.save_fetched_button.pack(side="right", padx=10)

        # --- Response Display ---
        self.app.fetch_display = ctk.CTkTextbox(gateway_frame, height=250)
        self.app.fetch_display.pack(fill="both", expand=True, pady=10)

    def _on_fetch_type_change(self, selected_type: str):
        self.app.log(f"Fetch type changed to: {selected_type}")

    def _on_external_fetch(self):
        url = self.app._entry_get(self.app.external_url_entry)
        if not url:
            self.app.log("⚠️ Please enter a URL to fetch from.")
            return

        fetch_type = self.app.fetch_type_var.get()
        self.app.log(f"Fetching data from {url} as {fetch_type}...")
        self.app.progress(0)

        def do_fetch():
            headers = {'Accept': 'application/json'} if 'JSON' in fetch_type else {'Accept': 'text/csv'}
            response = requests.get(url, headers=headers, timeout=120)
            response.raise_for_status()
            return response.text

        def on_done(content):
            self.app.log("✅ Fetch successful.")
            self.app.progress(1)
            self.fetched_data_content = content
            self.app.fetch_display.delete("1.0", "end")
            
            if "JSON" in fetch_type:
                try:
                    # Pretty print JSON
                    parsed_json = json.loads(content)
                    self.app.fetch_display.insert("1.0", json.dumps(parsed_json, indent=2))
                except json.JSONDecodeError:
                    self.app.fetch_display.insert("1.0", content) # Show raw text if not valid JSON
            else:
                self.app.fetch_display.insert("1.0", content)

            self.app.save_fetched_button.configure(state="normal")
            self._offer_save_fetched_data()

        def on_error(e):
            self.app.log(f"❌ Fetch failed: {e}")
            self.app.status(f"Error fetching data: {e}")
            self.app.progress(0)
            self.app.fetch_display.delete("1.0", "end")
            self.app.fetch_display.insert("1.0", f"Error: {e}")
            self.app.save_fetched_button.configure(state="disabled")

        self.app.runner.run(do_fetch, on_done, on_error)

    def _on_save_fetched_data(self):
        if not self.fetched_data_content:
            messagebox.showwarning("No Data", "No data has been fetched yet.")
            return

        fetch_type = self.app.fetch_type_var.get()
        file_ext = ".json" if "JSON" in fetch_type else ".csv"
        file_types = [("JSON files", "*.json"), ("All files", "*.*")] if file_ext == ".json" else [("CSV files", "*.csv"), ("All files", "*.*")]

        filepath = filedialog.asksaveasfilename(
            defaultextension=file_ext,
            filetypes=file_types,
            title=f"Save Fetched {fetch_type} Data"
        )
        if not filepath:
            return

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(self.fetched_data_content)
            self.app.log(f"✅ Fetched data saved to {filepath}")
            messagebox.showinfo("Save Successful", f"Data saved to {filepath}")
        except Exception as e:
            self.app.log(f"❌ Failed to save fetched data: {e}")
            messagebox.showerror("Save Error", f"Could not save file: {e}")

    def _offer_save_fetched_data(self):
        """Asks user if they want to save the data or load it into exploration."""
        res = messagebox.askyesnocancel("Data Fetched", 
                                        "Data fetched successfully. \n"
                                        "YES - Load data into the Exploration tab.\n"
                                        "NO - Just view the data here.\n"
                                        "CANCEL - Do nothing.")
        if res is True: # Yes
            self._load_fetched_data_for_exploration()
        elif res is False: # No
            pass # Do nothing
        else: # Cancel
            pass

    def _load_fetched_data_for_exploration(self):
        if not self.fetched_data_content:
            return
        
        if pl is None:
            messagebox.showerror("Missing Library", "Polars is not installed. Cannot process data for exploration.")
            return

        try:
            fetch_type = self.app.fetch_type_var.get()
            if "JSON" in fetch_type:
                # Polars can read JSON directly
                df = pl.read_json(self.fetched_data_content)
            else: # CSV
                df = pl.read_csv(self.fetched_data_content.encode('utf-8'))
            
            self.app.exploration_data = df.to_dicts()
            self.app.log(f"✅ Loaded {len(self.app.exploration_data)} records into Exploration tab.")
            
            # Switch to exploration tab and open table viewer
            self.app._show_tab('exploration')
            self.app.exploration_tab._continue_to_table_explorer()

        except Exception as e:
            self.app.log(f"❌ Failed to parse fetched data for exploration: {e}")
            messagebox.showerror("Parsing Error", f"Could not parse the fetched data: {e}")
