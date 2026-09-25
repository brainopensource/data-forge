"""
Data Explorer Component - Refactored with centralized styling and BaseComponent inheritance
"""
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from typing import List, Any
from dataclasses import dataclass
from enum import Enum
import time

try:
    import customtkinter as ctk
    HAS_CTK = True
except ImportError:
    HAS_CTK = False

try:
    import polars as pl
except ImportError:
    pl = None

# Import our new button factory and base component
from frontend.presentation.styles.button_factory import ButtonFactory
from .base_component import BaseComponent

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
	value: Any

class DataExplorer(BaseComponent):
	"""Enhanced data exploration widget with efficient pagination for large datasets"""
	
	def __init__(self, parent, data=None, app_log=lambda x: print(x), export_callback=None, component_id=None):
		super().__init__(parent, component_id)
		self.app_log = app_log
		self.export_callback = export_callback
		self.full_data = data or []
		self.filtered_data = self.full_data.copy()
		self.columns = list(self.full_data[0].keys()) if self.full_data else []
		
		self.current_page = 1
		self.page_size = self._calculate_optimal_page_size(len(self.full_data))
		self.sort_column = None
		self.sort_ascending = True
		self.active_filters: List[ColumnFilter] = []
		
		self.search_index = {}
		self._build_search_index()
		
		# Initialize component using BaseComponent pattern
		self.initialize()
		
	def get_component_type(self) -> str:
		"""Get component type for identification"""
		return "data_explorer"
		
	def build_ui(self):
		"""Build the component's UI - required by BaseComponent"""
		self._build_explorer()
		# Register the main frame widget
		self.register_widget('main_frame', self.main_frame)
		
	def get_main_widget(self):
		"""Get the main widget for this component"""
		return getattr(self, 'main_frame', None)
	
	def _build_search_index(self):
		"""Build a simple search index for quick search"""
		start_time = time.time()
		self.search_index = {}
		for i, row in enumerate(self.full_data):
			for col, value in row.items():
				s_val = str(value).lower()
				if s_val not in self.search_index:
					self.search_index[s_val] = set()
				self.search_index[s_val].add(i)
		self._log_performance(f"Built search index in {time.time() - start_time:.4f}s")
	
	def _build_explorer(self):
		"""Build the main explorer widget"""
		self.main_frame = ctk.CTkFrame(self.parent)
		self.main_frame.pack(fill="both", expand=True)
		
		# Configure main_frame for expansion
		self.main_frame.grid_rowconfigure(2, weight=1)
		self.main_frame.grid_columnconfigure(0, weight=1)
		
		self._build_toolbar()
		self._build_filter_panel()
		
		if HAS_CTK:
			self._build_table()
		else:
			self._build_text_fallback()
			
		self._build_bottom_panel()
		self._refresh_table()
	
	def _build_toolbar(self):
		"""Build the top toolbar with search and actions"""
		toolbar = ctk.CTkFrame(self.main_frame)
		toolbar.grid(row=0, column=0, sticky="ew", padx=10, pady=5)
		
		ctk.CTkLabel(toolbar, text="🔍").pack(side="left", padx=(5, 2))
		self.search_entry = ctk.CTkEntry(toolbar, placeholder_text="Quick search...")
		self.search_entry.pack(side="left", padx=(0, 10), fill="x", expand=True)
		self.search_entry.bind("<Return>", self._on_quick_search)
		
		# Use our new button factory for consistent styling
		self.filter_toggle_btn = ButtonFactory.create_secondary_button(
			toolbar, 
			"🎚️ Filters", 
			self._toggle_filters, 
			width=100
		)
		self.filter_toggle_btn.pack(side="left", padx=5)
		
		self.export_btn = ButtonFactory.create_success_button(
			toolbar, 
			"📄 Export", 
			self._export_filtered, 
			width=100
		)
		self.export_btn.pack(side="left", padx=5)
		
		self.popup_btn = ButtonFactory.create_primary_button(
			toolbar, 
			"🪟 Pop Out", 
			self._pop_out_table, 
			width=100
		)
		self.popup_btn.pack(side="left", padx=5)
	
	def _build_filter_panel(self):
		"""Build the collapsible filter panel"""
		self.filter_panel = ctk.CTkFrame(self.main_frame)
		# Don't pack it yet, it's hidden by default
		
		self.filter_rows_container = ctk.CTkFrame(self.filter_panel)
		self.filter_rows_container.pack(fill="x", expand=True, padx=5, pady=5)
		
		filter_actions = ctk.CTkFrame(self.filter_panel)
		filter_actions.pack(fill="x", padx=10, pady=5)
		
		# Use our new button factory for consistent styling
		add_filter_btn = ButtonFactory.create_primary_button(
			filter_actions, 
			"+ Add Filter", 
			self._add_filter_row
		)
		add_filter_btn.pack(side="left")
		
		clear_all_btn = ButtonFactory.create_warning_button(
			filter_actions, 
			"🧹 Clear All", 
			self._clear_all_filters
		)
		clear_all_btn.pack(side="left", padx=10)
	
	def _build_table(self):
		"""Build the main data table using ttk.Treeview"""
		table_frame = ctk.CTkFrame(self.main_frame)
		table_frame.grid(row=2, column=0, sticky="nsew", padx=10, pady=5)
		
		self.table = ttk.Treeview(table_frame, columns=self.columns, show="headings")
		
		for col in self.columns:
			self.table.heading(col, text=col, command=lambda c=col: self._sort_by_column(c))
		
		self._calculate_optimal_column_widths()
		
		self.table.pack(side="left", fill="both", expand=True)
		
		scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.table.yview)
		self.table.configure(yscrollcommand=scrollbar.set)
		scrollbar.pack(side="right", fill="y")
		
		self.table.bind("<<TreeviewSelect>>", self._on_row_select)
	
	def _calculate_optimal_column_widths(self):
		"""Calculate and set optimal column widths"""
		for col in self.columns:
			# Simple heuristic: header width + some padding
			self.table.column(col, width=len(col) * 10 + 20, anchor="w")
	
	def _build_text_fallback(self):
		"""Build a text-based table for non-CTK environments"""
		self.text_area = ctk.CTkTextbox(self.main_frame)
		self.text_area.grid(row=2, column=0, sticky="nsew", padx=10, pady=5)
	
	def _on_row_select(self, event):
		"""Handle row selection to show details"""
		# This can be expanded to show a detail view
		pass
	
	def _build_bottom_panel(self):
		"""Build the bottom panel with pagination and stats"""
		bottom_frame = ctk.CTkFrame(self.main_frame)
		bottom_frame.grid(row=3, column=0, sticky="ew", padx=10, pady=5)
		
		self.stats_label = ctk.CTkLabel(bottom_frame, text="")
		self.stats_label.pack(side="left", padx=10)
		
		pagination_frame = ctk.CTkFrame(bottom_frame)
		pagination_frame.pack(side="right")
		
		# Use our new button factory for consistent styling and hover effects
		self.first_btn = ButtonFactory.create_secondary_button(
			pagination_frame, "«", self._first_page, width=30
		)
		self.first_btn.pack(side="left")
		
		self.prev_btn = ButtonFactory.create_secondary_button(
			pagination_frame, "‹", self._prev_page, width=30
		)
		self.prev_btn.pack(side="left")
		
		self.page_entry = ctk.CTkEntry(pagination_frame, width=50)
		self.page_entry.pack(side="left", padx=5)
		self.page_entry.bind("<Return>", self._jump_to_page)
		
		self.page_label = ctk.CTkLabel(pagination_frame, text="/ 1")
		self.page_label.pack(side="left", padx=(0, 5))
		
		self.next_btn = ButtonFactory.create_secondary_button(
			pagination_frame, "›", self._next_page, width=30
		)
		self.next_btn.pack(side="left")
		
		self.last_btn = ButtonFactory.create_secondary_button(
			pagination_frame, "»", self._last_page, width=30
		)
		self.last_btn.pack(side="left")
		
		page_size_options = self._get_smart_page_sizes()
		self.page_size_var = self._get_string_var(str(self.page_size))
		self.page_size_menu = ctk.CTkOptionMenu(pagination_frame, values=page_size_options, variable=self.page_size_var, command=self._on_page_size_change)
		self.page_size_menu.pack(side="right", padx=10)
	
	def _get_smart_page_sizes(self):
		"""Generate a list of reasonable page sizes"""
		base_sizes = [25, 50, 100, 250, 500, 1000]
		total = len(self.full_data)
		return [str(s) for s in base_sizes if s < total] + [str(total)]
	
	def _jump_to_page(self, event=None):
		"""Jump to a specific page number"""
		try:
			page = int(self._entry_get(self.page_entry))
			if 1 <= page <= self._get_total_pages():
				self.current_page = page
				self._refresh_table()
			else:
				raise ValueError()
		except (ValueError, TypeError):
			messagebox.showerror("Invalid Page", f"Please enter a page number between 1 and {self._get_total_pages()}.")
			self._entry_set(self.page_entry, str(self.current_page))
	
	def _first_page(self):
		self.current_page = 1
		self._refresh_table()
	
	def _last_page(self):
		self.current_page = self._get_total_pages()
		self._refresh_table()
	
	def _add_filter_row(self):
		"""Add a new row to the filter panel"""
		row_frame = ctk.CTkFrame(self.filter_rows_container)
		row_frame.pack(fill="x", padx=5, pady=2)
		
		column_var = self._get_string_var(self.columns[0])
		ctk.CTkOptionMenu(row_frame, values=self.columns, variable=column_var).pack(side="left", padx=2)
		
		filter_type_var = self._get_string_var(FilterType.CONTAINS.value)
		ctk.CTkOptionMenu(row_frame, values=[ft.value for ft in FilterType], variable=filter_type_var).pack(side="left", padx=2)
		
		value_entry = ctk.CTkEntry(row_frame, placeholder_text="Value")
		value_entry.pack(side="left", fill="x", expand=True, padx=2)
		
		# Use our new button factory for consistent styling
		apply_btn = ButtonFactory.create_success_button(
			row_frame, "✓", 
			lambda: self._apply_filter_row(column_var, filter_type_var, value_entry), 
			width=30
		)
		apply_btn.pack(side="left", padx=2)
		
		remove_btn = ButtonFactory.create_error_button(
			row_frame, "✗", 
			lambda: self._remove_filter_row(row_frame), 
			width=30
		)
		remove_btn.pack(side="left", padx=2)
	
	def _apply_filter_row(self, column_var, filter_type_var, value_entry):
		"""Apply a filter from a single filter row"""
		col = self._get_var_value(column_var)
		ft = FilterType(self._get_var_value(filter_type_var))
		val = self._entry_get(value_entry)
		
		new_filter = ColumnFilter(column=col, filter_type=ft, value=val)
		
		# Avoid duplicate filters
		if new_filter not in self.active_filters:
			self.active_filters.append(new_filter)
			self._apply_filters()
	
	def _remove_filter_row(self, row_frame):
		"""Remove a filter row and its corresponding filter"""
		# This is a simplified removal. A more robust implementation would
		# link the row frame to the filter object it created.
		row_frame.destroy()
		# For simplicity, we just re-apply all filters from scratch after any removal
		# A better way would be to identify which filter to remove.
		# self._rebuild_filters_from_ui()
		# self._apply_filters()
		messagebox.showinfo("Info", "Filter row removed. To update, clear all filters and re-add them.")

	def _apply_single_filter(self, data, filter_obj: ColumnFilter):
		"""Apply a single filter object to a dataset"""
		col, ft, val = filter_obj.column, filter_obj.filter_type, filter_obj.value
		
		if ft == FilterType.CONTAINS:
			return [r for r in data if val.lower() in str(r.get(col, '')).lower()]
		if ft == FilterType.EQUALS:
			return [r for r in data if str(r.get(col, '')) == val]
		if ft == FilterType.GREATER:
			return self._filter_greater(data, col, val)
		if ft == FilterType.LESS:
			return self._filter_less(data, col, val)
		if ft == FilterType.NOT_EMPTY:
			return [r for r in data if str(r.get(col, '')) != '']
		if ft == FilterType.IS_EMPTY:
			return [r for r in data if str(r.get(col, '')) == '']
		return data
	
	def _filter_greater(self, data, column, value):
		"""Filter data where column value is greater than the given value"""
		try:
			filter_val = float(value)
		except ValueError:
			# If value is not numeric, fall back to string comparison
			return [r for r in data if str(r.get(column, '')) > value]
		
		result = []
		for r in data:
			col_val = r.get(column, '')
			try:
				col_num = float(col_val)
				if col_num > filter_val:
					result.append(r)
			except (ValueError, TypeError):
				# If column value is not numeric, skip this row
				continue
		return result
	
	def _filter_less(self, data, column, value):
		"""Filter data where column value is less than the given value"""
		try:
			filter_val = float(value)
		except ValueError:
			# If value is not numeric, fall back to string comparison
			return [r for r in data if str(r.get(column, '')) < value]
		
		result = []
		for r in data:
			col_val = r.get(column, '')
			try:
				col_num = float(col_val)
				if col_num < filter_val:
					result.append(r)
			except (ValueError, TypeError):
				# If column value is not numeric, skip this row
				continue
		return result

	def _refresh_table(self):
		"""Refresh the table with data for the current page"""
		start_time = time.time()
		
		if HAS_CTK:
			self.table.delete(*self.table.get_children())
		else:
			self.text_area.delete("1.0", "end")
			
		start_index = (self.current_page - 1) * self.page_size
		end_index = start_index + self.page_size
		page_data = self.filtered_data[start_index:end_index]
		
		if HAS_CTK:
			for row in page_data:
				self.table.insert("", "end", values=[row.get(col, "") for col in self.columns])
		else:
			self._refresh_text_table()
			
		self._update_stats()
		self._log_performance(f"Refreshed table in {time.time() - start_time:.4f}s")
	
	def _apply_filters(self):
		"""Apply all active filters to the full dataset"""
		start_time = time.time()
		
		temp_data = self.full_data
		for f in self.active_filters:
			temp_data = self._apply_single_filter(temp_data, f)
			
		self.filtered_data = temp_data
		self.current_page = 1
		self._refresh_table()
		self._log_performance(f"Applied {len(self.active_filters)} filters in {time.time() - start_time:.4f}s")
	
	def _log_performance(self, message: str):
		"""Log a performance-related message"""
		self.app_log(f"[DataExplorer Perf] {message}")
	
	def _get_total_pages(self) -> int:
		"""Calculate the total number of pages"""
		if not self.filtered_data:
			return 1
		return (len(self.filtered_data) - 1) // self.page_size + 1
	
	def _update_button_state(self, button, state: str):
		"""Enable or disable a button"""
		if hasattr(button, 'configure'):
			button.configure(state=state)
	
	def _on_first_page(self):
		self.current_page = 1
		self._refresh_table()
	
	def _on_last_page(self):
		self.current_page = self._get_total_pages()
		self._refresh_table()
	
	def _on_jump_to_page(self, event=None):
		self._jump_to_page()
	
	def _calculate_optimal_page_size(self, total_rows: int) -> int:
		"""Determine a good default page size"""
		if total_rows < 100: return 50
		if total_rows < 1000: return 100
		return 250
	
	def _entry_set(self, entry, value: str):
		"""Set the value of an entry widget"""
		entry.delete(0, "end")
		entry.insert(0, value)
	
	def _refresh_text_table(self):
		"""Refresh the text-based table view"""
		header = " | ".join(self.columns)
		self.text_area.insert("end", header + "\n")
		self.text_area.insert("end", "-" * len(header) + "\n")
		
		start_index = (self.current_page - 1) * self.page_size
		end_index = start_index + self.page_size
		
		for row in self.filtered_data[start_index:end_index]:
			row_str = " | ".join(str(row.get(c, '')) for c in self.columns)
			self.text_area.insert("end", row_str + "\n")
	
	def _update_stats(self):
		"""Update the stats label with current view info"""
		total_filtered = len(self.filtered_data)
		total_full = len(self.full_data)
		
		start_index = (self.current_page - 1) * self.page_size + 1
		end_index = min(start_index + self.page_size - 1, total_filtered)
		
		if total_filtered == 0:
			stats_text = "Showing 0 of 0 records"
		else:
			stats_text = f"Showing {start_index}-{end_index} of {total_filtered:,}"
		
		if total_filtered != total_full:
			stats_text += f" (filtered from {total_full:,})"
			
		self.stats_label.configure(text=stats_text)
		
		self.page_label.configure(text=f"/ {self._get_total_pages()}")
		self._entry_set(self.page_entry, str(self.current_page))
		
		self._update_button_state(self.first_btn, "normal" if self.current_page > 1 else "disabled")
		self._update_button_state(self.prev_btn, "normal" if self.current_page > 1 else "disabled")
		self._update_button_state(self.next_btn, "normal" if self.current_page < self._get_total_pages() else "disabled")
		self._update_button_state(self.last_btn, "normal" if self.current_page < self._get_total_pages() else "disabled")
	
	def _sort_by_column(self, column):
		"""Sort the data by a specific column"""
		if self.sort_column == column:
			self.sort_ascending = not self.sort_ascending
		else:
			self.sort_column = column
			self.sort_ascending = True
			
		start_time = time.time()
		# This can be slow for large datasets. Consider using pandas/polars if available.
		self.filtered_data.sort(key=lambda x: x.get(column, ''), reverse=not self.sort_ascending)
		self._log_performance(f"Sorted data by {column} in {time.time() - start_time:.4f}s")
		
		self._refresh_table()
	
	def _on_quick_search(self, event=None):
		"""Perform a quick search across all columns"""
		query = self._entry_get(self.search_entry).lower()
		if not query:
			self.filtered_data = self.full_data
			self._apply_filters() # Re-apply persistent filters
			return
			
		start_time = time.time()
		
		# Use the pre-built index for faster search
		matching_indices = set()
		for term, indices in self.search_index.items():
			if query in term:
				matching_indices.update(indices)
		
		if matching_indices:
			self.filtered_data = [self.full_data[i] for i in sorted(list(matching_indices))]
		else:
			self.filtered_data = []
			
		self.current_page = 1
		self._refresh_table()
		self._log_performance(f"Quick search for '{query}' in {time.time() - start_time:.4f}s")
	
	def _toggle_filters(self):
		"""Show or hide the advanced filter panel"""
		if self.filter_panel.winfo_ismapped():
			self.filter_panel.grid_remove()
		else:
			self.filter_panel.grid(row=1, column=0, sticky="ew", padx=10, pady=5)
	
	def _clear_all_filters(self):
		"""Clear all active filters and reset the view"""
		self.active_filters = []
		for widget in self.filter_rows_container.winfo_children():
			widget.destroy()
		self.filtered_data = self.full_data
		self.current_page = 1
		self._refresh_table()
	
	def _prev_page(self):
		if self.current_page > 1:
			self.current_page -= 1
			self._refresh_table()
	
	def _next_page(self):
		if self.current_page < self._get_total_pages():
			self.current_page += 1
			self._refresh_table()
	
	def _on_page_size_change(self, new_size_str):
		"""Handle page size change"""
		try:
			self.page_size = int(new_size_str)
			self.current_page = 1
			self._refresh_table()
		except ValueError:
			pass # Should not happen with OptionMenu
	
	def _export_filtered(self):
		"""Export the currently filtered data"""
		if not self.filtered_data:
			messagebox.showwarning("No Data", "There is no data to export.")
			return
		
		if self.export_callback:
			self.export_callback(self.filtered_data)
		else:
			self._export_filtered_direct()

	def _export_filtered_direct(self):
		"""Directly handle export if no callback is provided"""
		if pl is None:
			messagebox.showerror("Missing Library", "Polars is not installed. Cannot export to CSV.")
			return
			
		filepath = filedialog.asksaveasfilename(
			defaultextension=".csv",
			filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
		)
		if not filepath:
			return
			
		try:
			df = pl.DataFrame(self.filtered_data)
			df.write_csv(filepath)
			messagebox.showinfo("Export Successful", f"Exported {len(self.filtered_data)} rows to {filepath}")
		except Exception as e:
			messagebox.showerror("Export Error", f"Failed to export data: {e}")
	
	def _pop_out_table(self):
		"""Pop out the table in a new window"""
		filter_info = ""
		if len(self.filtered_data) != len(self.full_data):
			filter_info = f"Showing {len(self.filtered_data):,} of {len(self.full_data):,} records (filtered)"
		else:
			filter_info = f"Showing all {len(self.full_data):,} records"
		
		FloatingDataExplorer(self.parent, self.filtered_data, filter_info)

	def _entry_get(self, entry) -> str:
		return entry.get()
	
	def _get_var_value(self, var) -> str:
		return var.get()
	
	def _get_string_var(self, value: str):
		return tk.StringVar(value=value)
	
	def _get_tk_frame(self, parent):
		return tk.Frame(parent)
	
	def update_data(self, new_data):
		"""Update the explorer with new data"""
		self.full_data = new_data or []
		self.columns = list(self.full_data[0].keys()) if self.full_data else []
		self._build_search_index()
		self._clear_all_filters() # This also refreshes the table
		
		# Re-create table headers if columns changed
		if HAS_CTK:
			self.table.configure(columns=self.columns)
			for col in self.columns:
				self.table.heading(col, text=col, command=lambda c=col: self._sort_by_column(c))
			self._calculate_optimal_column_widths()
		
		self.app_log(f"Data Explorer updated with {len(self.full_data)} new records.")

class FloatingDataExplorer:
	"""Floating data explorer with pagination support"""
	
	def __init__(self, parent_window, data, filter_info=""):
		self.parent = parent_window
		self.data = data
		self.filter_info = filter_info
		self.page_size = 50
		self.current_page = 1
		
		self.top_level = ctk.CTkToplevel(self.parent)
		self.top_level.title("Data Viewer")
		self.top_level.geometry("800x600")
		
		self._build_floating_explorer()
	
	def _build_floating_explorer(self):
		"""Build the UI for the floating explorer"""
		main_frame = ctk.CTkFrame(self.top_level)
		main_frame.pack(fill="both", expand=True, padx=10, pady=10)
		
		if self.filter_info:
			ctk.CTkLabel(main_frame, text=self.filter_info, font=("Arial", 10)).pack(anchor="w", pady=(0, 5))
			
		self._build_table(main_frame)
		self._build_pagination_controls(main_frame)
		self._load_page()
	
	def _build_table(self, parent):
		"""Build the table for displaying data"""
		table_frame = ctk.CTkFrame(parent)
		table_frame.pack(fill="both", expand=True)
		
		columns = list(self.data[0].keys()) if self.data else []
		self.table = ttk.Treeview(table_frame, columns=columns, show="headings")
		
		for col in columns:
			self.table.heading(col, text=col)
			self.table.column(col, width=100)
			
		self.table.pack(side="left", fill="both", expand=True)
		
		scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.table.yview)
		self.table.configure(yscrollcommand=scrollbar.set)
		scrollbar.pack(side="right", fill="y")
	
	def _build_pagination_controls(self, parent):
		"""Build the pagination controls"""
		controls_frame = ctk.CTkFrame(parent)
		controls_frame.pack(fill="x", pady=5)
		
		self.first_btn = ctk.CTkButton(controls_frame, text="«", command=self._first_page, width=30)
		self.first_btn.pack(side="left", padx=2)
		self.prev_btn = ctk.CTkButton(controls_frame, text="‹", command=self._prev_page, width=30)
		self.prev_btn.pack(side="left", padx=2)
		
		self.pagination_info = ctk.CTkLabel(controls_frame, text="")
		self.pagination_info.pack(side="left", padx=10)
		
		self.next_btn = ctk.CTkButton(controls_frame, text="›", command=self._next_page, width=30)
		self.next_btn.pack(side="left", padx=2)
		self.last_btn = ctk.CTkButton(controls_frame, text="»", command=self._last_page, width=30)
		self.last_btn.pack(side="left", padx=2)
		
		page_size_var = tk.StringVar(value=str(self.page_size))
		ctk.CTkOptionMenu(
			controls_frame,
			values=["25", "50", "100", "200"],
			variable=page_size_var,
			command=self._on_page_size_change
		).pack(side="right", padx=10)
	
	def _load_page(self):
		"""Load data for the current page into the table"""
		self.table.delete(*self.table.get_children())
		
		start = (self.current_page - 1) * self.page_size
		end = start + self.page_size
		page_data = self.data[start:end]
		
		for row in page_data:
			self.table.insert("", "end", values=list(row.values()))
			
		self._update_pagination_info()
		self._update_navigation_buttons()
	
	def _update_pagination_info(self):
		"""Update the pagination info label"""
		total_pages = (len(self.data) - 1) // self.page_size + 1
		self.pagination_info.configure(text=f"Page {self.current_page} of {total_pages}")
	
	def _update_navigation_buttons(self):
		"""Update the state of navigation buttons"""
		total_pages = (len(self.data) - 1) // self.page_size + 1
		self.first_btn.configure(state="normal" if self.current_page > 1 else "disabled")
		self.prev_btn.configure(state="normal" if self.current_page > 1 else "disabled")
		self.next_btn.configure(state="normal" if self.current_page < total_pages else "disabled")
		self.last_btn.configure(state="normal" if self.current_page < total_pages else "disabled")
	
	def _first_page(self):
		self.current_page = 1
		self._load_page()
	
	def _prev_page(self):
		if self.current_page > 1:
			self.current_page -= 1
			self._load_page()
	
	def _next_page(self):
		total_pages = (len(self.data) - 1) // self.page_size + 1
		if self.current_page < total_pages:
			self.current_page += 1
			self._load_page()
	
	def _last_page(self):
		self.current_page = (len(self.data) - 1) // self.page_size + 1
		self._load_page()
	
	def _on_page_size_change(self, new_size_str):
		"""Handle page size change"""
		self.page_size = int(new_size_str)
		self.current_page = 1
		self._load_page()

if __name__ == '__main__':
    # Example usage
    root = ctk.CTk()
    root.title("Data Explorer Demo")
    root.geometry("1200x800")

    sample_data = [
        {'id': i, 'product': f'Product-{i%10}', 'price': 100 + i, 'in_stock': (i%2==0)}
        for i in range(500)
    ]

    def on_export(data_to_export):
        print(f"Exporting {len(data_to_export)} records.")
        # In a real app, this would open a file dialog and save.
        messagebox.showinfo("Export", f"Would export {len(data_to_export)} records.")

    data_explorer = DataExplorer(root, data=sample_data, export_callback=on_export)
    
    def open_floating():
        FloatingDataExplorer(root, sample_data, "Showing all sample data")

    ctk.CTkButton(root, text="Open Floating Viewer", command=open_floating).pack(pady=10)

    root.mainloop()
