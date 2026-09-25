"""
Logs Tab for DataForge Frontend
Displays application logs in a dedicated tab for better space utilization.
"""

from datetime import datetime
import customtkinter as ctk
from typing import Any


class LogsTab:
    """Logs tab that displays application logs in a dedicated interface."""

    def __init__(self, parent: Any, app_instance: Any):
        """Initialize the logs tab.

        Args:
            parent: Parent widget
            app_instance: Reference to the main application instance
        """
        self.parent = parent
        self.app = app_instance

        # Create main container (don't pack it - let the main app manage layout)
        self.main_frame = ctk.CTkFrame(parent)

        # Configure main frame to expand
        self.main_frame.grid_rowconfigure(0, weight=1)
        self.main_frame.grid_columnconfigure(0, weight=1)

        # Build the logs interface
        self._build_logs_interface()

    def _build_logs_interface(self):
        """Build the logs display interface."""
        # Header section
        header_frame = ctk.CTkFrame(self.main_frame)
        header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))

        # Title and description
        title_label = ctk.CTkLabel(
            header_frame,
            text="📋 Application Logs",
            font=ctk.CTkFont(size=24, weight="bold") if hasattr(ctk, 'CTkFont') else ("Arial", 18, "bold")
        )
        title_label.pack(pady=(20, 10))

        desc_label = ctk.CTkLabel(
            header_frame,
            text="View all application activity and system messages",
            font=ctk.CTkFont(size=14) if hasattr(ctk, 'CTkFont') else ("Arial", 12),
            text_color="#888888"
        )
        desc_label.pack(pady=(0, 20))

        # Control buttons
        controls_frame = ctk.CTkFrame(header_frame)
        controls_frame.pack(fill="x", padx=20, pady=(0, 20))

        # Clear logs button
        clear_btn = ctk.CTkButton(
            controls_frame,
            text="🗑️ Clear Logs",
            command=self._clear_logs,
            width=120,
            height=35
        )
        clear_btn.pack(side="left", padx=(0, 10))

        # Auto-scroll toggle
        self.auto_scroll_var = self._ctk_boolean(True)
        auto_scroll_cb = ctk.CTkCheckBox(
            controls_frame,
            text="Auto-scroll to bottom",
            variable=self.auto_scroll_var
        )
        auto_scroll_cb.pack(side="left", padx=(20, 0))

        # Log statistics
        stats_frame = ctk.CTkFrame(controls_frame)
        stats_frame.pack(side="right")

        self.log_count_label = ctk.CTkLabel(
            stats_frame,
            text="Total logs: 0",
            font=ctk.CTkFont(size=12) if hasattr(ctk, 'CTkFont') else ("Arial", 10),
            text_color="#888888"
        )
        self.log_count_label.pack(pady=5)

        # Main logs display area - this should expand
        logs_frame = ctk.CTkFrame(self.main_frame)
        logs_frame.grid(row=1, column=0, sticky="nsew", padx=20, pady=(0, 20))

        # Configure logs_frame to expand
        logs_frame.grid_rowconfigure(0, weight=1)
        logs_frame.grid_columnconfigure(0, weight=1)

        # Create scrollable text area for logs
        self.logs_textbox = ctk.CTkTextbox(
            logs_frame,
            wrap="word",
            font=ctk.CTkFont(size=11) if hasattr(ctk, 'CTkFont') else ("Courier", 10)
        )
        self.logs_textbox.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)

        # Bottom info section
        info_frame = ctk.CTkFrame(self.main_frame)
        info_frame.grid(row=2, column=0, sticky="ew", pady=(0, 20))

        info_label = ctk.CTkLabel(
            info_frame,
            text="💡 Tip: Logs are also printed to the console for debugging purposes",
            font=ctk.CTkFont(size=12) if hasattr(ctk, 'CTkFont') else ("Arial", 10),
            text_color="#666666"
        )
        info_label.pack(pady=10)

    def _clear_logs(self):
        """Clear all logs from the display."""
        self.logs_textbox.configure(state="normal")
        self.logs_textbox.delete("1.0", "end")
        self.logs_textbox.configure(state="disabled")
        self._update_log_count()

        # Also clear the main app's log storage if it exists
        if hasattr(self.app, 'log_messages'):
            self.app.log_messages.clear()

    def add_log_message(self, message: str):
        """Add a log message to the display.

        Args:
            message: The log message to add
        """
        try:
            # Format the message with timestamp
            timestamp = datetime.now().strftime("%H:%M:%S")
            formatted_message = f"[{timestamp}] {message}\n"

            # Add to text box
            self.logs_textbox.configure(state="normal")
            self.logs_textbox.insert("end", formatted_message)
            self.logs_textbox.configure(state="disabled")

            # Auto-scroll if enabled
            if hasattr(self, 'auto_scroll_var') and self.auto_scroll_var.get():
                self.logs_textbox.see("end")

            # Update log count
            self._update_log_count()
        except Exception as e:
            # Fallback: print to console if UI update fails
            print(f"[LogsTab Error] {e}")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    def _update_log_count(self):
        """Update the log count display."""
        try:
            # Count lines in the text box
            content = self.logs_textbox.get("1.0", "end-1c")
            line_count = len([line for line in content.split('\n') if line.strip()])

            self.log_count_label.configure(text=f"Total logs: {line_count}")
        except Exception:
            self.log_count_label.configure(text="Total logs: --")

    def _ctk_boolean(self, value: bool):
        """Create a boolean variable compatible with the current UI framework."""
        try:
            import customtkinter as ctk
            return ctk.BooleanVar(value=value)
        except Exception:
            # Fallback for basic tkinter
            import tkinter as tk
            return tk.BooleanVar(value=value)
