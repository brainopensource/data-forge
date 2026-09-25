"""Simple validation result popup components."""

from typing import Dict, Any, List
from frontend.services.ui_framework_adapter import UIFrameworkAdapter


class ValidationResultPopup:
    """Simple popup for displaying validation results."""
    
    def __init__(self, parent, ui_adapter: UIFrameworkAdapter):
        self.parent = parent
        self.ui_adapter = ui_adapter
    
    def show_validation_results(self, results: Dict[str, Any]):
        """Show validation results in a popup."""
        popup = self._create_popup("Data Validation Results", "600x450")
        
        if not results.get("success", False):
            self._show_error_popup(popup, results.get("message", "Validation failed"))
            return
        
        # Header
        header_text = f"Quality Score: {results.get('quality_score', 0):.1f}%"
        if results.get('quality_score', 0) >= 80:
            header_text = f"✅ {header_text} (Excellent)"
        elif results.get('quality_score', 0) >= 60:
            header_text = f"⚠️ {header_text} (Good)"
        else:
            header_text = f"❌ {header_text} (Needs Improvement)"
        
        header_label = self.ui_adapter.create_label(
            popup,
            text=header_text,
            font=("Arial", 14, "bold")
        )
        header_label.pack(pady=10)
        
        # Summary
        summary_text = f"Analyzed {results.get('total_rows', 0)} rows\n"
        summary_text += f"Found {results.get('errors_count', 0)} errors and {results.get('warnings_count', 0)} warnings"
        
        summary_label = self.ui_adapter.create_label(popup, text=summary_text)
        summary_label.pack(pady=5)
        
        # Issues (if any)
        issues = results.get('issues', [])
        if issues:
            issues_label = self.ui_adapter.create_label(
                popup,
                text="Key Issues Found:",
                font=("Arial", 11, "bold")
            )
            issues_label.pack(pady=(10, 5))
            
            scroll_frame = self.ui_adapter.create_scrollable_frame(popup)
            scroll_frame.pack(fill="both", expand=True, padx=10, pady=5)
            
            for issue in issues:
                severity_icon = "🔴" if issue.get('severity') == 'error' else "🟡"
                issue_text = f"{severity_icon} Row {issue.get('row', 0)}: {issue.get('message', '')}"
                if issue.get('suggested_fix'):
                    issue_text += f"\n   💡 {issue.get('suggested_fix')}"
                
                issue_label = self.ui_adapter.create_label(
                    scroll_frame,
                    text=issue_text,
                    justify="left"
                )
                issue_label.pack(anchor="w", pady=2)
        
        # Close button
        self._add_close_button(popup)
    
    def show_cleaning_results(self, results: Dict[str, Any]):
        """Show data cleaning results in a popup."""
        popup = self._create_popup("Data Cleaning Results", "500x300")
        
        if not results.get("success", False):
            self._show_error_popup(popup, results.get("message", "Cleaning failed"))
            return
        
        # Success header
        header_label = self.ui_adapter.create_label(
            popup,
            text="🧹 Data Cleaning Complete",
            font=("Arial", 14, "bold")
        )
        header_label.pack(pady=10)
        
        # Statistics
        stats_text = f"Original rows: {results.get('original_rows', 0)}\n"
        stats_text += f"Cleaned rows: {results.get('cleaned_rows', 0)}\n"
        
        rows_removed = results.get('rows_removed', 0)
        if rows_removed > 0:
            stats_text += f"Rows removed: {rows_removed}"
        else:
            stats_text += "No rows removed"
        
        stats_label = self.ui_adapter.create_label(popup, text=stats_text)
        stats_label.pack(pady=10)
        
        message_label = self.ui_adapter.create_label(
            popup,
            text=results.get("message", "Cleaning completed successfully"),
            font=("Arial", 10)
        )
        message_label.pack(pady=5)
        
        # Close button
        self._add_close_button(popup)
    
    def show_quality_report(self, results: Dict[str, Any]):
        """Show quality report in a popup."""
        popup = self._create_popup("Data Quality Report", "700x500")
        
        if not results.get("success", False):
            self._show_error_popup(popup, results.get("message", "Report generation failed"))
            return
        
        report = results.get("report", {})
        
        # Header
        header_label = self.ui_adapter.create_label(
            popup,
            text="📊 Data Quality Report",
            font=("Arial", 16, "bold")
        )
        header_label.pack(pady=10)
        
        # Create scrollable content
        scroll_frame = self.ui_adapter.create_scrollable_frame(popup)
        scroll_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Build report text
        report_text = f"🔍 Overall Quality Score: {report.get('quality_score', 0):.1f}%\n\n"
        
        report_text += "📏 Dataset Overview:\n"
        report_text += f"• Total Rows: {report.get('total_rows', 0):,}\n"
        report_text += f"• Total Columns: {report.get('total_columns', 0)}\n"
        report_text += f"• Empty Rows: {report.get('empty_rows', 0)}\n"
        report_text += f"• Duplicate Rows: {report.get('duplicate_rows', 0)}\n\n"
        
        # Column completeness
        if report.get('columns_with_nulls'):
            report_text += "❌ Columns with Missing Values:\n"
            for col, null_count in report['columns_with_nulls'].items():
                percentage = (null_count / report.get('total_rows', 1)) * 100
                report_text += f"• {col}: {null_count} missing ({percentage:.1f}%)\n"
            report_text += "\n"
        
        # Recommendations
        if report.get('recommendations'):
            report_text += "💡 Recommendations:\n"
            for i, rec in enumerate(report['recommendations'], 1):
                report_text += f"{i}. {rec}\n"
        
        report_label = self.ui_adapter.create_label(
            scroll_frame,
            text=report_text,
            justify="left",
            font=("Courier", 9)
        )
        report_label.pack(anchor="w", padx=5, pady=5)
        
        # Close button
        self._add_close_button(popup)
    
    def show_recommendations(self, results: Dict[str, Any]):
        """Show recommendations in a popup."""
        popup = self._create_popup("Data Quality Recommendations", "600x400")
        
        if not results.get("success", False):
            self._show_error_popup(popup, results.get("message", "Recommendations generation failed"))
            return
        
        recommendations = results.get("recommendations", [])
        
        # Header
        header_text = f"💡 {results.get('total_count', 0)} Recommendations"
        if results.get('high_priority_count', 0) > 0:
            header_text += f" ({results.get('high_priority_count', 0)} high priority)"
        
        header_label = self.ui_adapter.create_label(
            popup,
            text=header_text,
            font=("Arial", 14, "bold")
        )
        header_label.pack(pady=10)
        
        if not recommendations:
            no_rec_label = self.ui_adapter.create_label(
                popup,
                text="✅ No recommendations needed. Your data quality is excellent!",
                font=("Arial", 11)
            )
            no_rec_label.pack(pady=20)
        else:
            # Scrollable recommendations list
            scroll_frame = self.ui_adapter.create_scrollable_frame(popup)
            scroll_frame.pack(fill="both", expand=True, padx=10, pady=5)
            
            for i, rec in enumerate(recommendations, 1):
                priority_icon = "🔴" if rec.get('priority') == 'high' else "🟡"
                rec_text = f"{priority_icon} {i}. {rec.get('text', '')}"
                rec_text += f"\n   📂 Category: {rec.get('category', 'general').title()}"
                
                rec_label = self.ui_adapter.create_label(
                    scroll_frame,
                    text=rec_text,
                    justify="left"
                )
                rec_label.pack(anchor="w", pady=3)
        
        # Close button
        self._add_close_button(popup)
    
    def show_validation_and_cleaning_results(self, results: Dict[str, Any]):
        """Show combined validation and cleaning results."""
        popup = self._create_popup("Validation & Cleaning Results", "700x500")
        
        if not results.get("success", False):
            self._show_error_popup(popup, results.get("message", "Process failed"))
            return
        
        # Header
        header_label = self.ui_adapter.create_label(
            popup,
            text="🔍🧹 Validation & Cleaning Complete",
            font=("Arial", 16, "bold")
        )
        header_label.pack(pady=10)
        
        # Create scrollable content
        scroll_frame = self.ui_adapter.create_scrollable_frame(popup)
        scroll_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Build results text
        text = ""
        
        # Initial validation
        initial_val = results.get("initial_validation", {})
        if initial_val:
            text += f"📊 Initial Quality Score: {initial_val.get('quality_score', 0):.1f}%\n"
            text += f"• Errors: {initial_val.get('errors_count', 0)}\n"
            text += f"• Warnings: {initial_val.get('warnings_count', 0)}\n\n"
        
        # Cleaning results
        cleaning = results.get("cleaning_result", {})
        if cleaning:
            text += "🧹 Cleaning Process:\n"
            text += f"• Original rows: {cleaning.get('original_rows', 0)}\n"
            text += f"• Final rows: {cleaning.get('cleaned_rows', 0)}\n"
            if cleaning.get('rows_removed', 0) > 0:
                text += f"• Rows removed: {cleaning.get('rows_removed', 0)}\n"
            text += "\n"
        
        # Final validation
        final_val = results.get("final_validation", {})
        if final_val:
            text += f"✅ Final Quality Score: {final_val.get('quality_score', 0):.1f}%\n"
            text += f"• Errors: {final_val.get('errors_count', 0)}\n"
            text += f"• Warnings: {final_val.get('warnings_count', 0)}\n\n"
            
            # Improvement calculation
            if initial_val and final_val:
                improvement = final_val.get('quality_score', 0) - initial_val.get('quality_score', 0)
                if improvement > 0:
                    text += f"📈 Quality Improvement: +{improvement:.1f}%\n"
                elif improvement < 0:
                    text += f"📉 Quality Change: {improvement:.1f}%\n"
                else:
                    text += "➡️ Quality maintained\n"
        
        text += f"\n{results.get('message', 'Process completed successfully')}"
        
        results_label = self.ui_adapter.create_label(
            scroll_frame,
            text=text,
            justify="left",
            font=("Courier", 10)
        )
        results_label.pack(anchor="w", padx=5, pady=5)
        
        # Close button
        self._add_close_button(popup)
    
    def _create_popup(self, title: str, geometry: str):
        """Create a basic popup window."""
        # Use tkinter directly for popup windows since UI adapter may not support toplevel
        import tkinter as tk
        popup = tk.Toplevel(self.parent)
        popup.title(title)
        popup.geometry(geometry)
        
        # Basic styling for dark theme
        popup.configure(bg='#2b2b2b')
        
        # Make popup modal
        popup.transient(self.parent)
        popup.grab_set()
        
        return popup
    
    def _show_error_popup(self, popup, message: str):
        """Show error message in popup."""
        error_label = self.ui_adapter.create_label(
            popup,
            text=f"❌ {message}",
            font=("Arial", 12)
        )
        error_label.pack(expand=True)
        
        self._add_close_button(popup)
    
    def _add_close_button(self, popup):
        """Add close button to popup."""
        close_btn = self.ui_adapter.create_button(
            popup,
            text="Close",
            command=popup.destroy
        )
        close_btn.pack(pady=10)
