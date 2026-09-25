"""Enhanced Data Explorer component with integrated validation service."""

import asyncio
from typing import List, Dict, Any, Optional
from frontend.components.data_explorer import DataExplorer
from frontend.domain.services.data_validation_service import (
    DataValidationService, ValidationRule, ValidationType, Severity
)
from frontend.application.commands.validation_commands import (
    ValidateDatasetCommand, CleanDatasetCommand, GenerateQualityReportCommand
)
from frontend.application.handlers.validation_handlers import (
    ValidateDatasetCommandHandler, CleanDatasetCommandHandler, GenerateQualityReportCommandHandler
)
from frontend.application.queries.validation_queries import (
    GetDataQualityMetricsQuery, GetDataQualityRecommendationsQuery
)
from frontend.application.handlers.validation_query_handlers import (
    GetDataQualityMetricsQueryHandler, GetDataQualityRecommendationsQueryHandler
)
from frontend.core.container import get_service
from frontend.utils.error_handler import ErrorHandler
from frontend.services.ui_framework_adapter import UIFrameworkAdapter


class EnhancedDataExplorer(DataExplorer):
    """Data Explorer with integrated validation and cleaning capabilities."""
    
    def __init__(self, parent, data=None):
        super().__init__(parent, data)
        
        # Get services from DI container
        self.validation_service = get_service(DataValidationService)
        self.error_handler = get_service(ErrorHandler)
        self.ui_adapter = get_service(UIFrameworkAdapter)
        
        # Initialize CQRS handlers
        self._setup_handlers()
        
        # Add validation UI components
        self._add_validation_panel()
        
        # Track validation state
        self.last_validation_result = None
        self.last_quality_report = None
    
    def _setup_handlers(self):
        """Setup CQRS command and query handlers."""
        self.validate_handler = ValidateDatasetCommandHandler(self.validation_service, self.error_handler)
        self.clean_handler = CleanDatasetCommandHandler(self.validation_service, self.error_handler)
        self.quality_report_handler = GenerateQualityReportCommandHandler(self.validation_service, self.error_handler)
        
        self.metrics_query_handler = GetDataQualityMetricsQueryHandler(self.validation_service, self.error_handler)
        self.recommendations_query_handler = GetDataQualityRecommendationsQueryHandler(self.validation_service, self.error_handler)
    
    def _add_validation_panel(self):
        """Add data validation panel to the explorer toolbar."""
        validation_frame = self.ui_adapter.create_frame(self.toolbar_frame)
        validation_frame.pack(side="right", padx=5)
        
        # Validation button
        validate_btn = self.ui_adapter.create_button(
            validation_frame, 
            text="🔍 Validate",
            command=self._run_validation
        )
        validate_btn.pack(side="left", padx=2)
        
        # Clean button
        clean_btn = self.ui_adapter.create_button(
            validation_frame,
            text="🧹 Clean",
            command=self._clean_data
        )
        clean_btn.pack(side="left", padx=2)
        
        # Quality report button
        quality_btn = self.ui_adapter.create_button(
            validation_frame,
            text="📊 Quality",
            command=self._show_quality_report
        )
        quality_btn.pack(side="left", padx=2)
        
        # Recommendations button
        rec_btn = self.ui_adapter.create_button(
            validation_frame,
            text="💡 Tips",
            command=self._show_recommendations
        )
        rec_btn.pack(side="left", padx=2)
    
    def _run_validation(self):
        """Run data validation asynchronously."""
        if not self.filtered_data:
            self._update_info("❌ No data to validate")
            return
        
        def run_async():
            try:
                # Add common validation rules
                rules = [
                    ValidationRule(
                        name="not_null_check",
                        column="*",
                        rule_type=ValidationType.NOT_NULL,
                        parameters={},
                        severity=Severity.WARNING,
                        message="Null values detected"
                    )
                ]
                
                command = ValidateDatasetCommand(data=self.filtered_data, validation_rules=rules)
                
                # Run validation in event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(self.validate_handler.handle(command))
                loop.close()
                
                self.last_validation_result = result
                
                # Update UI on main thread
                self.parent.after(0, lambda: self._show_validation_results(result))
                
            except Exception as e:
                self.error_handler.handle_error(e, "Validation failed")
                self.parent.after(0, lambda: self._update_info(f"❌ Validation failed: {str(e)}"))
        
        # Run in background thread
        import threading
        thread = threading.Thread(target=run_async, daemon=True)
        thread.start()
        
        self._update_info("🔍 Running validation...")
    
    def _clean_data(self):
        """Clean data asynchronously."""
        if not self.filtered_data:
            self._update_info("❌ No data to clean")
            return
        
        def run_async():
            try:
                command = CleanDatasetCommand(data=self.filtered_data)
                
                # Run cleaning in event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(self.clean_handler.handle(command))
                loop.close()
                
                if result.success:
                    # Update data on main thread
                    self.parent.after(0, lambda: self._apply_cleaned_data(result.data))
                else:
                    self.parent.after(0, lambda: self._update_info(f"❌ Cleaning failed: {result.message}"))
                
            except Exception as e:
                self.error_handler.handle_error(e, "Data cleaning failed")
                self.parent.after(0, lambda: self._update_info(f"❌ Cleaning failed: {str(e)}"))
        
        # Run in background thread
        import threading
        thread = threading.Thread(target=run_async, daemon=True)
        thread.start()
        
        self._update_info("🧹 Cleaning data...")
    
    def _show_quality_report(self):
        """Show comprehensive quality report."""
        if not self.filtered_data:
            self._update_info("❌ No data to analyze")
            return
        
        def run_async():
            try:
                query = GetDataQualityMetricsQuery(data=self.filtered_data)
                
                # Run query in event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(self.metrics_query_handler.handle(query))
                loop.close()
                
                self.last_quality_report = result
                
                # Show report on main thread
                self.parent.after(0, lambda: self._display_quality_report(result))
                
            except Exception as e:
                self.error_handler.handle_error(e, "Quality report failed")
                self.parent.after(0, lambda: self._update_info(f"❌ Quality report failed: {str(e)}"))
        
        # Run in background thread
        import threading
        thread = threading.Thread(target=run_async, daemon=True)
        thread.start()
        
        self._update_info("📊 Generating quality report...")
    
    def _show_recommendations(self):
        """Show data quality recommendations."""
        if not self.filtered_data:
            self._update_info("❌ No data to analyze")
            return
        
        def run_async():
            try:
                query = GetDataQualityRecommendationsQuery(data=self.filtered_data)
                
                # Run query in event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(self.recommendations_query_handler.handle(query))
                loop.close()
                
                # Show recommendations on main thread
                self.parent.after(0, lambda: self._display_recommendations(result))
                
            except Exception as e:
                self.error_handler.handle_error(e, "Recommendations failed")
                self.parent.after(0, lambda: self._update_info(f"❌ Recommendations failed: {str(e)}"))
        
        # Run in background thread
        import threading
        thread = threading.Thread(target=run_async, daemon=True)
        thread.start()
        
        self._update_info("💡 Generating recommendations...")
    
    def _show_validation_results(self, result):
        """Display validation results in popup."""
        if not result.success:
            self._update_info(f"❌ {result.message}")
            return
        
        validation_data = result.data
        popup = self.ui_adapter.create_toplevel()
        popup.title("Data Validation Results")
        popup.geometry("700x500")
        
        # Header with quality score
        header_frame = self.ui_adapter.create_frame(popup)
        header_frame.pack(fill="x", padx=10, pady=10)
        
        score_label = self.ui_adapter.create_label(
            header_frame,
            text=f"Quality Score: {validation_data.quality_score:.1f}%",
            font=("Arial", 16, "bold")
        )
        score_label.pack(pady=5)
        
        # Summary
        summary_text = f"Found {validation_data.errors_count} errors and {validation_data.warnings_count} warnings in {validation_data.total_rows_checked} rows"
        summary_label = self.ui_adapter.create_label(header_frame, text=summary_text)
        summary_label.pack(pady=5)
        
        # Issues list
        if validation_data.issues:
            issues_frame = self.ui_adapter.create_scrollable_frame(popup)
            issues_frame.pack(fill="both", expand=True, padx=10, pady=10)
            
            for issue in validation_data.issues[:20]:  # Show first 20 issues
                issue_text = f"Row {issue.row_index}: {issue.message}"
                if issue.suggested_fix:
                    issue_text += f"\nSuggested fix: {issue.suggested_fix}"
                
                issue_label = self.ui_adapter.create_label(
                    issues_frame,
                    text=issue_text,
                    justify="left"
                )
                issue_label.pack(anchor="w", pady=2)
        
        # Close button
        close_btn = self.ui_adapter.create_button(
            popup,
            text="Close",
            command=popup.destroy
        )
        close_btn.pack(pady=10)
        
        self._update_info(f"✅ Validation complete. Quality: {validation_data.quality_score:.1f}%")
    
    def _display_quality_report(self, result):
        """Display comprehensive quality report."""
        if not result.success:
            self._update_info(f"❌ {result.message}")
            return
        
        report_data = result.data
        popup = self.ui_adapter.create_toplevel()
        popup.title("Data Quality Report")
        popup.geometry("800x600")
        
        # Create scrollable content
        scroll_frame = self.ui_adapter.create_scrollable_frame(popup)
        scroll_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Overall metrics
        metrics_text = f"""📊 Data Quality Report
        
🔍 Overall Quality Score: {report_data.get('quality_score', 0):.1f}%

📏 Data Size:
• Total Rows: {report_data.get('total_rows', 0):,}
• Total Columns: {report_data.get('total_columns', 0)}
• Empty Rows: {report_data.get('empty_rows', 0)}
• Duplicate Rows: {report_data.get('duplicate_rows', 0)}

📈 Completeness:
• Overall Completeness: {report_data.get('completeness_metrics', {}).get('overall_completeness', 0):.1f}%
• Complete Rows: {report_data.get('completeness_metrics', {}).get('complete_rows_percentage', 0):.1f}%
"""
        
        metrics_label = self.ui_adapter.create_label(
            scroll_frame,
            text=metrics_text,
            justify="left",
            font=("Courier", 10)
        )
        metrics_label.pack(anchor="w", pady=10)
        
        # Column completeness
        if 'completeness_metrics' in report_data and 'column_completeness' in report_data['completeness_metrics']:
            col_completeness = report_data['completeness_metrics']['column_completeness']
            
            completeness_text = "📋 Column Completeness:\n"
            for col, percentage in sorted(col_completeness.items(), key=lambda x: x[1], reverse=True):
                completeness_text += f"• {col}: {percentage:.1f}%\n"
            
            completeness_label = self.ui_adapter.create_label(
                scroll_frame,
                text=completeness_text,
                justify="left",
                font=("Courier", 9)
            )
            completeness_label.pack(anchor="w", pady=5)
        
        # Recommendations
        if 'recommendations' in report_data:
            rec_text = "💡 Recommendations:\n"
            for i, rec in enumerate(report_data['recommendations'][:10], 1):
                rec_text += f"{i}. {rec}\n"
            
            rec_label = self.ui_adapter.create_label(
                scroll_frame,
                text=rec_text,
                justify="left",
                font=("Arial", 9)
            )
            rec_label.pack(anchor="w", pady=5)
        
        # Close button
        close_btn = self.ui_adapter.create_button(
            popup,
            text="Close",
            command=popup.destroy
        )
        close_btn.pack(pady=10)
        
        self._update_info(f"📊 Quality report generated. Score: {report_data.get('quality_score', 0):.1f}%")
    
    def _display_recommendations(self, result):
        """Display data quality recommendations."""
        if not result.success:
            self._update_info(f"❌ {result.message}")
            return
        
        rec_data = result.data
        popup = self.ui_adapter.create_toplevel()
        popup.title("Data Quality Recommendations")
        popup.geometry("600x400")
        
        # Header
        header_label = self.ui_adapter.create_label(
            popup,
            text="💡 Data Quality Improvement Recommendations",
            font=("Arial", 14, "bold")
        )
        header_label.pack(pady=10)
        
        # Summary
        summary = rec_data.get('summary', {})
        summary_text = f"Found {summary.get('total_recommendations', 0)} recommendations ({summary.get('high_priority', 0)} high priority)"
        summary_label = self.ui_adapter.create_label(popup, text=summary_text)
        summary_label.pack(pady=5)
        
        # Recommendations list
        scroll_frame = self.ui_adapter.create_scrollable_frame(popup)
        scroll_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        for i, rec in enumerate(rec_data.get('recommendations', []), 1):
            priority_icon = "🔴" if rec['priority'] == 'high' else "🟡"
            rec_text = f"{priority_icon} {i}. {rec['recommendation']}\n   Category: {rec['category']}"
            
            rec_label = self.ui_adapter.create_label(
                scroll_frame,
                text=rec_text,
                justify="left"
            )
            rec_label.pack(anchor="w", pady=3)
        
        # Close button
        close_btn = self.ui_adapter.create_button(
            popup,
            text="Close",
            command=popup.destroy
        )
        close_btn.pack(pady=10)
        
        self._update_info(f"💡 Generated {len(rec_data.get('recommendations', []))} recommendations")
    
    def _apply_cleaned_data(self, cleaned_data):
        """Apply cleaned data to the explorer."""
        if cleaned_data:
            # Update the data
            self.data = cleaned_data
            self.filtered_data = cleaned_data.copy()
            
            # Refresh the display
            self._refresh_table()
            self._update_stats()
            
            self._update_info(f"🧹 Data cleaned successfully. {len(cleaned_data)} rows processed.")
        else:
            self._update_info("❌ No cleaned data received")
    
    def update_data(self, new_data):
        """Override to reset validation state when data changes."""
        super().update_data(new_data)
        self.last_validation_result = None
        self.last_quality_report = None
