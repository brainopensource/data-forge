"""Data validation integration utilities for the main app."""

import asyncio
import threading
from typing import List, Dict, Any, Optional
from frontend.domain.services.data_validation_service import (
    DataValidationService, ValidationRule, ValidationType, Severity
)
from frontend.application.commands.validation_commands import (
    ValidateDatasetCommand, CleanDatasetCommand, GenerateQualityReportCommand
)
from frontend.application.queries.validation_queries import (
    GetDataQualityMetricsQuery, GetDataQualityRecommendationsQuery
)
from frontend.core.container import get_service
from frontend.utils.error_handler import ErrorHandler


class DataValidationIntegration:
    """Integration class for data validation features in the main app."""
    
    def __init__(self):
        # Get services from DI container
        self.validation_service = get_service(DataValidationService)
        self.error_handler = get_service(ErrorHandler)
        
        # Setup default validation rules
        self._setup_default_validation_rules()
    
    def _setup_default_validation_rules(self):
        """Setup common validation rules for datasets."""
        # Not null rule for critical columns
        not_null_rule = ValidationRule(
            name="critical_not_null",
            column="*",  # Apply to all columns
            rule_type=ValidationType.NOT_NULL,
            parameters={},
            severity=Severity.WARNING,
            message="Missing values detected"
        )
        self.validation_service.add_validation_rule(not_null_rule)
        
        # Set default cleaning rules
        self.validation_service.cleaning_rules = [
            "remove_empty_rows",
            "standardize_columns", 
            "handle_missing",
            "convert_types"
        ]
    
    def validate_data_sync(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Synchronously validate data and return results."""
        try:
            if not data:
                return {
                    "success": False,
                    "message": "No data to validate",
                    "quality_score": 0.0
                }
            
            result = self.validation_service.validate_dataset(data)
            
            return {
                "success": True,
                "message": f"Validation complete. Quality score: {result.quality_score:.1f}%",
                "quality_score": result.quality_score,
                "errors_count": result.errors_count,
                "warnings_count": result.warnings_count,
                "total_rows": result.total_rows_checked,
                "issues": [
                    {
                        "row": issue.row_index,
                        "column": issue.column,
                        "message": issue.message,
                        "severity": issue.severity.value,
                        "suggested_fix": issue.suggested_fix
                    }
                    for issue in result.issues[:10]  # Limit to first 10 for display
                ]
            }
            
        except Exception as e:
            self.error_handler.handle_error(e, "Data validation failed")
            return {
                "success": False,
                "message": f"Validation failed: {str(e)}",
                "quality_score": 0.0
            }
    
    def clean_data_sync(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Synchronously clean data and return results."""
        try:
            if not data:
                return {
                    "success": False,
                    "message": "No data to clean",
                    "cleaned_data": []
                }
            
            cleaned_data = self.validation_service.clean_dataset(data)
            
            return {
                "success": True,
                "message": f"Data cleaned successfully. {len(cleaned_data)} rows processed.",
                "cleaned_data": cleaned_data,
                "original_rows": len(data),
                "cleaned_rows": len(cleaned_data),
                "rows_removed": len(data) - len(cleaned_data)
            }
            
        except Exception as e:
            self.error_handler.handle_error(e, "Data cleaning failed")
            return {
                "success": False,
                "message": f"Cleaning failed: {str(e)}",
                "cleaned_data": data  # Return original data if cleaning fails
            }
    
    def get_quality_report_sync(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Synchronously generate quality report."""
        try:
            if not data:
                return {
                    "success": False,
                    "message": "No data to analyze"
                }
            
            report = self.validation_service.get_quality_report(data)
            
            return {
                "success": True,
                "message": f"Quality report generated. Score: {report.get('quality_score', 0):.1f}%",
                "report": report
            }
            
        except Exception as e:
            self.error_handler.handle_error(e, "Quality report generation failed")
            return {
                "success": False,
                "message": f"Report generation failed: {str(e)}"
            }
    
    def get_recommendations_sync(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Synchronously get data quality recommendations."""
        try:
            if not data:
                return {
                    "success": False,
                    "message": "No data to analyze",
                    "recommendations": []
                }
            
            recommendations = self.validation_service._generate_recommendations(data)
            
            # Add priority and categorization
            prioritized_recommendations = []
            for rec in recommendations:
                priority = "high" if any(word in rec.lower() for word in ["remove", "missing", "duplicate"]) else "medium"
                category = self._categorize_recommendation(rec)
                
                prioritized_recommendations.append({
                    "text": rec,
                    "priority": priority,
                    "category": category
                })
            
            return {
                "success": True,
                "message": f"Generated {len(recommendations)} recommendations",
                "recommendations": prioritized_recommendations,
                "total_count": len(recommendations),
                "high_priority_count": sum(1 for r in prioritized_recommendations if r["priority"] == "high")
            }
            
        except Exception as e:
            self.error_handler.handle_error(e, "Recommendations generation failed")
            return {
                "success": False,
                "message": f"Recommendations failed: {str(e)}",
                "recommendations": []
            }
    
    def _categorize_recommendation(self, recommendation: str) -> str:
        """Categorize recommendation by type."""
        rec_lower = recommendation.lower()
        if "missing" in rec_lower or "null" in rec_lower:
            return "completeness"
        elif "duplicate" in rec_lower:
            return "uniqueness"
        elif "remove" in rec_lower:
            return "cleanup"
        elif "type" in rec_lower:
            return "data_types"
        else:
            return "general"
    
    def validate_and_clean_async(self, data: List[Dict[str, Any]], callback=None):
        """Asynchronously validate and clean data with callback."""
        def run_async():
            try:
                # Validate first
                validation_result = self.validate_data_sync(data)
                
                # Clean if validation found issues
                if validation_result["success"] and (validation_result["errors_count"] > 0 or validation_result["warnings_count"] > 0):
                    cleaning_result = self.clean_data_sync(data)
                    
                    # Validate cleaned data
                    if cleaning_result["success"]:
                        final_validation = self.validate_data_sync(cleaning_result["cleaned_data"])
                        
                        result = {
                            "success": True,
                            "message": f"Validation and cleaning complete. Quality improved from {validation_result['quality_score']:.1f}% to {final_validation['quality_score']:.1f}%",
                            "initial_validation": validation_result,
                            "cleaning_result": cleaning_result,
                            "final_validation": final_validation,
                            "cleaned_data": cleaning_result["cleaned_data"]
                        }
                    else:
                        result = {
                            "success": False,
                            "message": "Validation succeeded but cleaning failed",
                            "initial_validation": validation_result,
                            "cleaning_result": cleaning_result
                        }
                else:
                    result = {
                        "success": True,
                        "message": "Data validation complete. No cleaning needed.",
                        "initial_validation": validation_result,
                        "cleaned_data": data
                    }
                
                if callback:
                    callback(result)
                    
            except Exception as e:
                error_result = {
                    "success": False,
                    "message": f"Validation and cleaning failed: {str(e)}",
                    "error": str(e)
                }
                if callback:
                    callback(error_result)
        
        # Run in background thread
        thread = threading.Thread(target=run_async, daemon=True)
        thread.start()


# Global instance for easy access
validation_integration = DataValidationIntegration()
