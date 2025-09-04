"""Query handlers for data validation operations."""

from typing import List, Dict, Any, Optional
from frontend.application.queries.base import IQueryHandler, QueryResult, IQuery
from frontend.application.queries.validation_queries import (
    GetDataQualityMetricsQuery,
    GetValidationRulesQuery,
    GetDataQualityRecommendationsQuery,
    GetColumnQualityAnalysisQuery,
    GetDataTypeAnalysisQuery,
    CheckDataConsistencyQuery,
    GetValidationHistoryQuery
)
from frontend.domain.services.data_validation_service import DataValidationService
from frontend.utils.error_handler import ErrorHandler
from frontend.utils.data_type_detector import DataTypeDetector


class GetDataQualityMetricsQueryHandler(IQueryHandler[Dict[str, Any]]):
    """Handler for data quality metrics queries."""
    
    def __init__(self, validation_service: DataValidationService, error_handler: ErrorHandler):
        self.validation_service = validation_service
        self.error_handler = error_handler
    
    async def handle(self, query: IQuery) -> QueryResult[Dict[str, Any]]:
        """Handle data quality metrics query."""
        try:
            assert isinstance(query, GetDataQualityMetricsQuery)
            metrics = self.validation_service.get_quality_report(query.data)
            
            # Add additional metrics
            total_rows = len(query.data)
            total_cols = len(query.data[0].keys()) if query.data else 0
            
            enhanced_metrics = {
                **metrics,
                "data_size": {
                    "total_rows": total_rows,
                    "total_columns": total_cols,
                    "total_cells": total_rows * total_cols
                },
                "completeness_metrics": self._calculate_completeness_metrics(query.data)
            }
            
            return QueryResult(
                success=True,
                message="Quality metrics retrieved successfully",
                data=enhanced_metrics
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "Failed to get quality metrics")
            return QueryResult(
                success=False,
                message=f"Failed to get metrics: {str(e)}",
                error=e
            )
    
    def _calculate_completeness_metrics(self, data: List[Dict]) -> Dict[str, Any]:
        """Calculate detailed completeness metrics."""
        if not data:
            return {}
        
        column_completeness = {}
        for col in data[0].keys():
            non_null_count = sum(1 for row in data if row.get(col) not in [None, "", " "])
            completeness = (non_null_count / len(data)) * 100
            column_completeness[col] = completeness
        
        overall_completeness = sum(column_completeness.values()) / len(column_completeness)
        
        return {
            "overall_completeness": overall_completeness,
            "column_completeness": column_completeness,
            "complete_rows": sum(1 for row in data if all(v not in [None, "", " "] for v in row.values())),
            "complete_rows_percentage": (sum(1 for row in data if all(v not in [None, "", " "] for v in row.values())) / len(data)) * 100
        }


class GetValidationRulesQueryHandler(IQueryHandler[Dict[str, Any]]):
    """Handler for validation rules queries."""
    
    def __init__(self, validation_service: DataValidationService, error_handler: ErrorHandler):
        self.validation_service = validation_service
        self.error_handler = error_handler
    
    async def handle(self, query: IQuery) -> QueryResult[Dict[str, Any]]:
        """Handle validation rules query."""
        try:
            assert isinstance(query, GetValidationRulesQuery)
            rules_data = {
                "active_rules": len(self.validation_service.validation_rules),
                "cleaning_rules": self.validation_service.cleaning_rules,
                "validation_rules": [
                    {
                        "name": rule.name,
                        "column": rule.column,
                        "type": rule.rule_type.value,
                        "severity": rule.severity.value,
                        "message": rule.message
                    }
                    for rule in self.validation_service.validation_rules
                ]
            }
            
            return QueryResult(
                success=True,
                message="Validation rules retrieved successfully",
                data=rules_data
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "Failed to get validation rules")
            return QueryResult(
                success=False,
                message=f"Failed to get rules: {str(e)}",
                error=e
            )


class GetDataQualityRecommendationsQueryHandler(IQueryHandler[Dict[str, Any]]):
    """Handler for data quality recommendations queries."""
    
    def __init__(self, validation_service: DataValidationService, error_handler: ErrorHandler):
        self.validation_service = validation_service
        self.error_handler = error_handler
    
    async def handle(self, query: IQuery) -> QueryResult[Dict[str, Any]]:
        """Handle data quality recommendations query."""
        try:
            assert isinstance(query, GetDataQualityRecommendationsQuery)
            recommendations = self.validation_service._generate_recommendations(query.data)
            
            # Add priority levels to recommendations
            prioritized_recommendations = []
            for rec in recommendations:
                priority = "high" if any(word in rec.lower() for word in ["remove", "missing", "duplicate"]) else "medium"
                prioritized_recommendations.append({
                    "recommendation": rec,
                    "priority": priority,
                    "category": self._categorize_recommendation(rec)
                })
            
            return QueryResult(
                success=True,
                message=f"Generated {len(recommendations)} recommendations",
                data={
                    "recommendations": prioritized_recommendations,
                    "summary": {
                        "total_recommendations": len(recommendations),
                        "high_priority": sum(1 for r in prioritized_recommendations if r["priority"] == "high"),
                        "medium_priority": sum(1 for r in prioritized_recommendations if r["priority"] == "medium")
                    }
                }
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "Failed to generate recommendations")
            return QueryResult(
                success=False,
                message=f"Failed to generate recommendations: {str(e)}",
                error=e
            )
    
    def _categorize_recommendation(self, recommendation: str) -> str:
        """Categorize recommendation by type."""
        if "missing" in recommendation.lower() or "null" in recommendation.lower():
            return "completeness"
        elif "duplicate" in recommendation.lower():
            return "uniqueness"
        elif "remove" in recommendation.lower():
            return "cleanup"
        elif "type" in recommendation.lower():
            return "data_types"
        else:
            return "general"


class GetColumnQualityAnalysisQueryHandler(IQueryHandler[Dict[str, Any]]):
    """Handler for column quality analysis queries."""
    
    def __init__(self, validation_service: DataValidationService, data_type_detector: DataTypeDetector, error_handler: ErrorHandler):
        self.validation_service = validation_service
        self.data_type_detector = data_type_detector
        self.error_handler = error_handler
    
    async def handle(self, query: IQuery) -> QueryResult[Dict[str, Any]]:
        """Handle column quality analysis query."""
        try:
            assert isinstance(query, GetColumnQualityAnalysisQuery)
            if not query.data:
                return QueryResult(success=True, message="No data to analyze", data={})
            
            columns_to_analyze = query.columns or list(query.data[0].keys())
            analysis = {}
            
            for col in columns_to_analyze:
                column_values = [row.get(col) for row in query.data]
                
                # Get type analysis
                type_summary = self.data_type_detector.get_type_summary(column_values)
                
                # Get validation errors for this column
                validation_errors = self.validation_service.validate_column_values(col, column_values)
                
                analysis[col] = {
                    "type_analysis": type_summary,
                    "validation_issues": [error.to_dict() for error in validation_errors],
                    "quality_score": self._calculate_column_quality_score(column_values, validation_errors),
                    "statistics": self._calculate_column_statistics(column_values, type_summary["detected_type"])
                }
            
            return QueryResult(
                success=True,
                message=f"Column analysis completed for {len(columns_to_analyze)} columns",
                data=analysis
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "Column quality analysis failed")
            return QueryResult(
                success=False,
                message=f"Column analysis failed: {str(e)}",
                error=e
            )
    
    def _calculate_column_quality_score(self, values: List[Any], errors: List[Any]) -> float:
        """Calculate quality score for a column."""
        if not values:
            return 0.0
        
        # Base score on completeness
        non_null_count = sum(1 for v in values if v not in [None, "", " "])
        completeness_score = (non_null_count / len(values)) * 100
        
        # Reduce score based on validation errors
        error_penalty = min(len(errors) * 5, 50)  # Max 50% penalty for errors
        
        return max(0.0, completeness_score - error_penalty)
    
    def _calculate_column_statistics(self, values: List[Any], data_type: str) -> Dict[str, Any]:
        """Calculate statistics for a column based on its data type."""
        non_null_values = [v for v in values if v not in [None, "", " "]]
        
        stats = {
            "count": len(values),
            "non_null_count": len(non_null_values),
            "null_count": len(values) - len(non_null_values),
            "null_percentage": ((len(values) - len(non_null_values)) / len(values)) * 100 if values else 0
        }
        
        if data_type in ["integer", "float"] and non_null_values:
            try:
                numeric_values = [float(v) for v in non_null_values if str(v).replace('.', '').replace('-', '').isdigit()]
                if numeric_values:
                    stats.update({
                        "min": min(numeric_values),
                        "max": max(numeric_values),
                        "mean": sum(numeric_values) / len(numeric_values),
                        "unique_count": len(set(numeric_values))
                    })
            except (ValueError, TypeError):
                pass
        elif data_type == "string" and non_null_values:
            str_values = [str(v) for v in non_null_values]
            stats.update({
                "unique_count": len(set(str_values)),
                "min_length": min(len(s) for s in str_values),
                "max_length": max(len(s) for s in str_values),
                "avg_length": sum(len(s) for s in str_values) / len(str_values)
            })
        
        return stats


class GetDataTypeAnalysisQueryHandler(IQueryHandler[Dict[str, Any]]):
    """Handler for data type analysis queries."""
    
    def __init__(self, data_type_detector: DataTypeDetector, error_handler: ErrorHandler):
        self.data_type_detector = data_type_detector
        self.error_handler = error_handler
    
    async def handle(self, query: IQuery) -> QueryResult[Dict[str, Any]]:
        """Handle data type analysis query."""
        try:
            assert isinstance(query, GetDataTypeAnalysisQuery)
            if not query.data:
                return QueryResult(success=True, message="No data to analyze", data={})
            
            type_analysis = {}
            for col in query.data[0].keys():
                column_values = [row.get(col) for row in query.data]
                type_summary = self.data_type_detector.get_type_summary(column_values)
                type_analysis[col] = type_summary
            
            # Overall analysis
            overall_analysis = {
                "columns_analyzed": len(type_analysis),
                "type_distribution": {},
                "columns_by_type": {},
                "type_confidence_avg": 0.0
            }
            
            # Calculate type distribution
            for col, analysis in type_analysis.items():
                detected_type = analysis["detected_type"]
                if detected_type not in overall_analysis["type_distribution"]:
                    overall_analysis["type_distribution"][detected_type] = 0
                    overall_analysis["columns_by_type"][detected_type] = []
                
                overall_analysis["type_distribution"][detected_type] += 1
                overall_analysis["columns_by_type"][detected_type].append(col)
            
            # Calculate average confidence
            if type_analysis:
                total_confidence = sum(analysis["confidence"] for analysis in type_analysis.values())
                overall_analysis["type_confidence_avg"] = total_confidence / len(type_analysis)
            
            return QueryResult(
                success=True,
                message=f"Data type analysis completed for {len(type_analysis)} columns",
                data={
                    "column_analysis": type_analysis,
                    "overall_analysis": overall_analysis
                }
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "Data type analysis failed")
            return QueryResult(
                success=False,
                message=f"Data type analysis failed: {str(e)}",
                error=e
            )


class CheckDataConsistencyQueryHandler(IQueryHandler[Dict[str, Any]]):
    """Handler for data consistency checks."""
    
    def __init__(self, validation_service: DataValidationService, error_handler: ErrorHandler):
        self.validation_service = validation_service
        self.error_handler = error_handler
    
    async def handle(self, query: IQuery) -> QueryResult[Dict[str, Any]]:
        """Handle data consistency check query."""
        try:
            assert isinstance(query, CheckDataConsistencyQuery)
            if not query.data:
                return QueryResult(success=True, message="No data to check", data={})
            
            # Check schema consistency
            all_columns = set()
            row_schemas = []
            
            for i, row in enumerate(query.data):
                row_columns = set(row.keys())
                all_columns.update(row_columns)
                row_schemas.append({
                    "row_index": i,
                    "columns": row_columns,
                    "column_count": len(row_columns)
                })
            
            # Find inconsistencies
            inconsistencies = []
            expected_columns = all_columns
            
            for schema in row_schemas:
                missing_cols = expected_columns - schema["columns"]
                extra_cols = schema["columns"] - expected_columns
                
                if missing_cols or extra_cols:
                    inconsistencies.append({
                        "row_index": schema["row_index"],
                        "missing_columns": list(missing_cols),
                        "extra_columns": list(extra_cols),
                        "severity": "error" if missing_cols else "warning"
                    })
            
            # Calculate consistency score
            consistent_rows = len(query.data) - len(inconsistencies)
            consistency_score = (consistent_rows / len(query.data)) * 100 if query.data else 100.0
            
            return QueryResult(
                success=True,
                message=f"Data consistency check completed. Score: {consistency_score:.1f}%",
                data={
                    "consistency_score": consistency_score,
                    "total_rows": len(query.data),
                    "consistent_rows": consistent_rows,
                    "inconsistent_rows": len(inconsistencies),
                    "expected_columns": list(expected_columns),
                    "column_count": len(expected_columns),
                    "inconsistencies": inconsistencies[:20]  # Limit to first 20
                }
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "Data consistency check failed")
            return QueryResult(
                success=False,
                message=f"Consistency check failed: {str(e)}",
                error=e
            )
