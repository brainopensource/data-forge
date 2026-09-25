"""CQRS Queries for data validation operations."""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from frontend.application.queries.base import BaseQuery


@dataclass
class GetDataQualityMetricsQuery(BaseQuery):
    """Query to get data quality metrics."""
    data: List[Dict[str, Any]]


@dataclass
class GetValidationRulesQuery(BaseQuery):
    """Query to get current validation rules."""
    pass


@dataclass
class GetDataQualityRecommendationsQuery(BaseQuery):
    """Query to get data quality improvement recommendations."""
    data: List[Dict[str, Any]]


@dataclass
class GetColumnQualityAnalysisQuery(BaseQuery):
    """Query to get quality analysis for specific columns."""
    data: List[Dict[str, Any]]
    columns: Optional[List[str]] = None


@dataclass
class GetDataTypeAnalysisQuery(BaseQuery):
    """Query to get data type analysis for dataset."""
    data: List[Dict[str, Any]]


@dataclass
class CheckDataConsistencyQuery(BaseQuery):
    """Query to check data consistency across records."""
    data: List[Dict[str, Any]]


@dataclass
class GetValidationHistoryQuery(BaseQuery):
    """Query to get validation history for a dataset."""
    dataset_id: Optional[str] = None
    limit: int = 10
