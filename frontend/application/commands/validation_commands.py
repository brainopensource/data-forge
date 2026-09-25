"""CQRS Commands for data validation operations."""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from frontend.application.commands.base import BaseCommand
from frontend.domain.services.data_validation_service import ValidationRule, ValidationResult


@dataclass
class ValidateDatasetCommand(BaseCommand):
    """Command to validate a dataset."""
    data: List[Dict[str, Any]]
    validation_rules: Optional[List[ValidationRule]] = None


@dataclass
class CleanDatasetCommand(BaseCommand):
    """Command to clean a dataset."""
    data: List[Dict[str, Any]]
    cleaning_rules: Optional[List[str]] = None


@dataclass
class AddValidationRuleCommand(BaseCommand):
    """Command to add a validation rule."""
    rule: ValidationRule


@dataclass
class GenerateQualityReportCommand(BaseCommand):
    """Command to generate a data quality report."""
    data: List[Dict[str, Any]]


@dataclass
class ApplyDataCleaningCommand(BaseCommand):
    """Command to apply data cleaning transformations."""
    data: List[Dict[str, Any]]
    operations: List[str]  # List of cleaning operations to apply


@dataclass
class ValidateAndCleanCommand(BaseCommand):
    """Command to validate and clean data in one operation."""
    data: List[Dict[str, Any]]
    validation_rules: Optional[List[ValidationRule]] = None
    cleaning_rules: Optional[List[str]] = None
    auto_clean: bool = True
