"""Command handlers for data validation operations."""

from typing import List, Dict, Any
from frontend.application.commands.base import ICommandHandler, CommandResult
from frontend.application.commands.validation_commands import (
    ValidateDatasetCommand,
    CleanDatasetCommand,
    AddValidationRuleCommand,
    GenerateQualityReportCommand,
    ApplyDataCleaningCommand,
    ValidateAndCleanCommand
)
from frontend.domain.services.data_validation_service import DataValidationService, ValidationRule
from frontend.utils.error_handler import ErrorHandler


class ValidateDatasetCommandHandler(ICommandHandler[ValidateDatasetCommand]):
    """Handler for dataset validation commands."""
    
    def __init__(self, validation_service: DataValidationService, error_handler: ErrorHandler):
        self.validation_service = validation_service
        self.error_handler = error_handler
    
    async def handle(self, command: ValidateDatasetCommand) -> CommandResult:
        """Handle dataset validation."""
        try:
            # Add custom validation rules if provided
            if command.validation_rules:
                for rule in command.validation_rules:
                    self.validation_service.add_validation_rule(rule)
            
            # Perform validation
            result = self.validation_service.validate_dataset(command.data)
            
            return CommandResult(
                success=result.is_valid,
                message=f"Validation completed. Quality score: {result.quality_score:.1f}%",
                data=result
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "Dataset validation failed")
            return CommandResult(
                success=False,
                message=f"Validation failed: {str(e)}",
                error=e
            )


class CleanDatasetCommandHandler(ICommandHandler[CleanDatasetCommand]):
    """Handler for dataset cleaning commands."""
    
    def __init__(self, validation_service: DataValidationService, error_handler: ErrorHandler):
        self.validation_service = validation_service
        self.error_handler = error_handler
    
    async def handle(self, command: CleanDatasetCommand) -> CommandResult:
        """Handle dataset cleaning."""
        try:
            # Set cleaning rules if provided
            if command.cleaning_rules:
                self.validation_service.cleaning_rules = command.cleaning_rules
            else:
                # Use default cleaning rules
                self.validation_service.cleaning_rules = [
                    "remove_empty_rows",
                    "standardize_columns",
                    "handle_missing",
                    "convert_types"
                ]
            
            # Perform cleaning
            cleaned_data = self.validation_service.clean_dataset(command.data)
            
            return CommandResult(
                success=True,
                message=f"Dataset cleaned successfully. {len(cleaned_data)} rows processed.",
                data=cleaned_data
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "Dataset cleaning failed")
            return CommandResult(
                success=False,
                message=f"Cleaning failed: {str(e)}",
                error=e
            )


class AddValidationRuleCommandHandler(ICommandHandler[AddValidationRuleCommand]):
    """Handler for adding validation rules."""
    
    def __init__(self, validation_service: DataValidationService, error_handler: ErrorHandler):
        self.validation_service = validation_service
        self.error_handler = error_handler
    
    async def handle(self, command: AddValidationRuleCommand) -> CommandResult:
        """Handle adding validation rule."""
        try:
            self.validation_service.add_validation_rule(command.rule)
            
            return CommandResult(
                success=True,
                message=f"Validation rule '{command.rule.name}' added successfully.",
                data=command.rule
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "Failed to add validation rule")
            return CommandResult(
                success=False,
                message=f"Failed to add rule: {str(e)}",
                error=e
            )


class GenerateQualityReportCommandHandler(ICommandHandler[GenerateQualityReportCommand]):
    """Handler for generating data quality reports."""
    
    def __init__(self, validation_service: DataValidationService, error_handler: ErrorHandler):
        self.validation_service = validation_service
        self.error_handler = error_handler
    
    async def handle(self, command: GenerateQualityReportCommand) -> CommandResult:
        """Handle quality report generation."""
        try:
            report = self.validation_service.get_quality_report(command.data)
            
            return CommandResult(
                success=True,
                message=f"Quality report generated. Score: {report.get('quality_score', 0):.1f}%",
                data=report
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "Quality report generation failed")
            return CommandResult(
                success=False,
                message=f"Report generation failed: {str(e)}",
                error=e
            )


class ApplyDataCleaningCommandHandler(ICommandHandler[ApplyDataCleaningCommand]):
    """Handler for applying specific data cleaning operations."""
    
    def __init__(self, validation_service: DataValidationService, error_handler: ErrorHandler):
        self.validation_service = validation_service
        self.error_handler = error_handler
    
    async def handle(self, command: ApplyDataCleaningCommand) -> CommandResult:
        """Handle specific cleaning operations."""
        try:
            # Set the specific operations to apply
            original_rules = self.validation_service.cleaning_rules.copy()
            self.validation_service.cleaning_rules = command.operations
            
            # Apply cleaning
            cleaned_data = self.validation_service.clean_dataset(command.data)
            
            # Restore original rules
            self.validation_service.cleaning_rules = original_rules
            
            return CommandResult(
                success=True,
                message=f"Applied {len(command.operations)} cleaning operations successfully.",
                data=cleaned_data
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "Data cleaning operations failed")
            return CommandResult(
                success=False,
                message=f"Cleaning operations failed: {str(e)}",
                error=e
            )


class ValidateAndCleanCommandHandler(ICommandHandler[ValidateAndCleanCommand]):
    """Handler for combined validation and cleaning operations."""
    
    def __init__(self, validation_service: DataValidationService, error_handler: ErrorHandler):
        self.validation_service = validation_service
        self.error_handler = error_handler
    
    async def handle(self, command: ValidateAndCleanCommand) -> CommandResult:
        """Handle combined validation and cleaning."""
        try:
            # Add custom validation rules if provided
            if command.validation_rules:
                for rule in command.validation_rules:
                    self.validation_service.add_validation_rule(rule)
            
            # Perform initial validation
            initial_validation = self.validation_service.validate_dataset(command.data)
            
            # Apply cleaning if auto_clean is enabled
            cleaned_data = command.data
            if command.auto_clean:
                if command.cleaning_rules:
                    self.validation_service.cleaning_rules = command.cleaning_rules
                
                cleaned_data = self.validation_service.clean_dataset(command.data)
                
                # Validate again after cleaning
                final_validation = self.validation_service.validate_dataset(cleaned_data)
            else:
                final_validation = initial_validation
            
            result_data = {
                "original_data": command.data,
                "cleaned_data": cleaned_data,
                "initial_validation": initial_validation,
                "final_validation": final_validation,
                "improvement": {
                    "quality_score_improvement": final_validation.quality_score - initial_validation.quality_score,
                    "errors_reduced": initial_validation.errors_count - final_validation.errors_count,
                    "warnings_reduced": initial_validation.warnings_count - final_validation.warnings_count
                }
            }
            
            return CommandResult(
                success=True,
                message=f"Validation and cleaning completed. Quality improved from {initial_validation.quality_score:.1f}% to {final_validation.quality_score:.1f}%",
                data=result_data
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "Combined validation and cleaning failed")
            return CommandResult(
                success=False,
                message=f"Validation and cleaning failed: {str(e)}",
                error=e
            )
