"""Data validation domain service with CQRS and DI support."""

from typing import List, Dict, Any, Optional, Set, Callable
from ..entities.data_record import DataRecord
import re
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
from frontend.utils.error_handler import ErrorHandler


class ValidationType(Enum):
    """Available validation types."""
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    PATTERN = "pattern"
    DATA_TYPE = "data_type"
    CUSTOM = "custom"


class Severity(Enum):
    """Issue severity levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationRule:
    """Validation rule definition."""
    name: str
    column: str
    rule_type: ValidationType
    parameters: Dict[str, Any]
    severity: Severity
    message: str


@dataclass
class ValidationIssue:
    """Validation issue with contextual information."""
    rule_name: str
    column: str
    row_index: int
    severity: Severity
    message: str
    suggested_fix: Optional[str] = None


@dataclass
class ValidationResult:
    """Comprehensive validation result."""
    is_valid: bool
    issues: List[ValidationIssue]
    total_rows_checked: int
    errors_count: int
    warnings_count: int
    quality_score: float


class ValidationError:
    """Represents a validation error."""
    
    def __init__(self, field: str, message: str, code: str = "VALIDATION_ERROR"):
        self.field = field
        self.message = message
        self.code = code
    
    def __str__(self) -> str:
        return f"{self.field}: {self.message}"
    
    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary."""
        return {
            'field': self.field,
            'message': self.message,
            'code': self.code
        }


class DataValidationService:
    """Enhanced domain service for data validation with CQRS support."""
    
    def __init__(self, error_handler: Optional[ErrorHandler] = None):
        self.error_handler = error_handler or ErrorHandler()
        self._validation_rules: Dict[str, List[Callable]] = {}
        self.validation_rules: List[ValidationRule] = []
        self.cleaning_rules: List[str] = []
        self._setup_default_rules()
    
    def add_validation_rule(self, rule: ValidationRule) -> None:
        """Add a validation rule to the service."""
        self.validation_rules.append(rule)
    
    def validate_dataset(self, data: List[Dict]) -> ValidationResult:
        """Comprehensive dataset validation."""
        issues = []
        
        for rule in self.validation_rules:
            rule_issues = self._apply_validation_rule(data, rule)
            issues.extend(rule_issues)
        
        errors = [issue for issue in issues if issue.severity == Severity.ERROR]
        warnings = [issue for issue in issues if issue.severity == Severity.WARNING]
        
        quality_score = self._calculate_quality_score(len(data), len(errors), len(warnings))
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            issues=issues,
            total_rows_checked=len(data),
            errors_count=len(errors),
            warnings_count=len(warnings),
            quality_score=quality_score
        )
    
    def clean_dataset(self, data: List[Dict]) -> List[Dict]:
        """Apply cleaning transformations."""
        cleaned_data = data.copy()
        
        # Apply cleaning rules in order
        for rule in self.cleaning_rules:
            if rule == "remove_empty_rows":
                cleaned_data = self._remove_empty_rows(cleaned_data)
            elif rule == "standardize_columns":
                cleaned_data = self._standardize_column_names(cleaned_data)
            elif rule == "handle_missing":
                cleaned_data = self._handle_missing_values(cleaned_data)
            elif rule == "convert_types":
                cleaned_data = self._convert_data_types(cleaned_data)
        
        return cleaned_data
    
    def get_quality_report(self, data: List[Dict]) -> Dict[str, Any]:
        """Generate data quality metrics."""
        if not data:
            return {"error": "No data provided"}
        
        total_rows = len(data)
        total_columns = len(data[0].keys()) if data else 0
        
        # Analyze data quality
        empty_rows = sum(1 for row in data if all(v in [None, "", " "] for v in row.values()))
        duplicate_rows = total_rows - len({str(sorted(row.items())) for row in data})
        
        columns_with_nulls = {}
        for col in data[0].keys():
            null_count = sum(1 for row in data if row.get(col) in [None, "", " "])
            if null_count > 0:
                columns_with_nulls[col] = null_count
        
        quality_score = self._calculate_overall_quality(data)
        
        return {
            "total_rows": total_rows,
            "total_columns": total_columns,
            "empty_rows": empty_rows,
            "duplicate_rows": duplicate_rows,
            "columns_with_nulls": columns_with_nulls,
            "quality_score": quality_score,
            "recommendations": self._generate_recommendations(data)
        }
    
    def _apply_validation_rule(self, data: List[Dict], rule: ValidationRule) -> List[ValidationIssue]:
        """Apply a single validation rule."""
        issues = []
        
        for row_idx, row in enumerate(data):
            value = row.get(rule.column)
            
            if rule.rule_type == ValidationType.NOT_NULL:
                if value is None or value == "":
                    issues.append(ValidationIssue(
                        rule_name=rule.name,
                        column=rule.column,
                        row_index=row_idx,
                        severity=rule.severity,
                        message=f"Null value found in column '{rule.column}'",
                        suggested_fix="Provide a default value or remove the row"
                    ))
            
            elif rule.rule_type == ValidationType.RANGE:
                if value is not None and isinstance(value, (int, float)):
                    min_val = rule.parameters.get("min")
                    max_val = rule.parameters.get("max")
                    if min_val is not None and value < min_val:
                        issues.append(ValidationIssue(
                            rule_name=rule.name,
                            column=rule.column,
                            row_index=row_idx,
                            severity=rule.severity,
                            message=f"Value {value} is below minimum {min_val}",
                            suggested_fix=f"Set value to {min_val} or review data source"
                        ))
                    if max_val is not None and value > max_val:
                        issues.append(ValidationIssue(
                            rule_name=rule.name,
                            column=rule.column,
                            row_index=row_idx,
                            severity=rule.severity,
                            message=f"Value {value} exceeds maximum {max_val}",
                            suggested_fix=f"Set value to {max_val} or review data source"
                        ))
        
        return issues
    
    def _remove_empty_rows(self, data: List[Dict]) -> List[Dict]:
        """Remove completely empty rows."""
        return [row for row in data if not all(v in [None, "", " "] for v in row.values())]
    
    def _standardize_column_names(self, data: List[Dict]) -> List[Dict]:
        """Clean and standardize column names."""
        if not data:
            return data
        
        # Create mapping of old to new column names
        column_mapping = {}
        for col in data[0].keys():
            # Remove special characters, convert to lowercase, replace spaces with underscores
            new_col = col.strip().lower().replace(" ", "_").replace("-", "_")
            new_col = "".join(c for c in new_col if c.isalnum() or c == "_")
            column_mapping[col] = new_col
        
        # Apply mapping to all rows
        standardized_data = []
        for row in data:
            new_row = {column_mapping[k]: v for k, v in row.items()}
            standardized_data.append(new_row)
        
        return standardized_data
    
    def _handle_missing_values(self, data: List[Dict], strategy: str = "auto") -> List[Dict]:
        """Handle missing values with configurable strategies."""
        if not data:
            return data
        
        # Analyze each column to determine best strategy
        for col in data[0].keys():
            values = [row[col] for row in data if row[col] not in [None, "", " "]]
            
            if not values:
                continue
            
            # Determine column type and appropriate fill strategy
            if all(isinstance(v, (int, float)) for v in values):
                # Numeric column - use mean
                fill_value = sum(values) / len(values)
            elif all(isinstance(v, str) for v in values):
                # String column - use most common value
                from collections import Counter
                fill_value = Counter(values).most_common(1)[0][0]
            else:
                fill_value = None
            
            # Apply fill strategy
            if fill_value is not None:
                for row in data:
                    if row[col] in [None, "", " "]:
                        row[col] = fill_value
        
        return data
    
    def _convert_data_types(self, data: List[Dict]) -> List[Dict]:
        """Auto-detect and convert data types."""
        from frontend.utils.data_type_detector import DataTypeDetector
        
        detector = DataTypeDetector()
        
        # Process each column
        if not data:
            return data
        
        for col in data[0].keys():
            # Get all values for this column
            column_values = [row.get(col) for row in data]
            non_null_values = [v for v in column_values if v not in [None, "", " "]]
            
            if not non_null_values:
                continue
                
            # Detect type for the column
            detected_type = detector.detect_type(non_null_values)
            
            # Apply conversions based on detected type
            for row in data:
                value = row.get(col)
                if value not in [None, "", " "]:
                    if detected_type == "integer":
                        try:
                            row[col] = int(float(str(value)))
                        except (ValueError, TypeError):
                            pass
                    elif detected_type == "float":
                        try:
                            row[col] = float(str(value))
                        except (ValueError, TypeError):
                            pass
                    elif detected_type == "boolean":
                        str_val = str(value).lower()
                        row[col] = str_val in ["true", "1", "yes", "on"]
        
        return data
    
    def _calculate_quality_score(self, total_rows: int, errors: int, warnings: int) -> float:
        """Calculate overall quality score (0-100)."""
        if total_rows == 0:
            return 0.0
        
        error_penalty = (errors / total_rows) * 50  # Errors count as 50% penalty
        warning_penalty = (warnings / total_rows) * 25  # Warnings count as 25% penalty
        
        score = 100 - error_penalty - warning_penalty
        return max(0.0, min(100.0, score))
    
    def _calculate_overall_quality(self, data: List[Dict]) -> float:
        """Calculate comprehensive quality score."""
        if not data:
            return 0.0
        
        total_cells = len(data) * len(data[0])
        
        # Count issues
        empty_cells = sum(1 for row in data for value in row.values() if value in [None, "", " "])
        
        # Quality based on completeness
        completeness = (total_cells - empty_cells) / total_cells
        
        return completeness * 100
    
    def _generate_recommendations(self, data: List[Dict]) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []
        
        if not data:
            return ["No data to analyze"]
        
        # Check for empty rows
        empty_rows = sum(1 for row in data if all(v in [None, "", " "] for v in row.values()))
        if empty_rows > 0:
            recommendations.append(f"Remove {empty_rows} completely empty rows")
        
        # Check for columns with high null percentage
        for col in data[0].keys():
            null_count = sum(1 for row in data if row.get(col) in [None, "", " "])
            null_percentage = (null_count / len(data)) * 100
            
            if null_percentage > 50:
                recommendations.append(f"Column '{col}' has {null_percentage:.1f}% missing values - consider removing or imputing")
            elif null_percentage > 20:
                recommendations.append(f"Column '{col}' has {null_percentage:.1f}% missing values - consider imputation")
        
        # Check for duplicate rows
        unique_rows = len({str(sorted(row.items())) for row in data})
        if unique_rows < len(data):
            recommendations.append(f"Found {len(data) - unique_rows} duplicate rows - consider deduplication")
        
        return recommendations
    
    # Legacy methods for backward compatibility with existing DataRecord interface
    def validate_record(self, record: DataRecord) -> List[ValidationError]:
        """Validate a single data record (legacy interface)."""
        errors = []
        
        # Basic record validation
        if not record.id:
            errors.append(ValidationError("id", "Record ID is required", "MISSING_ID"))
            
        if not isinstance(record.data, dict):
            errors.append(ValidationError("data", "Record data must be a dictionary", "INVALID_DATA_TYPE"))
            return errors
        
        if not record.data:
            errors.append(ValidationError("data", "Record data cannot be empty", "EMPTY_DATA"))
            
        # Validate individual fields
        for field_name, value in record.data.items():
            field_errors = self._validate_field(field_name, value)
            errors.extend(field_errors)
            
        return errors
    
    def validate_records(self, records: List[DataRecord]) -> Dict[str, List[ValidationError]]:
        """Validate multiple records (legacy interface)."""
        validation_results = {}
        
        for record in records:
            errors = self.validate_record(record)
            if errors:
                validation_results[record.id] = errors
                
        return validation_results
    
    def validate_column_values(self, column_name: str, values: List[Any]) -> List[ValidationError]:
        """Validate values for a specific column."""
        errors = []
        
        if not values:
            errors.append(ValidationError(column_name, "Column has no values", "NO_VALUES"))
            return errors
        
        # Check for data type consistency
        type_counts = {}
        for value in values:
            value_type = type(value).__name__
            type_counts[value_type] = type_counts.get(value_type, 0) + 1
        
        # If more than 50% of values are None/null, flag it
        none_count = type_counts.get('NoneType', 0)
        if none_count > len(values) * 0.5:
            errors.append(ValidationError(
                column_name, 
                f"Column has too many null values ({none_count}/{len(values)})",
                "TOO_MANY_NULLS"
            ))
        
        # Check for mixed data types (excluding None)
        non_null_types = {k: v for k, v in type_counts.items() if k != 'NoneType'}
        if len(non_null_types) > 1:
            main_type = max(non_null_types.keys(), key=lambda k: non_null_types[k])
            if non_null_types[main_type] < sum(non_null_types.values()) * 0.8:
                errors.append(ValidationError(
                    column_name,
                    f"Column has mixed data types: {list(non_null_types.keys())}",
                    "MIXED_TYPES"
                ))
        
        return errors
    
    def validate_data_consistency(self, records: List[DataRecord]) -> List[ValidationError]:
        """Validate data consistency across records (legacy interface)."""
        errors = []
        
        if not records:
            return errors
        
        # Check schema consistency
        all_columns = set()
        record_columns = {}
        
        for record in records:
            columns = set(record.get_columns())
            all_columns.update(columns)
            record_columns[record.id] = columns
        
        # Find missing columns in each record
        for record_id, columns in record_columns.items():
            missing = all_columns - columns
            if missing:
                errors.append(ValidationError(
                    record_id,
                    f"Record missing columns: {sorted(missing)}",
                    "MISSING_COLUMNS"
                ))
        
        return errors
    
    def _validate_field(self, field_name: str, value: Any) -> List[ValidationError]:
        """Validate a single field value."""
        errors = []
        
        # Apply registered validation rules
        if field_name in self._validation_rules:
            for rule in self._validation_rules[field_name]:
                try:
                    rule_errors = rule(field_name, value)
                    if rule_errors:
                        errors.extend(rule_errors)
                except Exception as e:
                    errors.append(ValidationError(
                        field_name,
                        f"Validation rule error: {str(e)}",
                        "RULE_ERROR"
                    ))
        
        # Generic validation
        if value is None:
            # Allow null values for now - schema should define required fields
            pass
        elif isinstance(value, str):
            # String validation
            if len(value.strip()) == 0:
                errors.append(ValidationError(field_name, "String value is empty", "EMPTY_STRING"))
        elif isinstance(value, (int, float)):
            # Numeric validation
            if isinstance(value, float) and (value != value):  # NaN check
                errors.append(ValidationError(field_name, "Numeric value is NaN", "NAN_VALUE"))
        
        return errors
    
    def add_legacy_validation_rule(self, field_name: str, rule: Callable[[str, Any], List[ValidationError]]) -> None:
        """Add custom validation rule for a field (legacy interface)."""
        if field_name not in self._validation_rules:
            self._validation_rules[field_name] = []
        self._validation_rules[field_name].append(rule)
    
    def _setup_default_rules(self) -> None:
        """Setup default validation rules."""
        
        def validate_email(field_name: str, value: Any) -> List[ValidationError]:
            """Validate email format."""
            if not isinstance(value, str):
                return []
            
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if not re.match(email_pattern, value):
                return [ValidationError(field_name, "Invalid email format", "INVALID_EMAIL")]
            return []
        
        def validate_date_string(field_name: str, value: Any) -> List[ValidationError]:
            """Validate date string format."""
            if not isinstance(value, str):
                return []
            
            # Try to parse as date
            try:
                datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                return [ValidationError(field_name, "Invalid date format", "INVALID_DATE")]
            return []
        
        # Register rules for common field patterns
        self.add_legacy_validation_rule('email', validate_email)
        self.add_legacy_validation_rule('date', validate_date_string)
        self.add_legacy_validation_rule('created_at', validate_date_string)
        self.add_legacy_validation_rule('updated_at', validate_date_string)
    
    def get_validation_summary(self, records: List[DataRecord]) -> Dict[str, Any]:
        """Get validation summary for a list of records (legacy interface)."""
        total_records = len(records)
        validation_results = self.validate_records(records)
        consistency_errors = self.validate_data_consistency(records)
        
        total_errors = sum(len(errors) for errors in validation_results.values())
        total_errors += len(consistency_errors)
        
        valid_records = total_records - len(validation_results)
        
        return {
            'total_records': total_records,
            'valid_records': valid_records,
            'invalid_records': len(validation_results),
            'total_errors': total_errors,
            'consistency_errors': len(consistency_errors),
            'validation_rate': valid_records / total_records if total_records > 0 else 0,
            'error_details': validation_results,
            'consistency_issues': [error.to_dict() for error in consistency_errors]
        }
