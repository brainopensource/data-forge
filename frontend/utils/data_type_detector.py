"""
DataTypeDetector Utility Class

Intelligent data type detection for data exploration features.
Provides automatic type detection and conversion suggestions for various data formats.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
import re
from decimal import Decimal, InvalidOperation


class DataTypeDetector:
    """
    Utility class for detecting and converting data types in exploration datasets.
    
    Features:
    - Automatic type detection from sample data
    - Support for multiple date formats
    - Numeric pattern recognition (including formatted numbers)
    - Boolean value detection
    - Type conversion suggestions
    - Data validation
    """
    
    def __init__(self):
        # Numeric patterns for various formats
        self.numeric_patterns = [
            re.compile(r'^-?\d+\.?\d*$'),  # Basic numbers: 123, -45.67
            re.compile(r'^-?\d{1,3}(,\d{3})*(\.\d+)?$'),  # Comma-separated: 1,234.56
            re.compile(r'^-?\$\d{1,3}(,\d{3})*(\.\d+)?$'),  # Currency: $1,234.56
            re.compile(r'^-?\d+\.?\d*%$'),  # Percentage: 12.5%
        ]
        
        # Date patterns for common formats
        self.date_patterns = [
            re.compile(r'\d{4}-\d{2}-\d{2}'),  # YYYY-MM-DD
            re.compile(r'\d{2}/\d{2}/\d{4}'),  # MM/DD/YYYY
            re.compile(r'\d{2}-\d{2}-\d{4}'),  # MM-DD-YYYY
            re.compile(r'\d{4}/\d{2}/\d{2}'),  # YYYY/MM/DD
            re.compile(r'\d{1,2}/\d{1,2}/\d{4}'),  # M/D/YYYY
        ]
        
        # Boolean value mappings
        self.boolean_true_values = {'true', '1', 'yes', 'y', 'on', 'enabled', 'active'}
        self.boolean_false_values = {'false', '0', 'no', 'n', 'off', 'disabled', 'inactive'}
        
    def detect_type(self, values: List[Any], sample_size: int = 50) -> str:
        """
        Detect the most appropriate data type for a list of values.
        
        Args:
            values: List of values to analyze
            sample_size: Number of values to sample for analysis
            
        Returns:
            String representing the detected type: 'datetime', 'boolean', 'integer', 'float', 'string', 'object'
        """
        if not values:
            return 'object'
            
        # Sample non-None values for analysis
        sample = [v for v in values[:sample_size] if v is not None and str(v).strip()][:20]
        if not sample:
            return 'object'

        # Check types in order of specificity
        if self._is_datetime_values(sample):
            return 'datetime'
        elif self._is_boolean_values(sample):
            return 'boolean'
        elif self._is_integer_values(sample):
            return 'integer'
        elif self._is_float_values(sample):
            return 'float'
        else:
            return 'string'

    def suggest_conversion(self, values: List[Any], target_type: str) -> Dict[str, Any]:
        """
        Suggest how to convert values to a target type.
        
        Args:
            values: List of values to convert
            target_type: Target type for conversion
            
        Returns:
            Dictionary with conversion suggestions and success rate
        """
        if not values:
            return {'success_rate': 0.0, 'convertible_count': 0, 'total_count': 0, 'errors': []}
        
        sample = values[:50]  # Work with sample for performance
        convertible_count = 0
        errors = []
        
        for i, value in enumerate(sample):
            try:
                if target_type == 'integer':
                    int(self._clean_numeric_value(str(value)))
                elif target_type == 'float':
                    float(self._clean_numeric_value(str(value)))
                elif target_type == 'datetime':
                    self._parse_datetime_value(str(value))
                elif target_type == 'boolean':
                    self._parse_boolean_value(str(value))
                
                convertible_count += 1
            except (ValueError, TypeError) as e:
                errors.append(f"Row {i}: {str(e)}")
        
        success_rate = convertible_count / len(sample) if sample else 0.0
        
        return {
            'success_rate': success_rate,
            'convertible_count': convertible_count,
            'total_count': len(sample),
            'errors': errors[:5]  # Return first 5 errors
        }

    def validate_data(self, values: List[Any], expected_type: str) -> Dict[str, Any]:
        """
        Validate that values match the expected type.
        
        Args:
            values: List of values to validate
            expected_type: Expected data type
            
        Returns:
            Dictionary with validation results
        """
        if not values:
            return {'valid': True, 'errors': [], 'invalid_count': 0}
        
        errors = []
        invalid_count = 0
        
        for i, value in enumerate(values[:100]):  # Check first 100 values
            if value is None or str(value).strip() == '':
                continue
                
            is_valid = self._validate_single_value(value, expected_type)
            if not is_valid:
                invalid_count += 1
                errors.append(f"Row {i}: '{value}' is not a valid {expected_type}")
        
        return {
            'valid': invalid_count == 0,
            'errors': errors[:10],  # Return first 10 errors
            'invalid_count': invalid_count,
            'total_checked': min(len(values), 100)
        }

    def _is_datetime_values(self, values: List[Any]) -> bool:
        """Check if values appear to be datetime values."""
        datetime_count = 0
        
        for value in values:
            if isinstance(value, (datetime, date)):
                datetime_count += 1
            elif isinstance(value, str) and self._looks_like_datetime(value):
                datetime_count += 1
        
        # Consider datetime if more than 70% of values look like dates
        return datetime_count / len(values) > 0.7

    def _is_boolean_values(self, values: List[Any]) -> bool:
        """Check if values appear to be boolean values."""
        for value in values:
            if isinstance(value, bool):
                continue
            
            str_val = str(value).lower().strip()
            if str_val in self.boolean_true_values or str_val in self.boolean_false_values:
                continue
            else:
                return False
        return True

    def _is_integer_values(self, values: List[Any]) -> bool:
        """Check if values appear to be integer values."""
        for value in values:
            try:
                cleaned = self._clean_numeric_value(str(value))
                int_val = int(cleaned)
                float_val = float(cleaned)
                # Check if it's actually an integer (no decimal part)
                if int_val != float_val:
                    return False
            except (ValueError, TypeError):
                return False
        return True

    def _is_float_values(self, values: List[Any]) -> bool:
        """Check if values appear to be float values."""
        for value in values:
            try:
                self._clean_numeric_value(str(value))
                float(self._clean_numeric_value(str(value)))
            except (ValueError, TypeError):
                return False
        return True

    def _looks_like_datetime(self, value: str) -> bool:
        """Check if a string value looks like a datetime."""
        for pattern in self.date_patterns:
            if pattern.search(value):
                return True
        return False

    def _clean_numeric_value(self, value: str) -> str:
        """Clean numeric value by removing formatting characters."""
        # Remove currency symbols, commas, percentage signs
        cleaned = value.replace('$', '').replace(',', '').replace('%', '').strip()
        return cleaned

    def _parse_datetime_value(self, value: str) -> datetime:
        """Parse a string value as datetime."""
        # Try common datetime formats
        formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%m-%d-%Y',
            '%Y/%m/%d',
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %H:%M:%S'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        
        raise ValueError(f"Unable to parse datetime: {value}")

    def _parse_boolean_value(self, value: str) -> bool:
        """Parse a string value as boolean."""
        str_val = str(value).lower().strip()
        
        if str_val in self.boolean_true_values:
            return True
        elif str_val in self.boolean_false_values:
            return False
        else:
            raise ValueError(f"Unable to parse boolean: {value}")

    def _validate_single_value(self, value: Any, expected_type: str) -> bool:
        """Validate a single value against expected type."""
        try:
            if expected_type == 'integer':
                cleaned = self._clean_numeric_value(str(value))
                int(cleaned)
                return True
            elif expected_type == 'float':
                cleaned = self._clean_numeric_value(str(value))
                float(cleaned)
                return True
            elif expected_type == 'datetime':
                if isinstance(value, (datetime, date)):
                    return True
                self._parse_datetime_value(str(value))
                return True
            elif expected_type == 'boolean':
                if isinstance(value, bool):
                    return True
                self._parse_boolean_value(str(value))
                return True
            elif expected_type == 'string':
                return True  # Any value can be a string
            else:
                return True  # Unknown type, assume valid
        except (ValueError, TypeError):
            return False

    def get_type_summary(self, values: List[Any]) -> Dict[str, Any]:
        """
        Get a comprehensive summary of data types in the values.
        
        Args:
            values: List of values to analyze
            
        Returns:
            Dictionary with type analysis summary
        """
        if not values:
            return {'total_count': 0, 'null_count': 0, 'detected_type': 'object', 'confidence': 0.0}
        
        null_count = sum(1 for v in values if v is None or str(v).strip() == '')
        non_null_values = [v for v in values if v is not None and str(v).strip() != '']
        
        detected_type = self.detect_type(non_null_values)
        
        # Calculate confidence based on how many values match the detected type
        if non_null_values:
            validation_result = self.validate_data(non_null_values, detected_type)
            valid_count = validation_result['total_checked'] - validation_result['invalid_count']
            confidence = valid_count / validation_result['total_checked'] if validation_result['total_checked'] > 0 else 0.0
        else:
            confidence = 0.0
        
        return {
            'total_count': len(values),
            'null_count': null_count,
            'non_null_count': len(non_null_values),
            'detected_type': detected_type,
            'confidence': confidence,
            'sample_values': non_null_values[:5] if non_null_values else []
        }
