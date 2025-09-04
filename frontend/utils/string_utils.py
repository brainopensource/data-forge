"""
String utility functions to centralize and standardize string operations.

This module provides common string handling functions used throughout the frontend,
eliminating code duplication and ensuring consistent string processing.
"""

from typing import Any, Optional, Union
import re


class StringUtils:
    """Utility class for string operations."""
    
    @staticmethod
    def safe_str(value: Any, default: str = "") -> str:
        """
        Safely convert value to string with default for None.
        
        Args:
            value: The value to convert to string
            default: Default value to return if value is None
            
        Returns:
            String representation of value or default
        """
        return str(value) if value is not None else default
    
    @staticmethod
    def format_number(value: Any, decimals: int = 2) -> str:
        """
        Format numeric values consistently.
        
        Args:
            value: Numeric value to format
            decimals: Number of decimal places
            
        Returns:
            Formatted number string
        """
        try:
            if isinstance(value, str):
                # Remove common separators before conversion
                clean_value = value.replace(',', '').replace(' ', '')
                return f"{float(clean_value):.{decimals}f}"
            return f"{float(value):.{decimals}f}"
        except (ValueError, TypeError):
            return StringUtils.safe_str(value)
    
    @staticmethod
    def truncate_text(text: str, max_length: int = 50, suffix: str = "...") -> str:
        """
        Truncate text with suffix.
        
        Args:
            text: Text to truncate
            max_length: Maximum length before truncation
            suffix: Suffix to add when truncating
            
        Returns:
            Truncated text with suffix if needed
        """
        if not text or len(text) <= max_length:
            return text
        return text[:max_length - len(suffix)] + suffix
    
    @staticmethod
    def clean_numeric_string(value: str) -> str:
        """
        Clean numeric strings by removing common separators.
        
        Args:
            value: String value to clean
            
        Returns:
            Cleaned string ready for numeric conversion
        """
        if not isinstance(value, str):
            return str(value)
        return value.replace(',', '').replace(' ', '').strip()
    
    @staticmethod
    def to_title_case(text: str) -> str:
        """
        Convert text to title case with proper handling of acronyms.
        
        Args:
            text: Text to convert
            
        Returns:
            Title cased text
        """
        if not text:
            return text
        
        # Handle snake_case and kebab-case
        text = text.replace('_', ' ').replace('-', ' ')
        
        # Convert to title case
        return ' '.join(word.capitalize() for word in text.split())
    
    @staticmethod
    def validate_column_name(name: str) -> bool:
        """
        Validate if a string is a valid column name.
        
        Args:
            name: Column name to validate
            
        Returns:
            True if valid column name
        """
        if not name or not isinstance(name, str):
            return False
        
        # Check for valid identifier pattern
        return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name.strip()))
    
    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        """
        Format file size in human readable format.
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Formatted size string (e.g., "1.5 MB")
        """
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        size = float(size_bytes)
        
        while size >= 1024.0 and i < len(size_names) - 1:
            size /= 1024.0
            i += 1
        
        return f"{size:.1f} {size_names[i]}"
    
    @staticmethod
    def extract_row_value(row: dict, column: str, default: str = '') -> str:
        """
        Safely extract and convert row value to string.
        
        This method centralizes the common pattern of:
        str(row.get(column, default))
        
        Args:
            row: Dictionary row data
            column: Column name to extract
            default: Default value if column not found
            
        Returns:
            String value from row
        """
        return StringUtils.safe_str(row.get(column, default))
    
    @staticmethod
    def parse_numeric_value(value: str) -> float:
        """
        Parse string value to numeric, handling common formatting.
        
        Args:
            value: String value to parse
            
        Returns:
            Parsed numeric value
            
        Raises:
            ValueError: If value cannot be parsed as numeric
        """
        if not isinstance(value, str):
            return float(value)
        
        clean_value = StringUtils.clean_numeric_string(value)
        return float(clean_value)
