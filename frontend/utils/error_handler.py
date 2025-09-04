"""
Centralized error handling and logging utilities.

This module provides consistent error handling, logging, and user feedback
mechanisms throughout the frontend application.
"""

import logging
import traceback
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional, Union
from pathlib import Path


class ErrorLevel(Enum):
    """Error severity levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ErrorHandler:
    """Centralized error handling and logging utility."""
    
    _logger = None
    _log_file = None
    
    @classmethod
    def initialize(cls, log_file: Optional[Union[str, Path]] = None) -> None:
        """
        Initialize the error handler with logging configuration.
        
        Args:
            log_file: Optional path to log file
        """
        if cls._logger is None:
            cls._logger = logging.getLogger('DataForge.Frontend')
            cls._logger.setLevel(logging.DEBUG)
            
            # Create formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            cls._logger.addHandler(console_handler)
            
            # File handler if log file specified
            if log_file:
                cls._log_file = Path(log_file)
                cls._log_file.parent.mkdir(parents=True, exist_ok=True)
                
                file_handler = logging.FileHandler(cls._log_file)
                file_handler.setLevel(logging.DEBUG)
                file_handler.setFormatter(formatter)
                cls._logger.addHandler(file_handler)
    
    @classmethod
    def handle_error(
        cls,
        error: Exception,
        context: str = "",
        level: ErrorLevel = ErrorLevel.ERROR,
        user_message: Optional[str] = None,
        show_traceback: bool = True
    ) -> str:
        """
        Handle an error with logging and user message generation.
        
        Args:
            error: The exception that occurred
            context: Context where the error occurred
            level: Error severity level
            user_message: Custom user-friendly message
            show_traceback: Whether to include traceback in logs
            
        Returns:
            User-friendly error message
        """
        cls._ensure_initialized()
        
        # Create detailed log message
        error_details = f"Error in {context}: {str(error)}" if context else str(error)
        
        if show_traceback:
            error_details += f"\nTraceback:\n{traceback.format_exc()}"
        
        # Log the error
        cls._log_message(level, error_details)
        
        # Return user-friendly message
        if user_message:
            return user_message
        
        return cls.create_user_message(error, context)
    
    @classmethod
    def log_error(cls, message: str, level: ErrorLevel = ErrorLevel.ERROR) -> None:
        """
        Log an error message.
        
        Args:
            message: Error message to log
            level: Error severity level
        """
        cls._ensure_initialized()
        cls._log_message(level, message)
    
    @classmethod
    def create_user_message(cls, error: Exception, context: str = "") -> str:
        """
        Create a user-friendly error message.
        
        Args:
            error: The exception that occurred
            context: Context where the error occurred
            
        Returns:
            User-friendly error message
        """
        error_type = type(error).__name__
        
        # Map common errors to user-friendly messages
        user_messages = {
            'FileNotFoundError': 'File not found. Please check the file path.',
            'PermissionError': 'Permission denied. Please check file permissions.',
            'ValueError': 'Invalid data format. Please check your input.',
            'ConnectionError': 'Connection failed. Please check your network.',
            'TimeoutError': 'Operation timed out. Please try again.',
            'KeyError': 'Missing required data field.',
            'TypeError': 'Data type mismatch. Please check your data format.'
        }
        
        base_message = user_messages.get(error_type, 'An unexpected error occurred.')
        
        if context:
            return f"❌ {base_message} (in {context})"
        
        return f"❌ {base_message}"
    
    @classmethod
    def safe_execute(
        cls,
        func: Callable,
        *args,
        context: str = "",
        default_return: Any = None,
        user_message: Optional[str] = None,
        **kwargs
    ) -> tuple[Any, Optional[str]]:
        """
        Safely execute a function with error handling.
        
        Args:
            func: Function to execute
            *args: Function arguments
            context: Context description
            default_return: Value to return on error
            user_message: Custom error message for users
            **kwargs: Function keyword arguments
            
        Returns:
            Tuple of (result, error_message). error_message is None on success.
        """
        try:
            result = func(*args, **kwargs)
            return result, None
        except Exception as e:
            error_msg = cls.handle_error(e, context, user_message=user_message)
            return default_return, error_msg
    
    @classmethod
    def log_performance(cls, operation: str, duration: float) -> None:
        """
        Log performance information.
        
        Args:
            operation: Description of the operation
            duration: Duration in seconds
        """
        cls._ensure_initialized()
        message = f"Performance: {operation} took {duration:.3f}s"
        cls._log_message(ErrorLevel.INFO, message)
    
    @classmethod
    def log_user_action(cls, action: str, details: Optional[str] = None) -> None:
        """
        Log user actions for debugging.
        
        Args:
            action: User action description
            details: Additional action details
        """
        cls._ensure_initialized()
        message = f"User Action: {action}"
        if details:
            message += f" - {details}"
        cls._log_message(ErrorLevel.INFO, message)
    
    @classmethod
    def _ensure_initialized(cls) -> None:
        """Ensure logger is initialized."""
        if cls._logger is None:
            cls.initialize()
    
    @classmethod
    def _log_message(cls, level: ErrorLevel, message: str) -> None:
        """Log message at specified level."""
        cls._ensure_initialized()
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"[{timestamp}] {message}"
        
        if cls._logger:  # Type guard
            if level == ErrorLevel.DEBUG:
                cls._logger.debug(formatted_message)
            elif level == ErrorLevel.INFO:
                cls._logger.info(formatted_message)
            elif level == ErrorLevel.WARNING:
                cls._logger.warning(formatted_message)
            elif level == ErrorLevel.ERROR:
                cls._logger.error(formatted_message)
            elif level == ErrorLevel.CRITICAL:
                cls._logger.critical(formatted_message)
    
    @classmethod
    def format_validation_error(cls, field: str, value: Any, reason: str) -> str:
        """
        Format validation error messages consistently.
        
        Args:
            field: Field name that failed validation
            value: Invalid value
            reason: Reason for validation failure
            
        Returns:
            Formatted validation error message
        """
        return f"❌ Invalid {field}: '{value}' - {reason}"
    
    @classmethod
    def format_success_message(cls, operation: str, details: Optional[str] = None) -> str:
        """
        Format success messages consistently.
        
        Args:
            operation: Operation that succeeded
            details: Additional success details
            
        Returns:
            Formatted success message
        """
        message = f"✅ {operation}"
        if details:
            message += f" - {details}"
        return message


# Convenience functions for common operations
def handle_error(error: Exception, context: str = "", user_message: Optional[str] = None) -> str:
    """Convenience function for error handling."""
    return ErrorHandler.handle_error(error, context, user_message=user_message)


def safe_execute(func: Callable, *args, context: str = "", default_return: Any = None, **kwargs) -> tuple[Any, Optional[str]]:
    """Convenience function for safe execution."""
    return ErrorHandler.safe_execute(func, *args, context=context, default_return=default_return, **kwargs)


def log_user_action(action: str, details: Optional[str] = None) -> None:
    """Convenience function for logging user actions."""
    ErrorHandler.log_user_action(action, details)
