"""
Standardized logging utilities for consistent log message formatting.
"""
from app.config.logging_config import logger


def _pad_endpoint(endpoint_name: str) -> str:
    return endpoint_name.ljust(28)


def log_operation_start(endpoint_name: str, record_count: int, **kwargs) -> None:
    """Log the start of an operation with standardized format."""
    extra_info = ""
    if kwargs:
        extra_parts = []
        for key, value in kwargs.items():
            extra_parts.append(f"{key}={value}")
        if extra_parts:
            extra_info = f" ({', '.join(extra_parts)})"
    
    logger.info(f"[{_pad_endpoint(endpoint_name)}] Starting operation for {record_count:,} records{extra_info}")


def log_operation_success(endpoint_name: str, record_count: int, duration_seconds: float, **kwargs) -> None:
    """Log successful operation completion with standardized format."""
    throughput = int(record_count / duration_seconds) if duration_seconds > 0 else 0
    
    # Format message: [endpoint] Successfully wrote X records in Y.Zs (T records/sec)
    message = f"[{_pad_endpoint(endpoint_name)}] Successfully wrote {record_count:,} records in {duration_seconds:.3f}s ({throughput:,} records/sec)"
    
    logger.info(message)


def log_operation_read(endpoint_name: str, record_count: int, duration_seconds: float, source: str = "") -> None:
    """Log successful read operation with standardized format."""
    throughput = int(record_count / duration_seconds) if duration_seconds > 0 else 0
    
    # Format message: [endpoint] Successfully read X records in Y.Zs (T records/sec)
    message = f"[{_pad_endpoint(endpoint_name)}] Successfully read {record_count:,} records in {duration_seconds:.3f}s ({throughput:,} records/sec)"
    if source:
        message += f" from {source}"
    
    logger.info(message)


def log_operation_error(endpoint_name: str, error_message: str, record_count: int = 0) -> None:
    """Log operation error with standardized format."""
    if record_count > 0:
        logger.error(f"[{_pad_endpoint(endpoint_name)}] Failed processing {record_count:,} records - {error_message}")
    else:
        logger.error(f"[{_pad_endpoint(endpoint_name)}] Failed - {error_message}")


def log_application_event(event: str, details: str = "") -> None:
    """Log application-level events (startup, shutdown, etc.) with standardized format."""
    if details:
        logger.info(f"[{_pad_endpoint('application')}] {event} - {details}")
    else:
        logger.info(f"[{_pad_endpoint('application')}] {event}")
