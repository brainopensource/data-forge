"""
Standardized logging utilities for consistent log message formatting.
"""
from app.config.logging_config import logger


def log_operation(operation: str, status: str, record_count: int, duration_seconds: float, **kwargs) -> None:
    """Minimalist log for operations: op|status|count|duration|throughput"""
    throughput = int(record_count / duration_seconds) if duration_seconds > 0 else 0
    logger.info(
        "%s|%s|%d|%.3f|%d",
        operation,
        status,
        record_count,
        duration_seconds,
        throughput
    )


def log_operation_error(endpoint_name: str, error_message: str, record_count: int = 0) -> None:
    """Minimal error log: endpoint|error|count|message"""
    if record_count > 0:
        logger.error("%s|error|%d|%s", endpoint_name, record_count, error_message)
    else:
        logger.error("%s|error|%s", endpoint_name, error_message)


def log_application_event(event: str, details: str = "") -> None:
    """Minimal app event log: app|event|details"""
    if details:
        logger.info("app  |%s|%s", event, details)
    else:
        logger.info("app  |%s", event)
