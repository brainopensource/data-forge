"""
Standardized logging utilities for consistent log message formatting.
"""
from app.config.logging_config import logger


def log_operation_start(endpoint_name: str, record_count: int, **kwargs) -> None:
    """Minimal start log: endpoint:start:count"""
    logger.info("%s:start:%d", endpoint_name, record_count)


def log_operation_success(endpoint_name: str, record_count: int, duration_seconds: float, **kwargs) -> None:
    """Minimal success log: endpoint:success:count:duration:throughput"""
    throughput = int(record_count / duration_seconds) if duration_seconds > 0 else 0
    logger.info(
        "%s:success:%d:%.3f: %d/s",  # endpoint:success:count:duration:throughput
        endpoint_name,
        record_count,
        duration_seconds,
        throughput
    )


def log_operation_read(endpoint_name: str, record_count: int, duration_seconds: float, source: str = "") -> None:
    """Minimal read log: endpoint:read:count:duration:throughput"""
    throughput = int(record_count / duration_seconds) if duration_seconds > 0 else 0
    logger.info(
        "%s:read:%d:%.3f:%d",  # endpoint:read:count:duration:throughput
        endpoint_name,
        record_count,
        duration_seconds,
        throughput
    )


def log_operation_error(endpoint_name: str, error_message: str, record_count: int = 0) -> None:
    """Minimal error log: endpoint:error:count:message"""
    if record_count > 0:
        logger.error("%s:error:%d:%s", endpoint_name, record_count, error_message)
    else:
        logger.error("%s:error:%s", endpoint_name, error_message)


def log_application_event(event: str, details: str = "") -> None:
    """Minimal app event log: application:event:details"""
    if details:
        logger.info("application:%s:%s", event, details)
    else:
        logger.info("application:%s", event)
