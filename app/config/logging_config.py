import logging
import sys
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
from queue import Queue
from app.config.global_settings import APIConfig
import os  # ensure os is available

_log_listener = None

def setup_logging():
    """Configure logging for the application."""
    # Create logs directory if it doesn't exist
    log_dir = APIConfig.LOG_DIR
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Create formatters
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Console handler with UTF-8 encoding
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    # Set encoding to UTF-8 to handle Unicode characters on streams that support it
    if hasattr(console_handler.stream, 'reconfigure'):
        try:
            console_handler.stream.reconfigure(encoding='utf-8')  # type: ignore
        except Exception:
            pass

    # File handler with rotation and UTF-8 encoding
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "app.log"),
        maxBytes=10485760,  # 10MB
        backupCount=5,
        encoding='utf-8'  # Explicitly set UTF-8 encoding for file
    )
    file_handler.setFormatter(formatter)

    # Use a queue to handle log events asynchronously
    log_queue = Queue(-1)
    queue_handler = QueueHandler(log_queue)
    logger.addHandler(queue_handler)

    # Start a listener in background thread
    global _log_listener
    _log_listener = QueueListener(log_queue, console_handler, file_handler, respect_handler_level=True)
    _log_listener.start()

    return logger

def stop_logging():
    """Stop the background log listener gracefully."""
    global _log_listener
    if _log_listener:
        _log_listener.stop()

# Create and configure the logger
_base_logger = setup_logging()
logger = _base_logger