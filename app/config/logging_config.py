import logging
import sys
from logging.handlers import RotatingFileHandler
import os
from app.config.settings import settings

def setup_logging():
    """Configure logging for the application."""
    # Create logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create logger
    logger = logging.getLogger("src")
    logger.setLevel(logging.INFO)

    # Create formatters
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Console handler with UTF-8 encoding
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    # Set encoding to UTF-8 to handle Unicode characters
    if hasattr(console_handler.stream, 'reconfigure'):
        console_handler.stream.reconfigure(encoding='utf-8')
    logger.addHandler(console_handler)

    # File handler with rotation and UTF-8 encoding
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "app.log"),
        maxBytes=10485760,  # 10MB
        backupCount=5,
        encoding='utf-8'  # Explicitly set UTF-8 encoding for file
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

# Create and configure the logger
_base_logger = setup_logging()
logger = _base_logger