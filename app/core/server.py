"""
Server configuration and runner for the Data Forge API.
Handles server-specific settings and startup for production deployment.
"""
import os
import sys
import uvicorn
from typing import Dict, Any

from app.config.global_settings import APIConfig, SYSTEM
from app.config.logging_utils import log_application_event


class ServerConfig:
    """Production server configuration for Windows optimization."""
    
    @staticmethod
    def get_production_config() -> Dict[str, Any]:
        """
        Get production-optimized server configuration.
        
        Returns:
            Dict[str, Any]: Server configuration parameters
        """
        return {
            "host": APIConfig.HOST,
            "port": APIConfig.PORT,
            "reload": False,  # Disable reload for production performance
            "workers": 1,     # Single worker optimized for Windows
            "loop": "auto",   # Let uvicorn choose the best loop for Windows
            "http": "h11",    # Use h11 for better HTTP performance on Windows
            "log_level": "info",
            "access_log": False,  # Disable access log for performance
            "server_header": False,  # Disable server header for performance
            "date_header": False,    # Disable date header for performance
            # Windows-specific optimizations
            "backlog": 2048,  # Increase backlog for Windows
            "limit_concurrency": 1000,  # Optimize for Windows concurrency
            "limit_max_requests": 10000,  # High request limit for performance
        }
    
    @staticmethod
    def get_development_config() -> Dict[str, Any]:
        """
        Get development server configuration.
        
        Returns:
            Dict[str, Any]: Development server configuration parameters
        """
        base_config = ServerConfig.get_production_config()
        base_config.update({
            "reload": True,
            "access_log": True,
            "log_level": "debug",
        })
        return base_config


def run_server(app_module: str = "app.core.application:app", production: bool = True) -> None:
    """
    Run the FastAPI server with appropriate configuration.
    
    Args:
        app_module: Module path to the FastAPI application
        production: Whether to use production or development configuration
    """
    config = ServerConfig.get_production_config() if production else ServerConfig.get_development_config()
    
    log_application_event(f"Starting Data Forge API on {APIConfig.HOST}:{APIConfig.PORT}")
    if sys.platform.startswith('win'):
        log_application_event("Using Windows ProactorEventLoop for maximum I/O performance")
    else:
        log_application_event("Using default asyncio loop (install uvloop for extra performance on Unix)")
    
    uvicorn.run(app_module, **config)


if __name__ == "__main__":
    run_server()
