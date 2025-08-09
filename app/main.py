"""
Main entry point for the Data Forge API.
This module serves as the application entry point and delegates to the application factory.
"""
# Import the configured application
from app.core.application import app

# For direct server execution
from app.core.server import run_server


if __name__ == "__main__":
    # Run the server when executed directly
    run_server()
