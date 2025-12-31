"""
Main entry point for the DataForge frontend application.
"""
from frontend.app_modular import DataForgeApp

def main():
    """Initializes and runs the main application."""
    app = DataForgeApp()
    app.run()

if __name__ == "__main__":
    main()
