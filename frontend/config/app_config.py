"""
Application Configuration for DataForge Frontend
"""
from pathlib import Path


class AppConfig:
    """Fixed application configuration"""
    API_BASE_URL = "http://localhost:8080"
    FAVICON_PATH = Path(__file__).parent.parent.parent / "static" / "images" / "favicon.ico"
    DEFAULT_SCHEMA = "well_production"
    DEFAULT_RECORDS = "10000"
    DEFAULT_COMPRESSION = "zstd"


class Colors:
    """Backward-compatible color shim for refactored theming."""
    from frontend.presentation.styles.theme import Theme
    
    PRIMARY = Theme.COLOR_PRIMARY
    # Use secondary (purple) as hover accent per new theme design
    PRIMARY_HOVER = Theme.COLOR_SECONDARY
    TEXT_PRIMARY = Theme.COLOR_TEXT_PRIMARY
