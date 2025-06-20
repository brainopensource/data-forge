@echo off
REM Windows-specific dependency installer for Data Forge API
REM This script installs all required dependencies for Windows

echo ===============================================
echo   Installing Data Forge API Dependencies
echo   (Windows-optimized version)
echo ===============================================

REM Activate virtual environment if it exists
if exist ".venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call .venv\Scripts\activate
) else (
    echo Virtual environment not found. Please create one first:
    echo python -m venv .venv
    echo.
    pause
    exit /b 1
)

echo.
echo Installing core dependencies...
pip install --upgrade pip

echo.
echo Installing FastAPI and core web dependencies...
pip install fastapi>=0.104.0 uvicorn[standard]>=0.24.0 pydantic>=2.0.0 python-multipart>=0.0.5

echo.
echo Installing high-performance data processing libraries...
pip install polars>=1.30.0 pyarrow>=18.0.0 duckdb>=1.0.0 numpy>=1.24.0

echo.
echo Installing Windows-compatible performance libraries...
pip install orjson>=3.9.0 psutil>=5.9.0

echo.
echo Installing configuration and environment libraries...
pip install python-dotenv>=0.19.0 pydantic-settings>=2.0.0

echo.
echo Installing development and testing libraries...
pip install pytest>=6.2.5 pytest-asyncio>=0.21.0

echo.
echo Skipping uvloop (not supported on Windows)
echo Using default asyncio event loop for optimal Windows performance

echo.
echo ===============================================
echo   Installation Complete!
echo ===============================================
echo.
echo Note: uvloop is not available on Windows, but the application
echo will use the default asyncio event loop which is optimized
echo for Windows performance.
echo.
echo You can now run the application with:
echo   app_refactored.bat
echo.
pause 