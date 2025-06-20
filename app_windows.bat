@echo off
REM Data Forge API - Windows Ultra-Performance Launcher
REM Optimized for maximum performance on Windows systems
cd /d %~dp0

echo ===============================================
echo   Data Forge API - Windows Ultra Performance
echo ===============================================
echo   Platform: Windows-optimized
echo   Target: 10M+ rows/second
echo   Event Loop: Windows ProactorEventLoop
echo   Deployment: Local-first
echo ===============================================

REM Set Windows-specific performance environment variables
set PYTHONUTF8=1
set PYTHONUNBUFFERED=1
set PYTHONHASHSEED=0

REM Activate the virtual environment
echo Activating virtual environment...
call .venv\Scripts\activate

REM Create required directories with Windows-optimized structure
if not exist "data" mkdir data
if not exist "temp" mkdir temp
if not exist "logs" mkdir logs
if not exist "data\cache" mkdir data\cache
if not exist "data\schemas" mkdir data\schemas

REM Display system information
echo.
echo System Information:
echo OS: Windows
echo Python: %PYTHON_EXE%
echo Working Directory: %CD%
echo.

REM Run the Windows-optimized FastAPI application
echo Starting Data Forge API (Windows Ultra-Performance)...
echo.
echo Performance Features:
echo   - Windows ProactorEventLoop for I/O
echo   - Single-process optimization
echo   - Native Windows async I/O
echo   - Optimized memory patterns
echo   - Local-first deployment
echo.
echo Available at: http://localhost:8080
echo API Documentation: http://localhost:8080/docs
echo Health Check: http://localhost:8080/health
echo Performance Info: http://localhost:8080/performance
echo.
echo Press Ctrl+C to stop the server
echo ===============================================

python -m app.main_windows

REM Handle graceful shutdown and error reporting
if %ERRORLEVEL% neq 0 (
    echo.
    echo ===============================================
    echo   Application Error - Exit Code: %ERRORLEVEL%
    echo ===============================================
    echo.
    echo Troubleshooting:
    echo 1. Check if all dependencies are installed
    echo 2. Verify Python environment is activated
    echo 3. Check logs directory for error details
    echo 4. Run: python -m pip install -r requirements_windows.txt
    echo.
    echo For support, check the logs or run the test script:
    echo   python test_windows.py
    echo.
    pause
) 