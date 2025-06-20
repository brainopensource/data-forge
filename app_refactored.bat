@echo off
REM Data Forge API - Refactored Version Launcher
REM Change directory to the location of this script (project root)
cd /d %~dp0

echo ===============================================
echo   Data Forge API - Ultra Performance Mode
echo ===============================================

REM Activate the virtual environment
echo Activating virtual environment...
call .venv\Scripts\activate

REM Create required directories
if not exist "data" mkdir data
if not exist "temp" mkdir temp
if not exist "logs" mkdir logs

REM Run the refactored FastAPI application
echo Starting Data Forge API (Refactored)...
echo Target Performance: 10M+ rows/second
echo.
echo Available at: http://localhost:8080
echo API Docs: http://localhost:8080/docs
echo Health Check: http://localhost:8080/health
echo Performance Info: http://localhost:8080/performance
echo.

python app\main_refactored.py

REM Keep the command prompt open if there's an error
if %ERRORLEVEL% neq 0 (
    echo.
    echo ===============================================
    echo   Application exited with error code: %ERRORLEVEL%
    echo ===============================================
    pause
) 