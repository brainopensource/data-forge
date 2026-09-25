@echo off
REM Data Forge - Frontend Launcher (Windows)
REM Launches only the Tk/CustomTkinter UI
cd /d %~dp0

echo ===============================================
echo   Data Forge - Frontend UI
echo ===============================================
echo   Platform: Windows
echo   Frontend: Tk/CustomTkinter
echo   Requires backend running at http://localhost:8080
echo ===============================================

REM Environment
set PYTHONUTF8=1
set PYTHONUNBUFFERED=1

echo Activating virtual environment...
call .venv13\Scripts\activate

echo Launching UI...
python -m frontend.main

echo.
echo ===============================================
echo   UI closed
echo ===============================================
pause
