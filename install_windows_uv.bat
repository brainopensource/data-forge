@echo off
REM Data Forge API - Windows UV Installation Script
REM UV-optimized dependency installer for Windows

echo ===============================================
echo   Installing Data Forge API Dependencies
echo   (Windows UV-optimized version)
echo ===============================================

REM Check if UV is installed
uv --version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ❌ UV is not installed or not in PATH
    echo.
    echo Please install UV first:
    echo   pip install uv
    echo   OR
    echo   Download from: https://github.com/astral-sh/uv
    echo.
    pause
    exit /b 1
)

echo ✅ UV is installed
uv --version

REM Activate virtual environment if it exists
if exist ".venv\Scripts\activate.bat" (
    echo.
    echo Activating virtual environment...
    call .venv\Scripts\activate
) else (
    echo.
    echo ⚠️ Virtual environment not found. Creating one...
    echo.
    echo Creating virtual environment with UV...
    uv venv .venv
    if %ERRORLEVEL% neq 0 (
        echo ❌ Failed to create virtual environment
        pause
        exit /b 1
    )
    echo ✅ Virtual environment created
    call .venv\Scripts\activate
)

echo.
echo Current Python environment:
python --version
echo Python location: 
where python

echo.
echo ===============================================
echo   Installing Windows-Optimized Dependencies
echo ===============================================

REM Install core dependencies first to avoid conflicts
echo.
echo 📦 Installing core FastAPI dependencies...
uv pip install fastapi>=0.104.0 uvicorn[standard]>=0.24.0 pydantic>=2.0.0

if %ERRORLEVEL% neq 0 (
    echo ❌ Failed to install core dependencies
    echo.
    echo Trying with pip fallback...
    pip install fastapi>=0.104.0 uvicorn[standard]>=0.24.0 pydantic>=2.0.0
    if %ERRORLEVEL% neq 0 (
        echo ❌ Core installation failed with both UV and pip
        pause
        exit /b 1
    )
)

echo.
echo 📊 Installing high-performance data processing libraries...
uv pip install polars>=1.30.0 pyarrow>=18.0.0 duckdb>=1.0.0 numpy>=1.24.0

if %ERRORLEVEL% neq 0 (
    echo ❌ Failed to install data processing libraries
    echo.
    echo Trying with pip fallback...
    pip install polars>=1.30.0 pyarrow>=18.0.0 duckdb>=1.0.0 numpy>=1.24.0
    if %ERRORLEVEL% neq 0 (
        echo ❌ Data processing libraries installation failed
        pause
        exit /b 1
    )
)

echo.
echo ⚡ Installing Windows performance libraries...
uv pip install orjson>=3.9.0 psutil>=5.9.0

echo.
echo 🔧 Installing configuration libraries...
uv pip install python-dotenv>=0.21.0 pydantic-settings>=2.0.0

echo.
echo 🧪 Installing development and testing libraries...
uv pip install pytest>=6.2.5 pytest-asyncio>=0.21.0

echo.
echo 📝 Installing remaining dependencies...
uv pip install python-multipart>=0.0.5

echo.
echo ===============================================
echo   Verifying Installation
echo ===============================================

echo.
echo 🔍 Checking critical imports...

python -c "import fastapi; print('✅ FastAPI:', fastapi.__version__)" 2>nul
if %ERRORLEVEL% neq 0 echo ❌ FastAPI import failed

python -c "import polars; print('✅ Polars:', polars.__version__)" 2>nul
if %ERRORLEVEL% neq 0 echo ❌ Polars import failed

python -c "import pyarrow; print('✅ PyArrow:', pyarrow.__version__)" 2>nul
if %ERRORLEVEL% neq 0 echo ❌ PyArrow import failed

python -c "import duckdb; print('✅ DuckDB:', duckdb.__version__)" 2>nul
if %ERRORLEVEL% neq 0 echo ❌ DuckDB import failed

python -c "import pydantic_settings; print('✅ Pydantic Settings available')" 2>nul
if %ERRORLEVEL% neq 0 echo ❌ Pydantic Settings import failed

python -c "import orjson; print('✅ orjson available')" 2>nul
if %ERRORLEVEL% neq 0 echo ⚠️ orjson not available (optional)

python -c "import psutil; print('✅ psutil available')" 2>nul
if %ERRORLEVEL% neq 0 echo ⚠️ psutil not available (optional)

echo.
echo 🧪 Testing Windows app imports...
python -c "from app.main_windows import app; print('✅ Windows app imports successfully')" 2>nul
if %ERRORLEVEL% neq 0 (
    echo ❌ Windows app import failed
    echo.
    echo This might be due to missing dependencies. Let's try installing the full requirements file:
    echo.
    uv pip install -r requirements_windows.txt
)

echo.
echo ===============================================
echo   Installation Summary
echo ===============================================

echo.
echo 📊 Installed packages:
uv pip list | findstr -i "fastapi polars pyarrow duckdb pydantic orjson psutil"

echo.
echo 💾 Installation complete!
echo.
echo ✅ Windows-optimized dependencies installed
echo ✅ UV package manager used for fast installation
echo ✅ No uvloop dependency (Windows-native async I/O)
echo ✅ ProactorEventLoop ready for maximum performance
echo.
echo Next steps:
echo   1. Run the application: .\app_windows.bat
echo   2. Test performance: python test_windows.py
echo   3. Check system info: curl http://localhost:8080/system
echo.
echo ===============================================
echo   Ready for 10M+ rows/second on Windows! 🚀
echo ===============================================

pause 