@echo off
REM Change directory to the location of this script (project root)
cd /d %~dp0

REM Activate the virtual environment
call .venv\Scripts\activate

REM (Optional) Install dependencies using uv if needed
REM uv pip install -r requirements.txt

REM Run the FastAPI application using uvicorn
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8080

REM Optional: Keep the command prompt open after the app exits (e.g., if it crashes)
pause