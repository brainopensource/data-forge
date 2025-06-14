@echo off
REM Change directory to your project root if the .bat file is not there
REM cd /d "E:\Code\ReactFastAPI\react-fast-V12"
REM (Uncomment and modify the line above if your .bat file is not in the project root)

REM Activate the virtual environment
call .venv\Scripts\activate

REM Run the FastAPI application using uvicorn
uvicorn app.main:app --reload --host 0.0.0.0 --port 8080

REM Optional: Keep the command prompt open after the app exits (e.g., if it crashes)
pause