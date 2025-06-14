@echo off
REM Change directory to the location of this script (project root)
cd /d %~dp0

REM Remove __pycache__ directories (CMD compatible)
for /d /r %%d in (__pycache__) do if exist "%%d" rd /s /q "%%d"

pause