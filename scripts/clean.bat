@echo off
REM Change directory to your project root if the .bat file is not there
REM cd /d "E:\Code\ReactFastAPI\react-fast-V12"
REM (Uncomment and modify the line above if your .bat file is not in the project root)

REM Remove __pycache__ directories
for /d /r . %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"
