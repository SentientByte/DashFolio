@echo off
setlocal

REM Change to the directory where the batch file resides
cd /d "%~dp0"

REM Activate local virtual environment if it exists
if exist ".venv\Scripts\activate.bat" (
    call ".venv\Scripts\activate.bat"
)

REM Run the DashFolio Flask application directly
python app.py

REM Keep the window open if the script exits immediately
pause

endlocal
