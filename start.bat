@echo off
echo ====================================
echo    Meme Engine - Starting...
echo ====================================
echo.

REM Check if .env exists
if not exist "config\.env" (
    echo [!] config\.env not found!
    echo [*] Copying .env.example to .env...
    copy "config\.env.example" "config\.env"
    echo [*] Please edit config\.env with your keys before running again.
    echo.
    pause
    exit /b 1
)

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [!] Python not found. Please install Python 3.10+
    pause
    exit /b 1
)

REM Install dependencies
echo [*] Checking dependencies...
pip install -r requirements.txt -q

echo.
echo Starting Meme Engine...
echo Dashboard will be at http://localhost:5001
echo.
python main.py
pause
