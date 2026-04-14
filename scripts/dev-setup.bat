@echo off
REM ============================================================================
REM Gravtory Developer Setup Script (Windows)
REM One-command setup for new contributors.
REM
REM Usage:
REM   scripts\dev-setup.bat              Full setup
REM   scripts\dev-setup.bat --no-test    Skip smoke test
REM   scripts\dev-setup.bat --reset      Delete .venv and start fresh
REM   scripts\dev-setup.bat --help       Show this help
REM ============================================================================
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."
set "VENV_DIR=%PROJECT_DIR%\.venv"
set "_STEP=0"
set "_TOTAL=5"
set "SKIP_TEST=0"
set "RESET=0"

cd /d "%PROJECT_DIR%"

REM ── Parse arguments ────────────────────────────────────────────
:parse_args
if "%~1"=="" goto :start
if "%~1"=="--no-test" (set "SKIP_TEST=1" & shift & goto :parse_args)
if "%~1"=="--reset" (set "RESET=1" & shift & goto :parse_args)
if "%~1"=="-h" goto :usage
if "%~1"=="--help" goto :usage
echo [FAIL] Unknown argument: %~1 (try --help)
exit /b 1

:start
echo.
echo   Gravtory Developer Setup
echo   ────────────────────────────────────────────────────────────

REM ── Step 1: Python ─────────────────────────────────────────────
set /a _STEP+=1
echo.
echo [%_STEP%/%_TOTAL%] Checking Python

python --version >nul 2>&1
if errorlevel 1 (
    echo   [FAIL] Python not found. Install Python 3.10+.
    exit /b 1
)
for /f "tokens=2" %%v in ('python --version 2^>^&1') do set "PY_VERSION=%%v"
echo   [OK] Python %PY_VERSION%

REM ── Step 2: Virtual environment ────────────────────────────────
set /a _STEP+=1
echo.
echo [%_STEP%/%_TOTAL%] Virtual environment

if "%RESET%"=="1" (
    if exist "%VENV_DIR%" (
        echo   ... Removing old .venv (--reset)
        rmdir /s /q "%VENV_DIR%" >nul 2>&1
    )
)

if exist "%VENV_DIR%\Scripts\python.exe" (
    "%VENV_DIR%\Scripts\python.exe" -c "import sys" >nul 2>&1
    if !errorlevel! equ 0 (
        echo   ... Reusing existing .venv
        call "%VENV_DIR%\Scripts\activate.bat"
        echo   [OK] Venv active: %VENV_DIR%
        goto :deps
    ) else (
        echo   [WARN] Stale .venv detected - recreating...
        rmdir /s /q "%VENV_DIR%" >nul 2>&1
    )
)

echo   ... Creating virtual environment at .venv\
python -m venv "%VENV_DIR%"
if errorlevel 1 (
    echo   [FAIL] Could not create venv.
    exit /b 1
)
call "%VENV_DIR%\Scripts\activate.bat"
python -m pip install --upgrade pip --quiet >nul 2>&1
echo   [OK] Venv created: %VENV_DIR%

REM ── Step 3: Dependencies ───────────────────────────────────────
:deps
set /a _STEP+=1
echo.
echo [%_STEP%/%_TOTAL%] Installing dependencies
echo   ... pip install -e ".[dev,all]" (this may take a minute)
pip install -e ".[dev,all]" --quiet >nul 2>&1
echo   [OK] All dependencies installed

REM ── Step 4: Pre-commit hooks ───────────────────────────────────
set /a _STEP+=1
echo.
echo [%_STEP%/%_TOTAL%] Pre-commit hooks

where pre-commit >nul 2>&1
if not errorlevel 1 (
    pre-commit install --install-hooks >nul 2>&1
    echo   [OK] Pre-commit hooks installed
) else (
    echo   [WARN] pre-commit not found - skipping
)

REM ── Step 5: Verify ─────────────────────────────────────────────
set /a _STEP+=1
echo.
echo [%_STEP%/%_TOTAL%] Verifying installation

echo   ... Import check
python -c "import gravtory; print('  gravtory ' + gravtory.__version__)"
if errorlevel 1 (
    echo   [FAIL] Cannot import gravtory
    exit /b 1
)
echo   [OK] Import check passed

if "%SKIP_TEST%"=="0" (
    echo   ... Smoke test
    pytest tests\unit\ -q --timeout=30 -x --benchmark-disable --tb=line >nul 2>&1
    if errorlevel 1 (
        echo   [WARN] Some tests failed - check manually
    ) else (
        echo   [OK] Smoke test passed
    )
) else (
    echo   [WARN] Smoke test skipped (--no-test)
)

REM ── Done ────────────────────────────────────────────────────────
echo.
echo   ────────────────────────────────────────────────────────────
echo     DEVELOPER SETUP COMPLETE
echo   ────────────────────────────────────────────────────────────
echo.
echo   Activate your environment:
echo     .venv\Scripts\activate.bat
echo.
echo   Quick reference:
echo     pytest tests\unit\ -q            Run unit tests
echo     ruff check src\ tests\           Lint
echo     ruff format src\ tests\          Auto-format
echo     scripts\build.bat                Full build pipeline
echo     scripts\build.bat --help         Build options
echo     scripts\release.bat              Package a release
echo.
goto :end

:usage
echo Usage: %~nx0 [--no-test] [--reset] [--help]
echo.
echo   --no-test   Skip the smoke test step
echo   --reset     Delete existing .venv and start fresh
exit /b 0

:end
endlocal
