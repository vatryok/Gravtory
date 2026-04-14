@echo off
REM ============================================================================
REM Gravtory Build Script (Windows)
REM Builds, tests, and packages the project for distribution.
REM
REM Usage:
REM   scripts\build.bat              Full build (lint + test + package)
REM   scripts\build.bat --quick      Package only (skip lint/test)
REM   scripts\build.bat --test       Run tests only
REM   scripts\build.bat --lint       Run lint + typecheck only
REM   scripts\build.bat --clean      Clean all build artifacts
REM   scripts\build.bat --help       Show this help
REM ============================================================================
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."
set "VENV_DIR=%PROJECT_DIR%\.venv"
set "DIST_DIR=%PROJECT_DIR%\dist"
set "BUILD_DIR=%PROJECT_DIR%\build"
set "_STEP=0"
set "_TOTAL=0"

cd /d "%PROJECT_DIR%"

if "%~1"=="" goto :full
if "%~1"=="--clean" goto :clean
if "%~1"=="--lint" goto :lint
if "%~1"=="--test" goto :test_only
if "%~1"=="--quick" goto :quick
if "%~1"=="--full" goto :full
if "%~1"=="-h" goto :usage
if "%~1"=="--help" goto :usage
goto :usage

REM ── Helpers ─────────────────────────────────────────────────────
:step
set /a _STEP+=1
echo.
echo [%_STEP%/%_TOTAL%] %~1
goto :eof

:check_venv
REM Prefer already-active venv
if defined VIRTUAL_ENV (
    echo   [OK] Using active venv: %VIRTUAL_ENV%
    goto :eof
)
REM Validate existing .venv
if exist "%VENV_DIR%\Scripts\python.exe" (
    "%VENV_DIR%\Scripts\python.exe" -c "import sys" >nul 2>&1
    if !errorlevel! equ 0 (
        echo   ... Activating existing .venv
        call "%VENV_DIR%\Scripts\activate.bat"
        echo   [OK] Venv active: %VENV_DIR%
        goto :eof
    ) else (
        echo   [WARN] Stale .venv detected - recreating...
        rmdir /s /q "%VENV_DIR%" >nul 2>&1
    )
)
echo   ... Creating virtual environment at .venv\
python -m venv "%VENV_DIR%"
if errorlevel 1 (
    echo   [FAIL] Could not create venv. Is python3 installed?
    exit /b 1
)
call "%VENV_DIR%\Scripts\activate.bat"
python -m pip install --upgrade pip --quiet >nul 2>&1
echo   [OK] Venv created: %VENV_DIR%
goto :eof

:install_deps
echo   ... Installing project with dev + all extras
pip install -e ".[dev,all]" --quiet >nul 2>&1
echo   [OK] Dependencies installed
goto :eof

REM ── Tasks ───────────────────────────────────────────────────────
:clean
set "_TOTAL=1"
call :step "Cleaning build artifacts"
if exist "%DIST_DIR%" rmdir /s /q "%DIST_DIR%"
if exist "%BUILD_DIR%" rmdir /s /q "%BUILD_DIR%"
if exist "%PROJECT_DIR%\.mypy_cache" rmdir /s /q "%PROJECT_DIR%\.mypy_cache"
if exist "%PROJECT_DIR%\.ruff_cache" rmdir /s /q "%PROJECT_DIR%\.ruff_cache"
if exist "%PROJECT_DIR%\.pytest_cache" rmdir /s /q "%PROJECT_DIR%\.pytest_cache"
if exist "%PROJECT_DIR%\htmlcov" rmdir /s /q "%PROJECT_DIR%\htmlcov"
if exist "%PROJECT_DIR%\.coverage" del /q "%PROJECT_DIR%\.coverage"
for /d /r "%PROJECT_DIR%" %%d in (__pycache__) do if exist "%%d" rmdir /s /q "%%d"
echo   [OK] Clean complete
goto :done

:lint
set "_TOTAL=1"
call :check_venv
call :install_deps
call :step "Linting and type-checking"
echo   ... ruff check
ruff check src\ tests\
if errorlevel 1 goto :fail
echo   [OK] Ruff lint passed
echo   ... ruff format --check
ruff format --check src\ tests\
if errorlevel 1 goto :fail
echo   [OK] Format check passed
echo   ... mypy
mypy src\gravtory\ --ignore-missing-imports
if errorlevel 1 goto :fail
echo   [OK] Type check passed
goto :done

:test_only
set "_TOTAL=1"
call :check_venv
call :install_deps
call :step "Running unit tests"
pytest tests\unit\ -q --timeout=60 --benchmark-disable --tb=short
if errorlevel 1 goto :fail
echo   [OK] Unit tests passed
goto :done

:package
call :step "Building sdist + wheel"
python -m build --version >nul 2>&1
if errorlevel 1 (
    echo   ... Installing build tooling
    pip install build twine --quiet >nul 2>&1
)
if exist "%DIST_DIR%" rmdir /s /q "%DIST_DIR%"
if exist "%BUILD_DIR%" rmdir /s /q "%BUILD_DIR%"
python -m build --outdir "%DIST_DIR%"
if errorlevel 1 goto :fail
echo   [OK] Package built
dir /b "%DIST_DIR%\*.tar.gz" "%DIST_DIR%\*.whl" 2>nul
call :step "Verifying package"
for %%f in ("%DIST_DIR%\*.whl") do pip install "%%f" --quiet --force-reinstall
python -c "import gravtory; print('  gravtory ' + gravtory.__version__)"
pip install -e ".[dev,all]" --quiet >nul 2>&1
echo   [OK] Package verification passed
goto :eof

:quick
set "_TOTAL=2"
call :check_venv
call :install_deps
call :package
goto :done

:full
set "_TOTAL=5"
call :check_venv
call :install_deps
call :step "Cleaning build artifacts"
call :clean
call :step "Linting and type-checking"
call :lint
call :step "Running unit tests"
call :test_only
call :package
echo.
echo   ============================================
echo     BUILD SUCCESSFUL
echo     Artifacts in: %DIST_DIR%\
echo   ============================================
goto :done

:fail
echo.
echo   [FAIL] Build failed.
exit /b 1

:usage
echo Usage: %~nx0 [--clean^|--lint^|--test^|--quick^|--full^|--help]
echo.
echo   --clean   Remove build artifacts
echo   --lint    Run linter + type checker
echo   --test    Run unit tests
echo   --quick   Package only (skip lint/test)
echo   --full    Full pipeline: clean + lint + test + package (default)
exit /b 0

:done
echo.
endlocal
