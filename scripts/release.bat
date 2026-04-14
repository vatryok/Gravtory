@echo off
REM ============================================================================
REM Gravtory Release Packaging Script (Windows)
REM Creates a clean, organized release folder ready for distribution.
REM
REM Usage:
REM   scripts\release.bat                Build release for current version
REM   scripts\release.bat --skip-build   Skip build, use existing dist\
REM   scripts\release.bat --help         Show this help
REM ============================================================================
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."
set "_STEP=0"
set "_TOTAL=4"
set "SKIP_BUILD=0"

cd /d "%PROJECT_DIR%"

REM ── Parse arguments ────────────────────────────────────────────
:parse_args
if "%~1"=="" goto :start
if "%~1"=="--skip-build" (set "SKIP_BUILD=1" & shift & goto :parse_args)
if "%~1"=="-h" goto :usage
if "%~1"=="--help" goto :usage
echo   [FAIL] Unknown argument: %~1 (try --help)
exit /b 1

:start
REM Detect version
set "VERSION="
for /f "tokens=2 delims=^""" %%v in ('findstr /r "^version" pyproject.toml 2^>nul') do set "VERSION=%%v"
if "!VERSION!"=="" (
    python -c "import tomllib,pathlib;d=tomllib.loads(pathlib.Path('pyproject.toml').read_text());print(d.get('project',{}).get('version','0.0.0'))" 2>nul > "%TEMP%\grav_ver.txt"
    set /p VERSION=<"%TEMP%\grav_ver.txt"
    del "%TEMP%\grav_ver.txt" >nul 2>&1
)
if "!VERSION!"=="" set "VERSION=0.0.0"

set "RELEASE_NAME=gravtory-!VERSION!"
set "RELEASE_DIR=%PROJECT_DIR%\release\!RELEASE_NAME!"

echo.
echo   Gravtory Release
echo   ────────────────────────────────────────────────────────────
echo   Version:   !VERSION!
echo   Output:    !RELEASE_DIR!\
echo   ────────────────────────────────────────────────────────────

REM ── Step 1: Build ──────────────────────────────────────────────
set /a _STEP+=1
echo.
echo [!_STEP!/%_TOTAL%] Running full build pipeline

if "%SKIP_BUILD%"=="1" (
    echo   [WARN] Skipped (--skip-build)
    if not exist "%PROJECT_DIR%\dist\*.whl" (
        echo   [FAIL] No artifacts in dist\. Run without --skip-build first.
        exit /b 1
    )
    echo   [OK] Using existing dist\ artifacts
) else (
    call "%SCRIPT_DIR%build.bat" --full
    if errorlevel 1 (
        echo   [FAIL] Build pipeline failed.
        exit /b 1
    )
    echo   [OK] Build pipeline passed
)

REM ── Step 2: Assemble ───────────────────────────────────────────
set /a _STEP+=1
echo.
echo [!_STEP!/%_TOTAL%] Assembling release directory

if exist "!RELEASE_DIR!" rmdir /s /q "!RELEASE_DIR!"
mkdir "!RELEASE_DIR!"
copy /y "%PROJECT_DIR%\dist\*.tar.gz" "!RELEASE_DIR!\" >nul 2>&1
copy /y "%PROJECT_DIR%\dist\*.whl" "!RELEASE_DIR!\" >nul 2>&1

set "ARTIFACT_COUNT=0"
for %%f in ("!RELEASE_DIR!\*.tar.gz" "!RELEASE_DIR!\*.whl") do set /a ARTIFACT_COUNT+=1
if !ARTIFACT_COUNT! equ 0 (
    echo   [FAIL] No build artifacts found.
    exit /b 1
)
echo   [OK] !ARTIFACT_COUNT! artifact(s) copied

REM ── Step 3: Checksums ──────────────────────────────────────────
set /a _STEP+=1
echo.
echo [!_STEP!/%_TOTAL%] Generating SHA-256 checksums

cd /d "!RELEASE_DIR!"
(for %%f in (*.tar.gz *.whl) do (
    echo ── %%f ──
    certutil -hashfile "%%f" SHA256 | findstr /v "hash certutil"
)) > CHECKSUMS.sha256 2>nul
cd /d "%PROJECT_DIR%"
echo   [OK] CHECKSUMS.sha256 written

REM ── Step 4: Release notes ──────────────────────────────────────
set /a _STEP+=1
echo.
echo [!_STEP!/%_TOTAL%] Generating release notes

(
echo # Gravtory !VERSION!
echo.
echo Released: %date%
echo.
echo ## Install from PyPI
echo.
echo ```bash
echo pip install gravtory
echo pip install gravtory[postgres]
echo pip install gravtory[all]
echo ```
echo.
echo ## Install from Wheel
echo.
echo ```bash
echo python -m venv .venv
echo .venv\Scripts\activate.bat
echo pip install !RELEASE_NAME!-py3-none-any.whl
echo ```
echo.
echo ## Checksums
echo.
echo See `CHECKSUMS.sha256` for SHA-256 verification.
echo.
echo ## Links
echo.
echo - Repository: https://github.com/vatryok/gravtory
echo - Changelog:  https://github.com/vatryok/gravtory/blob/main/CHANGELOG.md
echo - Issues:     https://github.com/vatryok/gravtory/issues
echo - PyPI:       https://pypi.org/project/gravtory/
) > "!RELEASE_DIR!\RELEASE_NOTES.md"
echo   [OK] RELEASE_NOTES.md generated

REM ── Summary ────────────────────────────────────────────────────
echo.
echo   ────────────────────────────────────────────────────────────
echo     RELEASE READY: !RELEASE_NAME!
echo   ────────────────────────────────────────────────────────────
echo.
echo   Contents:
dir /b "!RELEASE_DIR!\" 2>nul | findstr /v /r "^$"
echo.
echo   Location: !RELEASE_DIR!\
echo.
goto :end

:usage
echo Usage: %~nx0 [--skip-build] [--help]
echo.
echo   --skip-build   Skip the full build; package existing dist\ artifacts
exit /b 0

:end
endlocal
