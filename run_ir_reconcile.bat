@echo off
REM earnings_agent -- weekly IR-signup reconcile.
REM Ticks off any TickTick task whose company has STARTED sending IR mail, so the
REM signup lists close themselves instead of rotting as JP works through them.
REM CRLF line endings are required: cmd.exe mis-parses LF-only .bat files.
REM %~dp0 keeps this portable -- no hardcoded user path.
REM No env gate here on purpose: a scheduled task does NOT inherit shell env vars,
REM and TICKTICK_ACCESS_TOKEN is loaded from .env on import (verified 2026-08-05).
setlocal
set "PROJECT_DIR=%~dp0"
set "PYTHON=%LOCALAPPDATA%\Programs\Python\Python314\python.exe"
set "PYTHONIOENCODING=utf-8"
cd /d "%PROJECT_DIR%"

if not exist "%PYTHON%" (
  echo Python not found at %PYTHON%
  exit /b 3
)

"%PYTHON%" ir_ticktick.py --reconcile
set RC=%ERRORLEVEL%
echo ir reconcile exited with %RC%
exit /b %RC%
