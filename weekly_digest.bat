@echo off
REM Scheduled wrapper for the weekly earnings digest.
REM Register with Windows Task Scheduler to run Sundays at 12:00 PM ET.
REM
REM Logs go to logs\weekly_digest_<date>.log so failures are debuggable after the fact.

setlocal

set "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

if not exist logs mkdir logs

for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value ^| find "="') do set dt=%%a
set "LOG_FILE=logs\weekly_digest_%dt:~0,8%.log"

python main.py --weekly-digest >> "%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

echo [%date% %time%] Exit code: %EXIT_CODE% >> "%LOG_FILE%"
exit /b %EXIT_CODE%
