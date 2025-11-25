@echo off
setlocal enabledelayedexpansion

echo ========================================
echo Orchestrator Status Check
echo ========================================
echo.

rem Проверяем процессы platform-orchestrator.exe
echo [1] Checking orchestrator processes...
tasklist /FI "IMAGENAME eq platform-orchestrator.exe" /FO TABLE 2>nul | findstr /I "platform-orchestrator.exe" >nul
if errorlevel 1 (
    echo    Status: NOT RUNNING
    echo    No orchestrator process found
) else (
    echo    Status: RUNNING
    echo    Process details:
    tasklist /FI "IMAGENAME eq platform-orchestrator.exe" /FO TABLE
)

echo.

rem Проверяем процессы platform-cli.exe (воркеры)
echo [2] Checking worker processes (platform-cli.exe)...
tasklist /FI "IMAGENAME eq platform-cli.exe" /FO TABLE 2>nul | findstr /I "platform-cli.exe" >nul
if errorlevel 1 (
    echo    Status: NO WORKERS RUNNING
    echo    No worker processes found
) else (
    echo    Status: WORKERS RUNNING
    echo    Worker processes:
    tasklist /FI "IMAGENAME eq platform-cli.exe" /FO TABLE
    echo.
    echo    Worker count:
    for /f %%a in ('tasklist /FI "IMAGENAME eq platform-cli.exe" /FO CSV ^| find /c "platform-cli.exe"') do echo    Total: %%a workers
)

echo.

rem Проверяем PID файл
echo [3] Checking PID file...
if exist "logs\orchestrator.pid" (
    set ORCHESTRATOR_PID=
    for /f "usebackq tokens=*" %%a in ("logs\orchestrator.pid") do set "ORCHESTRATOR_PID=%%a"
    rem Удаляем пробелы в начале и конце
    set "ORCHESTRATOR_PID=!ORCHESTRATOR_PID: =!"
    echo    PID file exists: logs\orchestrator.pid
    if not "!ORCHESTRATOR_PID!"=="" (
        echo    Stored PID: !ORCHESTRATOR_PID!
        
        rem Проверяем, существует ли процесс с этим PID
        tasklist /FI "PID eq !ORCHESTRATOR_PID!" 2>nul | findstr /C:"!ORCHESTRATOR_PID!" >nul
        if errorlevel 1 (
            echo    WARNING: Process with PID !ORCHESTRATOR_PID! not found (stale PID file)
        ) else (
            echo    Process with PID !ORCHESTRATOR_PID! is running
        )
    ) else (
        echo    WARNING: PID file is empty
    )
) else (
    echo    PID file not found
)

echo.

rem Проверяем файл остановки
echo [4] Checking stop file...
if exist "stop.orchestrator" (
    echo    Stop file exists: stop.orchestrator
    echo    WARNING: Stop file is present - orchestrator should stop soon
) else (
    echo    Stop file not found (normal)
)

echo.

rem Проверяем логи
echo [5] Recent log activity...
if exist "logs\orchestrator.log" (
    echo    Orchestrator log: logs\orchestrator.log
    for %%A in ("logs\orchestrator.log") do (
        echo    File size: %%~zA bytes
        echo    Last modified: %%~tA
    )
    echo.
    echo    Last 3 lines of orchestrator log:
    powershell -Command "Get-Content 'logs\orchestrator.log' -Tail 3"
) else (
    echo    Orchestrator log file not found
)

echo.

rem Проверяем логи воркеров
echo [6] Worker log files...
set WORKER_COUNT=0
for %%f in (logs\orchestrator-worker-*.log) do (
    set /a WORKER_COUNT+=1
    echo    Found: %%~nxf
)
if %WORKER_COUNT%==0 (
    echo    No worker log files found
) else (
    echo    Total worker log files: %WORKER_COUNT%
)

echo.
echo ========================================
echo Summary
echo ========================================

rem Подсчитываем процессы
set ORCH_COUNT=0
for /f %%a in ('tasklist /FI "IMAGENAME eq platform-orchestrator.exe" /FO CSV 2^>nul ^| find /c "platform-orchestrator.exe"') do set ORCH_COUNT=%%a

set WORKER_COUNT=0
for /f %%a in ('tasklist /FI "IMAGENAME eq platform-cli.exe" /FO CSV 2^>nul ^| find /c "platform-cli.exe"') do set WORKER_COUNT=%%a

echo Orchestrator processes: %ORCH_COUNT%
echo Worker processes: %WORKER_COUNT%

if %ORCH_COUNT% gtr 0 (
    echo.
    echo Orchestrator is RUNNING
) else (
    echo.
    echo Orchestrator is NOT RUNNING
)

if %WORKER_COUNT% gtr 0 (
    echo Workers are RUNNING
) else (
    echo Workers are NOT RUNNING
)

echo.
echo ========================================
echo.

endlocal

