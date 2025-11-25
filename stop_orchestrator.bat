@echo off
setlocal

echo Stopping orchestrator...

rem Проверяем, есть ли PID файл
if exist "logs\orchestrator.pid" (
    for /f "tokens=*" %%a in (logs\orchestrator.pid) do set ORCHESTRATOR_PID=%%a
    echo Found orchestrator PID: %ORCHESTRATOR_PID%
    
    rem Пытаемся остановить через taskkill
    taskkill /PID %ORCHESTRATOR_PID% /T /F >nul 2>&1
    if errorlevel 1 (
        echo Process with PID %ORCHESTRATOR_PID% not found or already stopped
    ) else (
        echo Successfully stopped orchestrator process %ORCHESTRATOR_PID%
    )
) else (
    echo PID file not found, trying to find orchestrator process...
    rem Ищем процесс по имени
    taskkill /IM platform-orchestrator.exe /T /F >nul 2>&1
    if errorlevel 1 (
        echo No running orchestrator process found
    ) else (
        echo Successfully stopped orchestrator process
    )
)

rem Создаем файл остановки на всякий случай
echo. > stop.orchestrator

rem Удаляем PID файл
if exist "logs\orchestrator.pid" del "logs\orchestrator.pid"

echo.
echo Orchestrator stop command sent.
echo If orchestrator is still running, it should stop within 5-10 seconds.

endlocal

