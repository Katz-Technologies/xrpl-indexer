@echo off
setlocal enabledelayedexpansion

rem === Запуск оркестратора в фоне ===
rem Оркестратор будет управлять воркерами для бэкфиллинга

rem Создаем директорию для логов если её нет
if not exist "logs" mkdir "logs"

rem Запускаем оркестратор в фоне и получаем его PID
start /B "" .\bin\platform-orchestrator.exe --workers 2 --from 100420000 --to 100434000 --servers "wss://s1.ripple.com/,wss://s2.ripple.com/" --check-interval 30s --verbose --redistribute-threshold 5000 > logs\orchestrator.log 2>&1

rem Ждем немного, чтобы процесс успел запуститься
timeout /t 1 /nobreak >nul

rem Получаем PID последнего запущенного процесса
set ORCHESTRATOR_PID=
for /f "tokens=2" %%a in ('tasklist /FI "IMAGENAME eq platform-orchestrator.exe" /FO LIST ^| findstr /I "PID"') do (
    set "ORCHESTRATOR_PID=%%a"
)
rem Удаляем пробелы
set "ORCHESTRATOR_PID=!ORCHESTRATOR_PID: =!"

echo Оркестратор запущен в фоне (PID: %ORCHESTRATOR_PID%)
echo Логи оркестратора: logs\orchestrator.log
echo Логи воркеров: logs\orchestrator-worker-*.log
echo Для просмотра логов в реальном времени:
echo   powershell Get-Content logs\orchestrator.log -Wait -Tail 50

rem Сохранить PID в файл для удобства
if not "!ORCHESTRATOR_PID!"=="" (
    echo !ORCHESTRATOR_PID!> logs\orchestrator.pid
    echo PID сохранен в logs\orchestrator.pid
) else (
    echo WARNING: Could not determine orchestrator PID
)

endlocal

