@echo off
setlocal

set SERVICE_NAME=platform
set BIN_DIR=bin

echo Creating bin directory...
if not exist "%BIN_DIR%" mkdir "%BIN_DIR%"

echo Building platform-server...
go build -o %BIN_DIR%\%SERVICE_NAME%-server.exe main.go
if errorlevel 1 (
    echo Failed to build platform-server
    exit /b 1
)

echo Building platform-cli...
go build -o %BIN_DIR%\%SERVICE_NAME%-cli.exe .\cmd\cli
if errorlevel 1 (
    echo Failed to build platform-cli
    exit /b 1
)

echo Building platform-orchestrator...
go build -o %BIN_DIR%\%SERVICE_NAME%-orchestrator.exe .\cmd\orchestrator
if errorlevel 1 (
    echo Failed to build platform-orchestrator
    exit /b 1
)

echo.
echo Build completed successfully!
echo Executables are in %BIN_DIR% directory:
echo   - %BIN_DIR%\%SERVICE_NAME%-server.exe
echo   - %BIN_DIR%\%SERVICE_NAME%-cli.exe
echo   - %BIN_DIR%\%SERVICE_NAME%-orchestrator.exe

endlocal

