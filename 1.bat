@echo off
setlocal enabledelayedexpansion

rem === Путь до CLI ===
set "CLI_PATH=.\bin\platform-cli"

rem === Список готовых параметров ===
set "PARAMS[1]=-from 99300000 -to 99303000 -server wss://xrplcluster.com"
set "PARAMS[2]=-from 99303000 -to 99306000 -server wss://xrplcluster.com"
set "PARAMS[3]=-from 99306000 -to 99309000 -server wss://xrplcluster.com"
set "PARAMS[4]=-from 99309000 -to 99312000 -server wss://xrpl.ws"
set "PARAMS[5]=-from 99312000 -to 99315000 -server wss://xrpl.ws"
set "PARAMS[6]=-from 99315000 -to 99318000 -server wss://xrpl.ws"
set "PARAMS[7]=-from 99318000 -to 99321000 -server wss://s1.ripple.com"
set "PARAMS[8]=-from 99321000 -to 99324000 -server wss://s1.ripple.com"
set "PARAMS[9]=-from 99324000 -to 99327000 -server wss://s2.ripple.com"
set "PARAMS[10]=-from 99327000 -to 99330000 -server wss://s2.ripple.com"

echo Запуск задач по списку параметров...

for /L %%i in (1,1,10) do (
    echo.
    echo === Открываю окно для задачи %%i: !PARAMS[%%i]! ===
    start "Backfill %%i" cmd /k "%CLI_PATH% backfill -verbose !PARAMS[%%i]!"
)

echo.
echo === Все задачи запущены в отдельных окнах ===
pause