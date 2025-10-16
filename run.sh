#!/bin/bash

LOG_FILE_PATH="logs/platform.1.log" ./bin/platform-cli backfill -verbose -from 98900000 -to 99062500 -server wss://s1.ripple.com &
LOG_FILE_PATH="logs/platform.2.log" ./bin/platform-cli backfill -verbose -from 99062500 -to 99225000 -server wss://s1.ripple.com &

LOG_FILE_PATH="logs/platform.3.log" ./bin/platform-cli backfill -verbose -from 99225000 -to 99387500 -server wss://s2.ripple.com &
LOG_FILE_PATH="logs/platform.4.log" ./bin/platform-cli backfill -verbose -from 99387500 -to 99550000 -server wss://s2.ripple.com &

echo "Запущено 4 процесса backfill"
echo "PID процессов: $(jobs -p | tr '\n' ' ')"
wait