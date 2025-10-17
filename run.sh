#!/bin/bash

# 1 процесс на s1.ripple.com
LOG_FILE_PATH="logs/platform.1.log" ./bin/platform-cli backfill -verbose -from 98900000 -to 99116667 -server wss://s1.ripple.com &

# 2 процесса на s2.ripple.com
LOG_FILE_PATH="logs/platform.2.log" ./bin/platform-cli backfill -verbose -from 99116667 -to 99333334 -server wss://s2.ripple.com &
LOG_FILE_PATH="logs/platform.3.log" ./bin/platform-cli backfill -verbose -from 99333334 -to 99550000 -server wss://s2.ripple.com &

echo "Запущено 3 процесса backfill (1 на s1, 2 на s2)"
echo "PID процессов: $(jobs -p | tr '\n' ' ')"
wait