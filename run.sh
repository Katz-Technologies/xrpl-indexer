#!/bin/bash

# 1 процесс на s1.ripple.com
LOG_FILE_PATH="logs/platform.1.log" ./bin/platform-cli backfill -verbose -from 98900000 -to 99119667 -server wss://s1.ripple.com &


echo "Запущено 3 процесса backfill (1 на s1, 2 на s2)"
echo "PID процессов: $(jobs -p | tr '\n' ' ')"
wait