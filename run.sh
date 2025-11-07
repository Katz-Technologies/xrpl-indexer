#!/bin/bash

# 1 процесс на s1.ripple.com
LOG_FILE_PATH="logs/platform.1.log" ./bin/platform-cli backfill -verbose -from 99119667 -to 99119670 -server wss://s1.ripple.com &


echo "Запущено 3 процесса backfill (1 на s1, 2 на s2)"
echo "PID процессов: $(jobs -p | tr '\n' ' ')"
wait