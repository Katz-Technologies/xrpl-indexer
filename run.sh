#!/bin/bash

# Запуск оркестратора в фоне
# Оркестратор будет управлять 3 воркерами для бэкфиллинга
./bin/platform-orchestrator --workers 2 --from 100000000 --to 100359800 --servers "wss://s1.ripple.com/,wss://s2.ripple.com/" --check-interval 30s --verbose --redistribute-threshold 5000 > logs/backfill-orchestrator.log 2>&1 &

ORCHESTRATOR_PID=$!

echo "Оркестратор запущен в фоне (PID: $ORCHESTRATOR_PID)"
echo "Логи оркестратора бэкфиллинга: logs/backfill-orchestrator.log"
echo "Логи воркеров бэкфиллинга: logs/backfill-worker-*.log"
echo "Для просмотра логов в реальном времени:"
echo "  tail -f logs/backfill-orchestrator.log"

# Сохранить PID в файл для удобства
echo $ORCHESTRATOR_PID > logs/orchestrator.pid
echo "PID сохранен в logs/orchestrator.pid"