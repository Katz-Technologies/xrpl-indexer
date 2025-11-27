#!/bin/bash
set -e

echo "[Kafka Init] Waiting for Kafka..."
sleep 5

echo "[Kafka Init] Creating topics..."
TOPICS=(
  xrpl-platform-transactions
  xrpl-platform-ch-moneyflows
)

for topic in "${TOPICS[@]}"; do
  kafka-topics --create --if-not-exists \
    --topic "$topic" \
    --bootstrap-server kafka-broker1:29092 \
    --partitions 1 \
    --replication-factor 1 || true
done

echo "[Kafka Init] ✅ All topics created."
