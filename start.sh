#!/bin/bash

rpk redpanda start \
  --smp 1 \
  --memory 1G \
  --overprovisioned \
  --node-id 0 \
  --kafka-addr PLAINTEXT://0.0.0.0:9092 \
  --advertise-kafka-addr PLAINTEXT://localhost:9092 &

sleep 15

rpk topic create postgres-crimes || true

python producer_postgres.py &
python consumer_postgres.py &

tail -f /dev/null
