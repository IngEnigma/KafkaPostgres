#!/bin/bash

docker.redpanda.com/redpandadata/redpanda:v23.3.8 redpanda start \
  --smp 1 \
  --memory 1G \
  --overprovisioned \
  --kafka-addr PLAINTEXT://0.0.0.0:9092 &

sleep 20

rpk topic create postgres-crimes

python kafkaProducer.py &
python kafkaCconsumer.py &

tail -f /dev/null
