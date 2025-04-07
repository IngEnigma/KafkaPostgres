#!/bin/bash

echo "🟡 Esperando a que Kafka esté listo..."

kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic crimes \
  --partitions 1 \
  --replication-factor 1

echo "✅ Tópico 'crimes' creado"
