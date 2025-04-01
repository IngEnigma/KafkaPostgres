#!/bin/bash

# Inicia servicios base
/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties &
while ! nc -z localhost 2181; do sleep 0.1; done

/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &
while ! nc -z localhost 9092; do sleep 0.1; done

# Crea el tema si no existe
/opt/kafka/bin/kafka-topics.sh --create \
  --topic crimes \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 || true

# Inicia el consumidor en segundo plano
python3 /opt/kafka/consumer.py &

# Ejecuta el productor (en primer plano)
python3 /opt/kafka/producer.py

# Mantén el contenedor corriendo
tail -f /dev/null
