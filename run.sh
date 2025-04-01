#!/bin/bash

# Configura directorio persistente para Zookeeper
mkdir -p /var/lib/zookeeper
chown -R $(whoami) /var/lib/zookeeper

# Inicia Zookeeper con configuración de memoria
export KAFKA_HEAP_OPTS="-Xms512M -Xmx1G"
/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties &

# Espera conexión Zookeeper
while ! nc -z localhost 2181; do sleep 1; done

# Inicia Kafka con configuración de memoria
export KAFKA_HEAP_OPTS="-Xms1G -Xmx2G"
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &

# Espera conexión Kafka
while ! nc -z localhost 9092; do sleep 1; done

# Crea topic con configuración optimizada
/opt/kafka/bin/kafka-topics.sh --create \
  --topic crimes \
  --bootstrap-server localhost:9092 \
  --config max.message.bytes=10485760 \
  --config retention.ms=604800000 \
  --partitions 1 \
  --replication-factor 1 || true

# Inicia servicios Python
python3 /opt/kafka/consumer.py &
python3 /opt/kafka/producer.py

# Mantén el contenedor activo
tail -f /dev/null
