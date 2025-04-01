#!/bin/bash

# Inicia Zookeeper en segundo plano
/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties &

# Espera a que Zookeeper esté listo (más robusto que un sleep fijo)
while ! nc -z localhost 2181; do
  sleep 0.1
done

# Inicia Kafka en primer plano
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
