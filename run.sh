#!/bin/bash

# Iniciar Zookeeper en segundo plano
kafka_2.12-3.7.2/bin/zookeeper-server-start.sh kafka_2.12-3.7.2/config/zookeeper.properties &

# Esperar a que Zookeeper esté listo
sleep 5

# Iniciar Kafka en segundo plano
kafka_2.12-3.7.2/bin/kafka-server-start.sh kafka_2.12-3.7.2/config/server.properties &

# Esperar a que Kafka esté listo
sleep 10

# Crear el topic si no existe
kafka_2.12-3.7.2/bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic crime_records || \
    echo "El topic crime_records ya existe o no se pudo crear"

# Iniciar el consumidor en segundo plano
python3 consumer.py &

# Esperar un momento antes de iniciar el productor
sleep 5

# Iniciar el productor
python3 producer.py

# Mantener el contenedor en ejecución
tail -f /dev/null
