#!/bin/bash

kafka_2.12-3.7.2/bin/zookeeper-server-start.sh kafka_2.12-3.7.2/config/zookeeper.properties &

sleep 5

kafka_2.12-3.7.2/bin/kafka-server-start.sh kafka_2.12-3.7.2/config/server.properties &

sleep 10

kafka_2.12-3.7.2/bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic crime || \
    echo "El topic crime_records ya existe o no se pudo crear"

sleep 5

python3 consumer.py &
 
 # Esperar un momento antes de iniciar el productor
sleep 5
 
 # Iniciar el productor
python3 producer.py
 
 # Mantener el contenedor en ejecución
tail -f /dev/null
