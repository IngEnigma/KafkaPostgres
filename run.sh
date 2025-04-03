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

tail -f /dev/null
