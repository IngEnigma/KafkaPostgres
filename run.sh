#!/bin/bash

/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties &

sleep 15

/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &

sleep 20

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic crime_records

python3 /app/producer/kafkaProducer.py &
python3 /app/consumer/kafkaConsumer.py &

tail -f /dev/null
