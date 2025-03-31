#!/bin/bash

/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &

/opt/kafka/wait-for-it.sh localhost:9092 --timeout=60 -- echo "Kafka está listo"

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic crime_records

sleep 10

python3 /app/producer/kafkaProducer.py &
python3 /app/consumer/kafkaConsumer.py &

tail -f /dev/null
