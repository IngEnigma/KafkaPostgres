#!/bin/bash

/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties &

sleep 5
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &

sleep 10
python3 producer.py &
python3 consumer.py &

tail -f /dev/null
