echo "Starting Zookeeper..."
/opt/kafka_2.12-3.7.2/bin/zookeeper-server-start.sh /opt/kafka_2.12-3.7.2/config/zookeeper.properties &

echo "Starting Kafka..."
/opt/kafka_2.12-3.7.2/bin/kafka-server-start.sh /opt/kafka_2.12-3.7.2/config/server.properties &
