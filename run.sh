#!/bin/bash

# Función para esperar por un puerto
wait_for_port() {
    local port=$1
    local max_retries=30
    local retry_interval=1
    local retries=0
    
    while ! nc -z localhost $port && [ $retries -lt $max_retries ]; do
        sleep $retry_interval
        retries=$((retries+1))
    done
    
    if [ $retries -eq $max_retries ]; then
        echo "Error: No se pudo conectar al puerto $port después de $max_retries intentos"
        exit 1
    fi
}

# Iniciar Kafka en modo KRaft
echo "Iniciando Kafka..."
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &

# Esperar a que Kafka esté listo
echo "Esperando a que Kafka esté listo..."
wait_for_port 9092
wait_for_port 9093
echo "Kafka está listo"

max_retries=5
retry_count=0
while [ $retry_count -lt $max_retries ]; do
    echo "Intentando crear topic (intento $((retry_count+1)))..."
    /opt/kafka/bin/kafka-topics.sh --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1 \
        --topic crime_records && break
        
    retry_count=$((retry_count+1))
    sleep 5
done

if [ $retry_count -eq $max_retries ]; then
    echo "Error: No se pudo crear el topic después de $max_retries intentos"
    exit 1
fi

echo "Topic creado exitosamente"

# Esperar adicional para asegurar que todo esté listo
sleep 5

# Iniciar aplicaciones
echo "Iniciando producer y consumer..."
python3 /app/producer/kafkaProducer.py &
python3 /app/consumer/kafkaConsumer.py &

# Mantener el contenedor corriendo
tail -f /dev/null
