#!/bin/bash

# Esperamos a que Kafka esté listo
echo "Esperando a Kafka para crear el tópico..."
sleep 10

# Creamos el tópico "crimes"
kafka-topics --bootstrap-server localhost:9092 \
             --create \
             --if-not-exists \
             --topic crimes \
             --partitions 1 \
             --replication-factor 1

echo "Tópico 'crimes' creado (si no existía)."
