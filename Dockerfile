FROM ubuntu:22.04

# Evita preguntas interactivas durante apt-get
ENV DEBIAN_FRONTEND=noninteractive

# Instala dependencias en un solo RUN para reducir capas y limpiar caché
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre \
        wget \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Descarga y descomprime Kafka (binarios pre-compilados)
RUN wget -q https://dlcdn.apache.org/kafka/3.7.2/kafka_2.12-3.7.2.tgz && \
    tar -xzf kafka_2.12-3.7.2.tgz && \
    rm kafka_2.12-3.7.2.tgz && \
    mv kafka_2.12-3.7.2 /opt/kafka

# Copia archivos de configuración y script
COPY zookeeper.properties /opt/kafka/config/
COPY server.properties /opt/kafka/config/
COPY run.sh /opt/kafka/

# Establece el directorio de trabajo
WORKDIR /opt/kafka

# Da permisos al script
RUN chmod +x run.sh

# Expone puertos (Zookeeper y Kafka)
EXPOSE 2181 9092

# Ejecuta el script al iniciar
CMD ["./run.sh"]
