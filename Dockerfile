FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Instala todas las dependencias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre \
        python3 \
        python3-pip \
        wget \
        curl \
        netcat-openbsd \
        postgresql-client && \ 
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Instala librerías de Python
RUN pip3 install kafka-python psycopg2-binary requests

# Descarga y configura Kafka
RUN wget -q https://dlcdn.apache.org/kafka/3.7.2/kafka_2.12-3.7.2.tgz && \
    tar -xzf kafka_2.12-3.7.2.tgz && \
    rm kafka_2.12-3.7.2.tgz && \
    mv kafka_2.12-3.7.2 /opt/kafka

COPY zookeeper.properties /opt/kafka/config/
COPY server.properties /opt/kafka/config/
COPY *.sh /opt/kafka/
COPY *.py /opt/kafka/  # Añadiremos scripts Python después

WORKDIR /opt/kafka
RUN chmod +x *.sh

EXPOSE 2181 9092

CMD ["./run.sh"]  # Nuevo script maestro
