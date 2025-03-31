FROM eclipse-temurin:17-jre-jammy

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    curl \
    python3 \
    python3-pip \
    netcat \
    && rm -rf /var/lib/apt/lists/*

ENV KAFKA_VERSION=3.7.2
ENV SCALA_VERSION=2.12
RUN wget -q https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    mv kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka

RUN curl -o /opt/kafka/wait-for-it.sh https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh && \
    chmod +x /opt/kafka/wait-for-it.sh

COPY zookeeper.properties /opt/kafka/config/
COPY server.properties /opt/kafka/config/
COPY run.sh /opt/kafka/

WORKDIR /app

COPY producer /app/producer
RUN pip install -r /app/producer/requirements.txt

COPY consumer /app/consumer
RUN pip install -r /app/consumer/requirements.txt

EXPOSE 9092 9093 2181

ENV KAFKA_HEAP_OPTS="-Xmx256m -Xms128m"
ENV KAFKA_JVM_PERFORMANCE_OPTS="-XX:MetaspaceSize=96m -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35"

CMD ["/bin/bash", "/opt/kafka/run.sh"]
