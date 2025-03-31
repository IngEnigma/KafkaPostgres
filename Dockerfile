FROM ubuntu

RUN apt-get update && \
    apt-get install -y \
    default-jre \
    wget \
    curl \
    python3 \
    python3-pip \
    postgresql-client && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://dlcdn.apache.org/kafka/3.7.2/kafka_2.12-3.7.2.tgz && \
    tar -xvf kafka_2.12-3.7.2.tgz && \
    mv kafka_2.12-3.7.2 /opt/kafka

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY producer.py consumer.py run.sh ./
COPY zookeeper.properties /opt/kafka/config/
COPY server.properties /opt/kafka/config/

RUN chmod +x run.sh

EXPOSE 2181 9092

CMD ["bash", "./run.sh"]
