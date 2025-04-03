FROM ubuntu:22.04

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
    rm kafka_2.12-3.7.2.tgz

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY run.sh .
COPY producer.py .
COPY consumer.py .

RUN chmod +x run.sh && \
    chmod +x kafka_2.12-3.7.2/bin/*.sh

EXPOSE 9092

CMD ["bash", "./run.sh"]
