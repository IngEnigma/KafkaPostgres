FROM ubuntu

RUN apt-get update && \
    apt-get install -y \
    default-jre \
    tree \
    wget \
    curl \
    python3 \
    python3-pip \
    python3-venv \
    python3-full \
    postgresql-client && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://dlcdn.apache.org/kafka/3.7.2/kafka_2.12-3.7.2.tgz && \
    tar -xvf kafka_2.12-3.7.2.tgz && \
    mv kafka_2.12-3.7.2 /opt/kafka

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafkaProducer.py kafkaConsumer.py start.sh ./
RUN chmod +x start.sh

EXPOSE 2181 9092

CMD ["bash", "./start.sh"]
