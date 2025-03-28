FROM ubuntu:20.04

RUN apt-get update && apt-get install -y default-jre wget curl

RUN wget https://dlcdn.apache.org/kafka/3.7.2/kafka_2.12-3.7.2.tgz -P /tmp
RUN tar -xvf /tmp/kafka_2.12-3.7.2.tgz -C /opt && rm /tmp/kafka_2.12-3.7.2.tgz

WORKDIR /opt/kafka_2.12-3.7.2

COPY run.sh ./
COPY zookeeper.properties ./config/
COPY server.properties ./config/

RUN chmod +x run.sh

EXPOSE 2181 9092

CMD ["bash", "./run.sh"]
