from kafka import KafkaProducer
import requests
import json
import os

def get_results():
    url = os.getenv("RESULTS_URL", "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/master/results/male_crimes/part-00000-*.json")
    response = requests.get(url)
    return [json.loads(line) for line in response.text.split('\n') if line.strip()]

def main():
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for data in get_results():
        producer.send('postgres-crimes', value=data)
        print(f"Produced: {data}")
    
    producer.flush()

if __name__ == "__main__":
    main()
