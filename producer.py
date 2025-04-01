import requests
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    
url = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"
response = requests.get(url)

for line in response.text.splitlines():
    if line.strip():
        crime = json.loads(line)
        producer.send('crimes', crime)
        print(f"Enviado: {crime['dr_no']}")

producer.flush()
