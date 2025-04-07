from fastapi import FastAPI, HTTPException
import requests
from kafka import KafkaProducer
import os
import json

KAFKA_TOPIC = "crime_data"
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.post("/send-data")
def send_data():
    url = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"
    try:
        response = requests.get(url)
        response.raise_for_status()

        lines = response.text.strip().splitlines()
        for line in lines:
            try:
                data = json.loads(line)
                producer.send(KAFKA_TOPIC, data)
            except json.JSONDecodeError as e:
                print(f"Skipping line due to JSON error: {e}")
        
        producer.flush()
        return {"message": "Data sent to Kafka successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
