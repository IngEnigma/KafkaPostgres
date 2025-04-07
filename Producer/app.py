from fastapi import FastAPI, HTTPException
import requests
from kafka import KafkaProducer, KafkaError
import os
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_TOPIC = "crime_data"
KAFKA_SERVER = "kafka:9092"  # Directo al contenedor Kafka

app = FastAPI()

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    logger.info("Kafka producer initialized.")
except KafkaError as e:
    logger.error(f"Error initializing Kafka producer: {e}")
    raise RuntimeError("Could not initialize Kafka producer.")

@app.post("/send-data")
def send_data():
    url = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Error downloading file: {e}")
        raise HTTPException(status_code=500, detail="Error downloading file.")

    lines = response.text.strip().splitlines()
    sent = 0
    errors = 0

    for idx, line in enumerate(lines, 1):
        try:
            data = json.loads(line)
            required_fields = ["dr_no", "report_date", "victim_age", "victim_sex", "crm_cd_desc"]
            if not all(field in data for field in required_fields):
                logger.warning(f"Skipping incomplete record at line {idx}: {data}")
                errors += 1
                continue

            producer.send(KAFKA_TOPIC, value=data)
            sent += 1

        except json.JSONDecodeError as e:
            logger.warning(f"Skipping invalid JSON at line {idx}: {e}")
            errors += 1
        except KafkaError as ke:
            logger.error(f"Kafka error at line {idx}: {ke}")
            errors += 1

    producer.flush()
    logger.info(f"Finished sending. Sent: {sent}, Errors: {errors}")
    return {"message": "Processing complete", "sent": sent, "errors": errors}
