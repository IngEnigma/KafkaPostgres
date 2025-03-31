import logging
import os
import json
import requests
from kafka import KafkaProducer

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_results():
    url = os.getenv("RESULTS_URL", "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/part-00000-8b3d0568-ab40-4980-a6e8-e7e006621725-c000.json")
    logger.info(f"Fetching data from {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = [json.loads(line) for line in response.text.split('\n') if line.strip()]
        logger.info(f"Fetched {len(data)} records successfully.")
        return data
    except requests.RequestException as e:
        logger.error(f"Error fetching results: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        return []

def main():
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Connecting to Kafka at {kafka_servers}")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("Kafka Producer initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing Kafka producer: {e}")
        return
    
    results = get_results()
    if not results:
        logger.warning("No data to produce. Exiting.")
        return
    
    for data in results:
        try:
            producer.send('postgres-crimes', value=data)
            logger.info(f"Produced: {data}")
        except Exception as e:
            logger.error(f"Error producing message: {e}")
    
    producer.flush()
    logger.info("All messages sent and producer flushed.")

if __name__ == "__main__":
    main()
