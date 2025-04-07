from flask import Flask, jsonify
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import requests
import time
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_producer():
    for i in range(10):  # 10 intentos
        try:
            return KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(2, 5, 0)
            )
        except NoBrokersAvailable:
            if i == 9:
                logger.error("No se pudo conectar a Kafka después de 10 intentos")
                raise
            logger.warning(f"Intento {i+1}/10 - Kafka no disponible, reintentando...")
            time.sleep(5)

producer = create_producer()

@app.route('/send-jsonl', methods=['POST'])
def send_jsonl():
    url = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"
    try:
        response = requests.get(url)
        response.raise_for_status()
        for line in response.text.strip().splitlines():
            data = json.loads(line)
            producer.send('crimes_topic', value=data)
        producer.flush()
        return jsonify({"status": "ok", "msg": "Datos enviados a Kafka"}), 200
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
