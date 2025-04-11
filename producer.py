from flask import Flask, jsonify, request
from confluent_kafka import Producer
import requests
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

PRODUCER_CONF = {
    'bootstrap.servers': 'cvq4abs3mareak309q80.any.us-west-2.mpx.prd.cloud.redpanda.com:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'IngEnigma',
    'sasl.password': 'BrARBOxX98VI4f2LIuIT1911NYGrXu',
}

producer = Producer(PRODUCER_CONF)

TOPIC = "crimes_pg"

JSONL_URL = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"

def delivery_report(err, msg):
    if err:
        logging.error(f"Error al enviar mensaje: {err}")
    else:
        logging.info(f"Mensaje enviado a {msg.topic()}: {msg.value().decode('utf-8')}")

@app.route('/send-crimes', methods=['POST'])
def send_crimes():
    try:
        logging.info(f"Descargando datos desde: {JSONL_URL}")
        response = requests.get(JSONL_URL)
        response.raise_for_status()

        lines = response.text.strip().splitlines()
        logging.info(f"Total de registros a enviar: {len(lines)}")

        for line in lines:
            logging.debug(f"Enviando: {line}")
            producer.produce(TOPIC, line.encode('utf-8'), callback=delivery_report)

        producer.flush()
        logging.info("Todos los datos fueron enviados correctamente.")

        return jsonify({"status": "success", "message": f"Todos los datos fueron enviados al tópico '{TOPIC}'"}), 200

    except Exception as e:
        logging.error(f"Error al enviar los datos: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health')
def health_check():
    return "ok", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
