from flask import Flask, jsonify
from confluent_kafka import Producer
import requests

app = Flask(__name__)

# Configuración del productor para Redpanda Cloud
producer_conf = {
    'bootstrap.servers': 'cvq4abs3mareak309q80.any.us-west-2.mpx.prd.cloud.redpanda.com:9092',
    'security.protocol': 'SASL_SSL',           
    'sasl.mechanism': 'SCRAM-SHA-256',         
    'sasl.username': 'IngEnigma',            
    'sasl.password': 'BrARBOxX98VI4f2LIuIT1911NYGrXu',          
}

producer = Producer(producer_conf)

TOPIC = "crimes"
JSONL_URL = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"

def delivery_report(err, msg):
    if err:
        print(f'Error al enviar: {err}')
    else:
        print(f'Enviado: {msg.value().decode("utf-8")} a {msg.topic()}')

@app.route('/send-crimes', methods=['POST'])
def send_crimes():
    try:
        response = requests.get(JSONL_URL)
        response.raise_for_status()

        for line in response.text.strip().splitlines():
            producer.produce(TOPIC, line.encode('utf-8'), callback=delivery_report)

        producer.flush()
        return jsonify({"status": "success", "message": "Todos los datos fueron enviados al tópico 'crimes'"}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
