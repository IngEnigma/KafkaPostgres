from flask import Flask, jsonify
from kafka import KafkaProducer
import json
import requests

app = Flask(__name__)
TOPIC = 'crimes_topic'

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/send-jsonl', methods=['POST'])
def send_jsonl():
    url = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"
    try:
        response = requests.get(url)
        response.raise_for_status()
        for line in response.text.strip().splitlines():
            data = json.loads(line)
            producer.send(TOPIC, value=data)
        producer.flush()
        return jsonify({"status": "ok", "msg": "Datos enviados a Kafka"}), 200
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
