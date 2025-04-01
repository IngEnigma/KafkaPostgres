import requests
from kafka import KafkaProducer
import json

def create_producer():
    """Crea y retorna un productor de Kafka con configuración predeterminada."""
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def fetch_data(url):
    """Obtiene datos desde una URL y los devuelve como una lista de líneas."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text.splitlines()
    except requests.RequestException as e:
        print(f"Error al obtener los datos: {e}")
        return []

def send_to_kafka(producer, topic, data_lines):
    """Envía los datos procesados a Kafka."""
    for line in data_lines:
        if line.strip():
            try:
                crime = json.loads(line)
                producer.send(topic, crime)
                print(f"Enviado: {crime.get('dr_no', 'ID no disponible')}")
            except json.JSONDecodeError as e:
                print(f"Error al procesar línea: {e}")
    producer.flush()

def main():
    url = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"
    topic = 'crimes'
    
    producer = create_producer()
    data_lines = fetch_data(url)
    send_to_kafka(producer, topic, data_lines)
    
    producer.close()

if __name__ == "__main__":
    main()
