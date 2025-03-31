import json
import requests
from kafka import KafkaProducer
from time import sleep

KAFKA_BROKER = 'localhost:9092'  
TOPIC_NAME = 'crimes_data'

DATA_URL = 'https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl'

def get_data_from_url():
    """Obtiene datos del archivo JSONL remoto"""
    response = requests.get(DATA_URL)
    response.raise_for_status()
    return response.text.splitlines()

def create_producer():
    """Crea y retorna un productor Kafka"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        acks='all',
        retries=3
    )

def send_data_to_kafka(producer, data_lines):
    """Envía datos al tópico Kafka"""
    for line in data_lines:
        try:
            crime_data = json.loads(line)
            producer.send(TOPIC_NAME, value=crime_data)
            print(f"Enviado: {crime_data['dr_no']}")
            sleep(0.1) 
        except json.JSONDecodeError as e:
            print(f"Error decodificando línea: {e}")
        except KeyError as e:
            print(f"Falta clave obligatoria en los datos: {e}")

def main():
    print("Iniciando productor Kafka...")
    producer = create_producer()
    
    print("Obteniendo datos del archivo remoto...")
    data_lines = get_data_from_url()
    
    print(f"Enviando {len(data_lines)} registros al tópico {TOPIC_NAME}...")
    send_data_to_kafka(producer, data_lines)
    
    producer.flush()
    print("Todos los datos han sido enviados.")

if __name__ == "__main__":
    main()
