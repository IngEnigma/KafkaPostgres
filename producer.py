import requests
from kafka import KafkaProducer
import json
import time

def create_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=16384,  # 16KB por lote
        linger_ms=100,     # Esperar hasta 100ms para agrupar
        compression_type='gzip',  # Comprimir mensajes
        max_request_size=1048576  # 1MB máximo por request
    )

def fetch_data(url):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        print(f"Error al obtener datos: {e}")
        return None

def send_to_kafka(producer, topic, response):
    batch_size = 100  # Mensajes por lote
    batch = []
    
    for line in response.iter_lines():
        if line:
            try:
                crime = json.loads(line)
                batch.append(crime)
                
                if len(batch) >= batch_size:
                    for record in batch:
                        producer.send(topic, record)
                    producer.flush()
                    print(f"Enviado lote de {len(batch)} registros")
                    batch = []
                    
            except json.JSONDecodeError as e:
                print(f"Error en línea: {e}")
    
    # Enviar los últimos registros restantes
    if batch:
        for record in batch:
            producer.send(topic, record)
        producer.flush()
        print(f"Enviado último lote de {len(batch)} registros")

def main():
    url = "https://raw.githubusercontent.com/.../data.jsonl"
    topic = 'crimes'
    
    producer = create_producer()
    response = fetch_data(url)
    
    if response:
        send_to_kafka(producer, topic, response)
    
    producer.close()

if __name__ == "__main__":
    main()
