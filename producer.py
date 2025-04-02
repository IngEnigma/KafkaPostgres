import json
from kafka import KafkaProducer
import requests
from typing import Dict, Any

def create_kafka_producer(bootstrap_servers: str = 'localhost:9092') -> KafkaProducer:
    """Crea y retorna un productor Kafka configurado."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  # Espera confirmación de todos los replicas
        retries=3    # Reintentos en caso de fallo
    )

def on_send_success(record_metadata) -> None:
    """Callback para manejar envíos exitosos."""
    print(f"Mensaje enviado con éxito a: "
          f"tópico={record_metadata.topic}, "
          f"partición={record_metadata.partition}, "
          f"offset={record_metadata.offset}")

def on_send_error(excp) -> None:
    """Callback para manejar errores de envío."""
    print(f'Error al enviar mensaje: {excp}', exc_info=True)

def fetch_jsonl_data(url: str) -> requests.Response:
    """Obtiene datos JSONL desde una URL."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Lanza excepción para códigos 4XX/5XX
        return response
    except requests.RequestException as e:
        print(f"Error al obtener datos: {e}")
        raise

def process_and_send_data(producer: KafkaProducer, topic: str, jsonl_data: requests.Response) -> None:
    """Procesa y envía datos JSONL a Kafka."""
    for line in jsonl_data.iter_lines():
        if line:  # Ignora líneas vacías
            try:
                record = json.loads(line.decode('utf-8'))
                # Validación básica del registro
                if isinstance(record, dict) and 'dr_no' in record:
                    producer.send(
                        topic, 
                        value=record
                    ).add_callback(on_send_success).add_errback(on_send_error)
                    print(f"Enviado registro DR_NO: {record['dr_no']}")
                else:
                    print(f"Registro inválido omitido: {record}")
            except json.JSONDecodeError as e:
                print(f"Error decodificando JSON: {e} - Línea: {line}")

def main() -> None:
    """Función principal."""
    # Configuración
    KAFKA_TOPIC = 'crime'
    DATA_URL = 'https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl'
    
    try:
        # Crear productor Kafka
        producer = create_kafka_producer()
        
        # Obtener datos
        print(f"Obteniendo datos desde {DATA_URL}...")
        jsonl_data = fetch_jsonl_data(DATA_URL)
        
        # Procesar y enviar datos
        print("Enviando datos a Kafka...")
        process_and_send_data(producer, KAFKA_TOPIC, jsonl_data)
        
        # Asegurar que todos los mensajes sean enviados
        producer.flush()
        print("Todos los mensajes han sido enviados.")
        
    except Exception as e:
        print(f"Error en el proceso principal: {e}")
    finally:
        # Cerrar el productor siempre
        if 'producer' in locals():
            producer.close()
            print("Productor Kafka cerrado.")

if __name__ == '__main__':
    main()
