import json
from kafka import KafkaProducer
import requests
from typing import Dict, Any
import time
import sys
from datetime import datetime

def create_kafka_producer(bootstrap_servers: str = 'localhost:9092') -> KafkaProducer:
    """Crea y retorna un productor Kafka configurado."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        compression_type='gzip',  # Compresión para ahorrar ancho de banda
        linger_ms=5  # Pequeña espera para agrupar mensajes
    )

def on_send_success(record_metadata) -> None:
    """Callback para manejar envíos exitosos."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] Envío exitoso | "
          f"Tópico: {record_metadata.topic} | "
          f"Partición: {record_metadata.partition} | "
          f"Offset: {record_metadata.offset}")

def on_send_error(excp) -> None:
    """Callback para manejar errores de envío."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] ERROR en envío: {excp}", exc_info=True)

def fetch_jsonl_data(url: str) -> requests.Response:
    """Obtiene datos JSONL desde una URL."""
    try:
        start_time = time.time()
        response = requests.get(url, stream=True)
        response.raise_for_status()
        end_time = time.time()
        print(f"Datos descargados en {end_time - start_time:.2f} segundos")
        print(f"Tamaño aproximado: {len(response.content) / 1024:.2f} KB")
        return response
    except requests.RequestException as e:
        print(f"Error al obtener datos: {e}")
        raise

def process_and_send_data(producer: KafkaProducer, topic: str, jsonl_data: requests.Response) -> Dict[str, Any]:
    """Procesa y envía datos JSONL a Kafka."""
    metrics = {
        'total_records': 0,
        'valid_records': 0,
        'invalid_records': 0,
        'start_time': time.time(),
        'total_size_bytes': 0
    }
    
    for line in jsonl_data.iter_lines():
        if line:
            metrics['total_records'] += 1
            metrics['total_size_bytes'] += len(line)
            
            try:
                record = json.loads(line.decode('utf-8'))
                if isinstance(record, dict) and 'dr_no' in record:
                    producer.send(
                        topic, 
                        value=record
                    ).add_callback(on_send_success).add_errback(on_send_error)
                    metrics['valid_records'] += 1
                    print(f"Enviando registro DR_NO: {record['dr_no']} | "
                          f"Tamaño: {len(line)} bytes")
                else:
                    metrics['invalid_records'] += 1
                    print(f"Registro inválido omitido: Tipo={type(record)} | Contenido={str(record)[:100]}...")
            except json.JSONDecodeError as e:
                metrics['invalid_records'] += 1
                print(f"Error decodificando JSON: {e} | Línea: {str(line)[:100]}...")
    
    return metrics

def print_summary(metrics: Dict[str, Any]) -> None:
    """Imprime un resumen de las métricas."""
    duration = time.time() - metrics['start_time']
    print("\n" + "="*50)
    print("RESUMEN DE EJECUCIÓN")
    print("="*50)
    print(f"Total registros procesados: {metrics['total_records']}")
    print(f"Registros válidos enviados: {metrics['valid_records']}")
    print(f"Registros inválidos: {metrics['invalid_records']}")
    print(f"Tiempo total: {duration:.2f} segundos")
    print(f"Velocidad: {metrics['total_records']/max(duration, 0.1):.2f} registros/segundo")
    print(f"Tamaño total datos: {metrics['total_size_bytes']/1024:.2f} KB")
    print(f"Tamaño promedio por registro: {metrics['total_size_bytes']/max(metrics['total_records'], 1):.2f} bytes")
    print("="*50 + "\n")

def main() -> None:
    """Función principal."""
    # Configuración
    KAFKA_TOPIC = 'crime'
    DATA_URL = 'https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl'
    
    try:
        # Crear productor Kafka
        print("Inicializando productor Kafka...")
        producer = create_kafka_producer()
        
        # Obtener datos
        print(f"\nDescargando datos desde {DATA_URL}...")
        jsonl_data = fetch_jsonl_data(DATA_URL)
        
        # Procesar y enviar datos
        print("\nIniciando envío de datos a Kafka...")
        metrics = process_and_send_data(producer, KAFKA_TOPIC, jsonl_data)
        
        # Asegurar que todos los mensajes sean enviados
        producer.flush()
        print("\nFinalizando envío...")
        print_summary(metrics)
        
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
    finally:
        # Cerrar el productor siempre
        if 'producer' in locals():
            producer.close()
            print("Productor Kafka cerrado correctamente.")

if __name__ == '__main__':
    main()
