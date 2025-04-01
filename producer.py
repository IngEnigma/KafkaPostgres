import requests
from kafka import KafkaProducer
import json
import time
import logging
from sys import getsizeof

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('kafka_producer.log')
    ]
)
logger = logging.getLogger(__name__)

def create_producer():
    """Crea un productor Kafka optimizado para mensajes JSON medianos"""
    logger.info("Creando productor Kafka con configuración:")

    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_request_size=1048576,
            batch_size=32768,
            linger_ms=500,
            compression_type='gzip',
            retries=3,
            request_timeout_ms=30000
        )
        
        # Verificación activa de conexión
        if producer.bootstrap_connected():
            logger.info("✅ Productor Kafka creado exitosamente")
            return producer
        else:
            logger.error("❌ El productor no pudo conectarse al broker")
            raise KafkaError("No se pudo conectar al broker")

    except NoBrokersAvailable as e:
        logger.error("🚨 No hay brokers disponibles: %s", e)
        raise
    except Exception as e:
        logger.error("🚨 Error crítico creando productor: %s", e)
        raise

def fetch_data(url):
    try:
        logger.info(f"Intentando conectar a la URL: {url}")
        start_time = time.time()
        response = requests.get(url, stream=True)
        response.raise_for_status()
        elapsed_time = time.time() - start_time
        logger.info(f"Conexión exitosa. Tiempo de respuesta: {elapsed_time:.2f} segundos")
        return response
    except requests.RequestException as e:
        logger.error(f"Error al obtener datos: {e}", exc_info=True)
        return None

def send_to_kafka(producer, topic, response):
    batch_size = 10  # Mensajes por lote
    batch = []
    total_messages = 0
    total_bytes = 0
    
    logger.info(f"Iniciando envío de datos al topic: {topic}")
    
    for line in response.iter_lines():
        if line:
            try:
                crime = json.loads(line)
                message_size = getsizeof(json.dumps(crime).encode('utf-8'))
                logger.debug(f"Mensaje procesado - Tamaño: {message_size} bytes - Contenido: {str(crime)[:100]}...")
                
                batch.append(crime)
                total_bytes += message_size

                if len(batch) >= batch_size:
                    batch_start_time = time.time()
                    for record in batch:
                        producer.send(topic, record)
                    producer.flush()
                    batch_time = time.time() - batch_start_time
                    
                    logger.info(f"✅ Lote enviado - Mensajes: {len(batch)} - Tamaño total: {total_bytes} bytes - Tiempo: {batch_time:.2f} segundos")
                    total_messages += len(batch)
                    batch = []
                    total_bytes = 0
                    
            except json.JSONDecodeError as e:
                logger.error(f"🔴 Error decodificando línea: {e} - Línea problemática: {line[:200]}", exc_info=True)
    
    # Enviar los últimos mensajes si los hay
    if batch:
        batch_start_time = time.time()
        for record in batch:
            producer.send(topic, record)
        producer.flush()
        batch_time = time.time() - batch_start_time
        logger.info(f"✅ Último lote enviado - Mensajes: {len(batch)} - Tamaño total: {total_bytes} bytes - Tiempo: {batch_time:.2f} segundos")
        total_messages += len(batch)
    
    logger.info(f"Proceso completado. Total mensajes enviados: {total_messages}")

def main():
    url = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"
    topic = 'crimes'
    
    logger.info("==== INICIANDO APLICACIÓN ====")
    logger.info(f"Configuración: URL={url}, Topic={topic}")
    
    try:
        producer = create_producer()
        response = fetch_data(url)
        
        if response:
            send_to_kafka(producer, topic, response)
        else:
            logger.error("No se pudo obtener respuesta del servidor")
    except Exception as e:
        logger.error(f"Error crítico en la aplicación: {e}", exc_info=True)
    finally:
        producer.close()
        logger.info("Productor Kafka cerrado correctamente")
        logger.info("==== APLICACIÓN FINALIZADA ====")

if __name__ == "__main__":
    main()
