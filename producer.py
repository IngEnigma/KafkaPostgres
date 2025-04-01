import json
import logging
from time import sleep
from typing import List, Dict, Any

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración de la aplicación
CONFIG = {
    "KAFKA_BROKER": 'localhost:9092',
    "TOPIC_NAME": 'crimes_data',
    "DATA_URL": 'https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl',
    "RETRY_ATTEMPTS": 3,
    "DELAY_BETWEEN_MESSAGES": 0.1,
    "REQUEST_TIMEOUT": 10,
}


class KafkaProducerError(Exception):
    """Excepción personalizada para errores del productor Kafka"""
    pass


def get_data_from_url() -> List[str]:
    """Obtiene datos del archivo JSONL remoto con manejo de errores"""
    try:
        response = requests.get(CONFIG["DATA_URL"], timeout=CONFIG["REQUEST_TIMEOUT"])
        response.raise_for_status()
        
        if not response.text:
            logger.warning("La respuesta está vacía")
            return []
            
        return response.text.splitlines()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al obtener datos: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        raise


def create_producer() -> KafkaProducer:
    """Crea y retorna un productor Kafka con reintentos"""
    for attempt in range(CONFIG["RETRY_ATTEMPTS"]):
        try:
            return KafkaProducer(
                bootstrap_servers=[CONFIG["KAFKA_BROKER"]],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                retries=3,
                request_timeout_ms=30000
            )
        except KafkaError as e:
            if attempt == CONFIG["RETRY_ATTEMPTS"] - 1:
                logger.error(f"Fallo al conectar con Kafka después de {CONFIG['RETRY_ATTEMPTS']} intentos")
                raise KafkaProducerError(f"Error de conexión Kafka: {str(e)}") from e
            logger.warning(f"Intento {attempt + 1} de conexión Kafka fallido. Reintentando...")
            sleep(2 ** attempt)
    raise KafkaProducerError("No se pudo crear el productor Kafka")


def send_data_to_kafka(producer: KafkaProducer, data_lines: List[str]) -> None:
    """Envía datos al tópico Kafka con manejo de errores"""
    for line_number, line in enumerate(data_lines, 1):
        try:
            if not line.strip():
                logger.warning(f"Línea {line_number} vacía. Saltando...")
                continue
                
            crime_data = json.loads(line)
            
            if 'dr_no' not in crime_data:
                logger.error(f"Línea {line_number}: Falta clave obligatoria 'dr_no'")
                continue
                
            future = producer.send(CONFIG["TOPIC_NAME"], value=crime_data)
            future.add_callback(
                lambda metadata: logger.info(
                    f"Enviado registro {crime_data['dr_no']} "
                    f"a partición {metadata.partition}"
                )
            )
            future.add_errback(
                lambda exc: logger.error(
                    f"Error al enviar registro {crime_data.get('dr_no', 'N/A')}: {str(exc)}"
                )
            )
            
            sleep(CONFIG["DELAY_BETWEEN_MESSAGES"])
            
        except json.JSONDecodeError as e:
            logger.error(f"Línea {line_number}: Error decodificando JSON - {str(e)}")
        except Exception as e:
            logger.error(f"Línea {line_number}: Error inesperado - {str(e)}")


def main() -> None:
    """Función principal de ejecución del programa"""
    producer = None
    try:
        logger.info("Iniciando productor Kafka...")
        producer = create_producer()
        
        logger.info("Obteniendo datos del archivo remoto...")
        data_lines = get_data_from_url()
        
        if not data_lines:
            logger.warning("No hay datos para enviar")
            return
            
        logger.info(f"Enviando {len(data_lines)} registros al tópico {CONFIG['TOPIC_NAME']}...")
        send_data_to_kafka(producer, data_lines)
        
    except KeyboardInterrupt:
        logger.info("Interrupción del usuario. Deteniendo...")
    except Exception as e:
        logger.error(f"Error crítico: {str(e)}", exc_info=True)
    finally:
        if producer:
            logger.info("Limpiando recursos...")
            producer.flush()
            producer.close()
        logger.info("Proceso completado")


if __name__ == "__main__":
    main()
