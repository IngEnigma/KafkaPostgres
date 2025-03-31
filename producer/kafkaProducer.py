from kafka import KafkaProducer
import json
import requests
from datetime import datetime
import logging
from typing import Dict, Any, Iterator

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CrimeDataProducer:
    def __init__(self, bootstrap_servers: list):
        """
        Inicializa el productor Kafka con configuraciones robustas
        
        Args:
            bootstrap_servers: Lista de servidores Kafka (ej: ['localhost:9092'])
        """
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Esperar confirmación de todos los replicas
            compression_type='gzip',  # Compresión para reducir tamaño
            retries=5,  # Número de reintentos
            request_timeout_ms=30000,  # 30 segundos de timeout
            max_request_size=1048576,  # 1MB (ajustar según necesidades)
            buffer_memory=33554432,  # 32MB buffer
            linger_ms=100,  # Esperar hasta 100ms para agrupar mensajes
            batch_size=16384  # 16KB por lote
        )
        logger.info("Productor Kafka inicializado correctamente")
        
    def fetch_crime_data(self, url: str) -> Iterator[Dict[str, Any]]:
        """
        Obtiene datos de crímenes desde la URL como un generador
        
        Args:
            url: URL del endpoint que devuelve los datos en JSONL
            
        Yields:
            Dict con los datos de cada crimen
            
        Raises:
            Exception: Si hay error en la conexión o formato de datos
        """
        try:
            logger.info(f"Conectando a {url} para obtener datos...")
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            for line_number, line in enumerate(response.iter_lines(), 1):
                if line:
                    try:
                        crime = json.loads(line)
                        yield crime
                    except json.JSONDecodeError as e:
                        logger.warning(f"Línea {line_number} no es JSON válido: {e}")
                    except Exception as e:
                        logger.warning(f"Error procesando línea {line_number}: {e}")
                        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de conexión al obtener datos: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al obtener datos: {e}")
            raise
            
    def _validate_crime(self, crime: Dict[str, Any]) -> bool:
        """
        Valida que el crimen tenga todos los campos requeridos y en formato correcto
        
        Args:
            crime: Diccionario con datos del crimen
            
        Returns:
            bool: True si el crimen es válido, False si no
        """
        required_fields = {
            'DR_NO': int,
            'report_date': str,
            'victim_age': int,
            'victim_sex': str,
            'crm_cd_desc': str
        }
        
        # Verificar campos requeridos
        if not all(field in crime for field in required_fields):
            logger.debug(f"Crimen omitido por campos faltantes: {crime.keys()}")
            return False
            
        # Verificar tipos de datos
        try:
            crime['DR_NO'] = int(crime['DR_NO'])
            crime['victim_age'] = int(crime['victim_age'])
            
            # Intentar parsear fecha
            datetime.strptime(crime['report_date'], '%m/%d/%Y %I:%M:%S %p')
            
            return True
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"Crimen omitido por error de validación: {e}")
            return False
            
    def produce_to_kafka(self, topic: str, data_url: str, batch_size: int = 100) -> None:
        """
        Procesa y envía datos de crímenes a Kafka
        
        Args:
            topic: Nombre del topic Kafka
            data_url: URL de origen de los datos
            batch_size: Número de mensajes por lote
        """
        logger.info(f"Iniciando producción al topic '{topic}'")
        start_time = datetime.now()
        total_sent = 0
        total_errors = 0
        
        try:
            for crime in self.fetch_crime_data(data_url):
                if not self._validate_crime(crime):
                    total_errors += 1
                    continue
                    
                try:
                    self.producer.send(topic, value=crime)
                    total_sent += 1
                    
                    if total_sent % batch_size == 0:
                        self.producer.flush()
                        logger.info(f"Enviados {total_sent} mensajes...")
                        
                except Exception as e:
                    logger.error(f"Error enviando crimen {crime.get('DR_NO')}: {e}")
                    total_errors += 1
                    
            # Flush final para asegurar envío
            self.producer.flush()
            
        except Exception as e:
            logger.error(f"Error en el proceso de producción: {e}")
            raise
        finally:
            elapsed = datetime.now() - start_time
            logger.info(
                f"Producción completada. "
                f"Total enviados: {total_sent}, "
                f"Errores: {total_errors}, "
                f"Duración: {elapsed}"
            )
        
    def close(self) -> None:
        """Cierra el productor Kafka correctamente"""
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("Productor cerrado correctamente")
        except Exception as e:
            logger.error(f"Error al cerrar el productor: {e}")

if __name__ == "__main__":
    # Configuración
    KAFKA_SERVERS = ['localhost:9092']
    TOPIC = 'crime_records'
    DATA_URL = 'https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl'
    
    # Iniciar productor
    producer = CrimeDataProducer(KAFKA_SERVERS)
    
    try:
        logger.info("Iniciando producción de datos de crímenes...")
        producer.produce_to_kafka(TOPIC, DATA_URL, batch_size=100)
    except KeyboardInterrupt:
        logger.info("\nDeteniendo productor...")
    except Exception as e:
        logger.error(f"Error fatal: {e}", exc_info=True)
    finally:
        producer.close()
