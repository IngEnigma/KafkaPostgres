from kafka import KafkaConsumer
import psycopg2
import json
import sys
from psycopg2 import sql
from psycopg2.extras import execute_batch
from datetime import datetime
import logging
from typing import Dict, Any, List, Tuple, Optional

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NeonCrimeConsumer:
    def __init__(self, db_config: Dict[str, str]):
        """
        Inicializa el consumidor con configuración de base de datos
        
        Args:
            db_config: Diccionario con configuración de conexión a Neon.tech
        """
        self.db_config = db_config
        self.conn = None
        
    def __enter__(self):
        """Establece conexión a Neon.tech"""
        try:
            self.conn = psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                sslmode='require',
                connect_timeout=10
            )
            self.conn.autocommit = False
            logger.info("Conexión a Neon.tech establecida!")
            self._setup_database()
            return self
        except Exception as e:
            logger.error(f"Error conectando a Neon.tech: {e}", exc_info=True)
            sys.exit(1)
            
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                self.conn.close()
                logger.info("Conexión a Neon.tech cerrada")
            except Exception as e:
                logger.error(f"Error al cerrar conexión: {e}")
                
    def _setup_database(self):
        """Crea la tabla e índices si no existen"""
        try:
            with self.conn.cursor() as cur:
                # Crear tabla crimes
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS crimes (
                        DR_NO BIGINT PRIMARY KEY,
                        report_date TIMESTAMP,
                        victim_age INTEGER,
                        victim_sex VARCHAR(2),
                        crm_cd_desc TEXT,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                # Crear índices
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_crimes_date ON crimes(report_date);
                    CREATE INDEX IF NOT EXISTS idx_crimes_victim_age ON crimes(victim_age);
                    CREATE INDEX IF NOT EXISTS idx_crimes_victim_sex ON crimes(victim_sex);
                """)
                
                self.conn.commit()
                logger.info("Esquema de base de datos verificado")
        except Exception as e:
            logger.error(f"Error configurando base de datos: {e}")
            self.conn.rollback()
            raise
            
    def _parse_crime_date(self, date_str: str) -> Optional[datetime]:
        """Intenta parsear la fecha del crimen"""
        try:
            return datetime.strptime(date_str, '%m/%d/%Y %I:%M:%S %p')
        except ValueError:
            logger.warning(f"Formato de fecha inválido: {date_str}")
            return None
        except Exception as e:
            logger.warning(f"Error parseando fecha: {e}")
            return None
            
    def _validate_crime(self, crime: Dict[str, Any]) -> bool:
        """Valida que el crimen tenga todos los campos requeridos"""
        required_fields = ['DR_NO', 'report_date', 'victim_age', 'victim_sex', 'crm_cd_desc']
        if not all(field in crime for field in required_fields):
            logger.debug(f"Crimen omitido por campos faltantes: {crime.keys()}")
            return False
            
        try:
            # Validar tipos básicos
            int(crime['DR_NO'])
            int(crime['victim_age'])
            str(crime['victim_sex'])
            str(crime['crm_cd_desc'])
            return True
        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"Crimen omitido por error de validación: {e}")
            return False
            
    def process_messages(self, consumer: KafkaConsumer, batch_size: int = 100) -> None:
        """
        Procesa mensajes de Kafka y los inserta en la base de datos
        
        Args:
            consumer: Instancia de KafkaConsumer
            batch_size: Tamaño del lote para inserción
        """
        batch = []
        total_processed = 0
        total_errors = 0
        last_commit = datetime.now()
        
        try:
            for message in consumer:
                try:
                    crime = message.value
                    
                    if not self._validate_crime(crime):
                        total_errors += 1
                        continue
                        
                    # Parsear fecha
                    report_date = self._parse_crime_date(crime['report_date'])
                    
                    batch.append((
                        int(crime['DR_NO']),
                        report_date,
                        int(crime['victim_age']),
                        crime['victim_sex'],
                        crime['crm_cd_desc']
                    ))
                    
                    # Insertar por lotes para mejor performance
                    if len(batch) >= batch_size or (datetime.now() - last_commit).seconds >= 5:
                        self._insert_batch(batch)
                        total_processed += len(batch)
                        batch = []
                        last_commit = datetime.now()
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Error decodificando JSON: {e}")
                    total_errors += 1
                except Exception as e:
                    logger.error(f"Error procesando mensaje: {e}", exc_info=True)
                    total_errors += 1
                    
            # Insertar cualquier resto en el batch
            if batch:
                self._insert_batch(batch)
                total_processed += len(batch)
                
        except Exception as e:
            logger.error(f"Error en el proceso de mensajes: {e}", exc_info=True)
            raise
        finally:
            logger.info(
                f"Proceso completado. "
                f"Total procesados: {total_processed}, "
                f"Errores: {total_errors}"
            )
            
    def _insert_batch(self, batch: List[Tuple]) -> None:
        """
        Inserta un lote de registros en PostgreSQL
        
        Args:
            batch: Lista de tuplas con datos de crímenes
        """
        if not batch:
            return
            
        try:
            with self.conn.cursor() as cur:
                execute_batch(
                    cur,
                    """
                    INSERT INTO crimes (DR_NO, report_date, victim_age, victim_sex, crm_cd_desc)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (DR_NO) DO NOTHING
                    """,
                    batch,
                    page_size=len(batch)
                )
                self.conn.commit()
                logger.info(f"Insertado lote de {len(batch)} registros")
        except Exception as e:
            logger.error(f"Error insertando lote: {e}", exc_info=True)
            self.conn.rollback()
            raise

if __name__ == "__main__":
    # Configuración de Neon.tech
    DB_CONFIG = {
        'host': 'ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech',
        'database': 'crimes',
        'user': 'crimes_owner',
        'password': 'npg_QUkH7TfKZlF8'
    }
    
    # Configuración Kafka
    KAFKA_CONFIG = {
        'bootstrap_servers': ['localhost:9092'],
        'group_id': 'neon-crime-consumers',
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': False,
        'consumer_timeout_ms': 60000,
        'max_poll_records': 1000,
        'session_timeout_ms': 30000,
        'heartbeat_interval_ms': 10000
    }
    
    try:
        logger.info("Iniciando consumidor...")
        with NeonCrimeConsumer(DB_CONFIG) as consumer:
            kafka_consumer = KafkaConsumer(
                'crime_records',
                **KAFKA_CONFIG,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            
            logger.info("Iniciando consumo de mensajes...")
            consumer.process_messages(kafka_consumer, batch_size=50)
            
    except KeyboardInterrupt:
        logger.info("\nDeteniendo consumidor...")
    except Exception as e:
        logger.error(f"Error fatal: {e}", exc_info=True)
    finally:
        logger.info("Proceso terminado")
