import json
import logging
import signal
import sys
from datetime import datetime
from typing import Dict, Any

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from psycopg2 import sql
from psycopg2.extensions import connection
from psycopg2.pool import ThreadedConnectionPool

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger('CrimeConsumer')

# Configuración de la aplicación
CONFIG = {
    "KAFKA": {
        "BROKERS": ['localhost:9092'],
        "TOPIC": 'crimes_data',
        "GROUP_ID": 'crimes-group',
        "AUTO_OFFSET_RESET": 'earliest',
        "SESSION_TIMEOUT_MS": 10000,
        "HEARTBEAT_INTERVAL_MS": 3000,
    },
    "POSTGRES": {
        "host": 'ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech',
        "database": 'crimes',
        "user": 'crimes_owner',
        "password": 'npg_QUkH7TfKZlF8',
        "sslmode": 'require',
        "min_connections": 1,
        "max_connections": 3
    },
    "CONSUMER": {
        "POLL_TIMEOUT_MS": 1000,
        "MAX_RETRIES": 3,
        "RETRY_DELAY": 5
    }
}

class DatabaseError(Exception):
    """Excepción personalizada para errores de base de datos"""
    pass

class ConsumerShutdown(Exception):
    """Excepción para shutdown controlado"""
    pass

class ConnectionPool:
    """Pool de conexiones PostgreSQL con reconexión automática"""
    def __init__(self):
        self.pool = None
        self._create_pool()

    def _create_pool(self):
        try:
            self.pool = ThreadedConnectionPool(
                CONFIG["POSTGRES"]["min_connections"],
                CONFIG["POSTGRES"]["max_connections"],
                **CONFIG["POSTGRES"]
            )
            logger.info("Pool de conexiones creado")
        except psycopg2.Error as e:
            logger.error(f"Error creando pool: {str(e)}")
            raise DatabaseError from e

    def get_conn(self) -> connection:
        """Obtiene una conexión del pool"""
        for _ in range(CONFIG["CONSUMER"]["MAX_RETRIES"]):
            try:
                return self.pool.getconn()
            except psycopg2.OperationalError as e:
                logger.warning(f"Error obteniendo conexión: {str(e)}. Reintentando...")
                self._create_pool()
                sleep(CONFIG["CONSUMER"]["RETRY_DELAY"])
        
        raise DatabaseError("No se pudo obtener conexión después de reintentos")

    def return_conn(self, conn: connection):
        """Devuelve una conexión al pool"""
        self.pool.putconn(conn)

def create_consumer() -> KafkaConsumer:
    """Crea y retorna un consumidor Kafka con reintentos"""
    for attempt in range(CONFIG["CONSUMER"]["MAX_RETRIES"]):
        try:
            return KafkaConsumer(
                CONFIG["KAFKA"]["TOPIC"],
                bootstrap_servers=CONFIG["KAFKA"]["BROKERS"],
                group_id=CONFIG["KAFKA"]["GROUP_ID"],
                auto_offset_reset=CONFIG["KAFKA"]["AUTO_OFFSET_RESET"],
                enable_auto_commit=False,
                session_timeout_ms=CONFIG["KAFKA"]["SESSION_TIMEOUT_MS"],
                heartbeat_interval_ms=CONFIG["KAFKA"]["HEARTBEAT_INTERVAL_MS"],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=CONFIG["CONSUMER"]["POLL_TIMEOUT_MS"]
            )
        except KafkaError as e:
            if attempt == CONFIG["CONSUMER"]["MAX_RETRIES"] - 1:
                logger.error("Falló la creación del consumidor después de reintentos")
                raise
            logger.warning(f"Intento {attempt + 1} fallido. Reintentando...")
            sleep(CONFIG["CONSUMER"]["RETRY_DELAY"])

def setup_database(conn_pool: ConnectionPool):
    """Configura la base de datos y crea la tabla si no existe"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS crimes (
        dr_no BIGINT PRIMARY KEY,
        report_date TIMESTAMP,
        victim_age INTEGER,
        victim_sex VARCHAR(10),
        crm_cd_desc TEXT
    )
    """
    conn = None
    try:
        conn = conn_pool.get_conn()
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            logger.info("Tabla verificada/creada exitosamente")
    except psycopg2.Error as e:
        logger.error(f"Error creando tabla: {str(e)}")
        conn.rollback()
        raise DatabaseError from e
    finally:
        if conn:
            conn_pool.return_conn(conn)

def process_message(conn_pool: ConnectionPool, message: Dict[str, Any]):
    """Procesa un mensaje individual y lo inserta en la base de datos"""
    required_fields = ['dr_no', 'report_date', 'victim_age', 'victim_sex', 'crm_cd_desc']
    
    # Validación de datos
    if not all(field in message for field in required_fields):
        logger.error(f"Mensaje inválido. Campos faltantes: {message}")
        return

    try:
        # Limpieza de datos
        cleaned_data = {
            'dr_no': int(message['dr_no']),
            'report_date': datetime.strptime(
                message['report_date'].replace(' 12:00:00 AM', ''),
                '%m/%d/%Y'
            ),
            'victim_age': int(message['victim_age'] if message['victim_age'] else 0),
            'victim_sex': message['victim_sex'][:10],
            'crm_cd_desc': message['crm_cd_desc'][:255]
        }
    except (ValueError, TypeError) as e:
        logger.error(f"Error limpiando datos: {str(e)} - Datos: {message}")
        return

    # Inserción en base de datos
    insert_query = sql.SQL("""
    INSERT INTO crimes (dr_no, report_date, victim_age, victim_sex, crm_cd_desc)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (dr_no) DO NOTHING
    """)
    
    conn = None
    try:
        conn = conn_pool.get_conn()
        with conn.cursor() as cursor:
            cursor.execute(insert_query, (
                cleaned_data['dr_no'],
                cleaned_data['report_date'],
                cleaned_data['victim_age'],
                cleaned_data['victim_sex'],
                cleaned_data['crm_cd_desc']
            ))
            conn.commit()
            logger.debug(f"Insertado DR No: {cleaned_data['dr_no']}")
    except psycopg2.Error as e:
        logger.error(f"Error insertando datos: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn_pool.return_conn(conn)

def shutdown_handler(signum, frame):
    """Maneja señales de apagado"""
    logger.info("Iniciando apagado controlado...")
    raise ConsumerShutdown

def main():
    """Función principal de ejecución del consumidor"""
    # Configurar manejo de señales
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    consumer = None
    conn_pool = None

    try:
        # Inicializar componentes
        logger.info("Iniciando consumidor...")
        conn_pool = ConnectionPool()
        setup_database(conn_pool)
        consumer = create_consumer()

        logger.info("Escuchando mensajes...")
        while True:
            batch = consumer.poll(timeout_ms=1000)
            if not batch:
                continue

            for topic_partition, messages in batch.items():
                for message in messages:
                    try:
                        process_message(conn_pool, message.value)
                    except Exception as e:
                        logger.error(f"Error procesando mensaje: {str(e)}", exc_info=True)
            
            # Commit manual de offsets
            try:
                consumer.commit()
            except KafkaError as e:
                logger.error(f"Error haciendo commit de offsets: {str(e)}")

    except ConsumerShutdown:
        logger.info("Apagado solicitado")
    except Exception as e:
        logger.error(f"Error crítico: {str(e)}", exc_info=True)
    finally:
        # Limpieza de recursos
        if consumer:
            consumer.close()
        if conn_pool:
            conn_pool.pool.closeall()
        logger.info("Consumidor detenido exitosamente")

if __name__ == "__main__":
    main()
