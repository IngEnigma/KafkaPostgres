from kafka import KafkaConsumer
import psycopg2
import json
import logging
from psycopg2.extras import execute_batch
from sys import getsizeof
import time

# Configuración avanzada de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('kafka_consumer.log')
    ]
)
logger = logging.getLogger(__name__)

def create_db_connection():
    """Establece conexión con la base de datos con logging detallado"""
    try:
        logger.info("Intentando conectar a la base de datos Neon.tech...")
        start_time = time.time()
        
        conn = psycopg2.connect(
            host="ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech",
            database="crimes",
            user="crimes_owner",
            password="npg_QUkH7TfKZlF8",
            connect_timeout=10,
            sslmode="require",
            sslrootcert="/etc/ssl/certs/ca-certificates.crt",
            options="endpoint=ep-curly-recipe-a50hnh5z-pooler",  
            keepalives=1, 
            keepalives_idle=30, 
            keepalives_interval=10,
            keepalives_count=5
        )
        
        elapsed_time = time.time() - start_time
        logger.info(f"✅ Conexión exitosa a Neon.tech | Tiempo: {elapsed_time:.2f}s")
        return conn
        
    except psycopg2.Error as e:
        logger.error(f"❌ Error de conexión a DB: {e}", exc_info=True)
        return None

def create_consumer():
    """Crea un consumidor Kafka con logging de configuración"""
    logger.info("Configurando consumidor Kafka:")
    logger.info(" - Topic: crimes")
    logger.info(" - Bootstrap servers: localhost:9092")
    logger.info(" - max_poll_records: 200")
    logger.info(" - auto_offset_reset: earliest")
    
    return KafkaConsumer(
        'crimes',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        max_poll_records=200,
        auto_offset_reset='earliest',
        consumer_timeout_ms=30000,
        enable_auto_commit=False  # Para mayor control
    )

def validate_crime_data(crime):
    """Valida y limpia los datos con logging detallado"""
    try:
        validated = (
            int(crime['dr_no']),
            str(crime.get('report_date', ''))[:100],
            int(crime['victim_age']) if crime.get('victim_age') else None,
            str(crime.get('victim_sex', ''))[:1].upper(),
            str(crime.get('crm_cd_desc', ''))[:100]
        )
        logger.debug(f"Datos validados: {validated}")
        return validated
        
    except (KeyError, ValueError) as e:
        logger.warning(f"Dato inválido - Error: {str(e)} - Datos: {crime}")
        return None

def process_messages(consumer, conn):
    """Procesa mensajes con logging de rendimiento y errores"""
    if not conn:
        logger.error("No se puede procesar mensajes sin conexión a DB")
        return

    insert_query = """
    INSERT INTO crimes (dr_no, report_date, victim_age, victim_sex, crm_cd_desc)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (dr_no) DO UPDATE SET
        report_date = EXCLUDED.report_date,
        victim_age = EXCLUDED.victim_age,
        victim_sex = EXCLUDED.victim_sex,
        crm_cd_desc = EXCLUDED.crm_cd_desc
    """
    
    batch_size = 10
    batch = []
    total_processed = 0
    batch_counter = 0
    start_time = time.time()
    
    try:
        with conn.cursor() as cur:
            for message in consumer:
                message_size = getsizeof(message.value)
                logger.debug(f"📩 Mensaje recibido | Tamaño: {message_size} bytes | Offset: {message.offset} | Partición: {message.partition}")
                
                crime_data = validate_crime_data(message.value)
                if crime_data:
                    batch.append(crime_data)
                
                if len(batch) >= batch_size:
                    batch_counter += 1
                    batch_start = time.time()
                    
                    try:
                        execute_batch(cur, insert_query, batch)
                        conn.commit()
                        elapsed = time.time() - batch_start
                        total_processed += len(batch)
                        
                        logger.info(f"✅ Lote #{batch_counter} insertado | Registros: {len(batch)} | Tiempo: {elapsed:.3f}s | Total: {total_processed}")
                        batch = []
                        
                    except Exception as e:
                        logger.error(f"❌ Error en lote #{batch_counter}: {str(e)}", exc_info=True)
                        conn.rollback()
                        # Opcional: reintentar o manejar errores específicos
            
            # Insertar último lote si existe
            if batch:
                batch_counter += 1
                batch_start = time.time()
                execute_batch(cur, insert_query, batch)
                conn.commit()
                total_processed += len(batch)
                logger.info(f"✅ Último lote #{batch_counter} insertado | Registros: {len(batch)} | Tiempo: {time.time() - batch_start:.3f}s")
                
    except Exception as e:
        logger.critical(f"🚨 Error crítico en process_messages: {str(e)}", exc_info=True)
        conn.rollback()
    finally:
        total_time = time.time() - start_time
        logger.info(f"📊 Resumen final | Total registros procesados: {total_processed} | Tiempo total: {total_time:.2f}s")

def main():
    logger.info("==== INICIANDO CONSUMIDOR KAFKA ====")
    
    conn = create_db_connection()
    consumer = create_consumer()
    
    try:
        if conn:
            logger.info("Iniciando procesamiento de mensajes...")
            process_messages(consumer, conn)
        else:
            logger.error("No se pudo iniciar por falta de conexión a DB")
    except KeyboardInterrupt:
        logger.info("Detención solicitada por usuario")
    except Exception as e:
        logger.critical(f"Error inesperado: {str(e)}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Conexión a DB cerrada")
        consumer.close()
        logger.info("Consumidor Kafka cerrado")
        logger.info("==== APLICACIÓN FINALIZADA ====")

if __name__ == "__main__":
    main()
