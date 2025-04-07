from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import psycopg2
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_consumer():
    for i in range(10):  # 10 intentos
        try:
            return KafkaConsumer(
                'crimes_topic',
                bootstrap_servers='kafka:9092',
                auto_offset_reset='earliest',
                group_id='grupo_crimes',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                api_version=(2, 5, 0)
        except NoBrokersAvailable:
            if i == 9:
                logger.error("No se pudo conectar a Kafka después de 10 intentos")
                raise
            logger.warning(f"Intento {i+1}/10 - Kafka no disponible, reintentando...")
            time.sleep(5)

def insert_to_db(data):
    conn = psycopg2.connect(
        host="ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech",
        database="crimes",
        user="crimes_owner",
        password="npg_QUkH7TfKZlF8"
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO crimes (dr_no, report_date, victim_age, victim_sex, crm_cd_desc)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                data.get("dr_no"),
                data.get("report_date"),
                data.get("victim_age"),
                data.get("victim_sex"),
                data.get("crm_cd_desc")
            ))
            conn.commit()
    except Exception as e:
        logger.error(f"Error al insertar: {e}")
    finally:
        conn.close()

try:
    consumer = create_consumer()
    logger.info("Consumidor esperando mensajes...")

    for message in consumer:
        logger.info(f"Recibido: {message.value}")
        insert_to_db(message.value)
except Exception as e:
    logger.error(f"Error en el consumidor: {e}")
