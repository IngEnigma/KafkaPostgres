import logging
import os
import json
import time
from kafka import KafkaConsumer
import psycopg2

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("PGHOST"),
            database=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
            sslmode="require"
        )
        logger.info("Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def setup_database():
    conn = create_connection()
    if conn is None:
        logger.error("Skipping database setup due to connection failure.")
        return
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crimes (
                    DR_NO INT PRIMARY KEY,
                    report_date TIMESTAMP,
                    victim_age INT,
                    victim_sex VARCHAR(1),
                    crm_cd_desc TEXT
                )
            """)
            conn.commit()
            logger.info("Database setup complete.")
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
    finally:
        conn.close()

def consume_messages():
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Connecting to Kafka at {kafka_servers}")
    
    try:
        consumer = KafkaConsumer(
            'postgres-crimes',
            bootstrap_servers=kafka_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logger.info("Kafka Consumer initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing Kafka consumer: {e}")
        return
    
    conn = create_connection()
    if conn is None:
        logger.error("Skipping message consumption due to database connection failure.")
        return
    
    for message in consumer:
        data = message.value
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO crimes (DR_NO, report_date, victim_age, victim_sex, crm_cd_desc)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (DR_NO) DO NOTHING
                """, (
                    data['DR_NO'],
                    data['report_date'],
                    data['victim_age'],
                    data['victim_sex'],
                    data['crm_cd_desc']
                ))
                conn.commit()
            logger.info(f"Inserted: {data['DR_NO']}")
        except Exception as e:
            logger.error(f"Error inserting {data['DR_NO']}: {e}")
            conn.rollback()

def main():
    setup_database()
    while True:
        try:
            consume_messages()
        except Exception as e:
            logger.error(f"Consumer error: {e}, retrying in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    main()
