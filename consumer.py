import json
from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql
from typing import Dict, Any
import time
from datetime import datetime
import sys

# Configuración de la base de datos
DB_CONFIG = {
    "host": "ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech",
    "database": "crimes",
    "user": "crimes_owner",
    "password": "npg_QUkH7TfKZlF8",
    "sslmode": "require"
}

# Configuración de Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "group_id": "crime-consumer-group",
    "auto_offset_reset": "earliest",
    "enable_auto_commit": False,
    "max_poll_records": 100,  # Procesar en lotes de 100
    "fetch_max_bytes": 52428800  # 50MB
}

TOPIC_NAME = "crime"

def create_db_connection():
    """Crea y retorna una conexión a la base de datos."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        print("Conexión a PostgreSQL establecida correctamente")
        return conn
    except psycopg2.Error as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        raise

def create_kafka_consumer():
    """Crea y retorna un consumidor Kafka configurado."""
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            **KAFKA_CONFIG
        )
        print(f"Consumidor Kafka configurado para el tópico: {TOPIC_NAME}")
        return consumer
    except Exception as e:
        print(f"Error al crear consumidor Kafka: {e}")
        raise

def insert_crime_record(conn, record: Dict[str, Any]) -> bool:
    """Inserta un registro de crimen en la base de datos."""
    query = sql.SQL("""
        INSERT INTO crimes (dr_no, report_date, victim_age, victim_sex, crm_cd_desc)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (dr_no) DO NOTHING
    """)
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (
                record.get('dr_no'),
                record.get('report_date'),
                record.get('victim_age'),
                record.get('victim_sex'),
                record.get('crm_cd_desc')
            ))
            conn.commit()
            return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error en base de datos: {e}")
        return False

def process_messages(consumer, conn):
    """Procesa mensajes del consumidor Kafka."""
    metrics = {
        'total_messages': 0,
        'successful_inserts': 0,
        'failed_inserts': 0,
        'invalid_records': 0,
        'start_time': time.time(),
        'last_commit_time': time.time()
    }
    
    try:
        print("\nIniciando consumo de mensajes...")
        for message in consumer:
            metrics['total_messages'] += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
            try:
                record = message.value
                print(f"\n[{timestamp}] Procesando mensaje | "
                      f"DR_NO: {record.get('dr_no')} | "
                      f"Tamaño: {len(str(record))} bytes | "
                      f"Partición: {message.partition} | "
                      f"Offset: {message.offset}")
                
                # Validación del registro
                if not all(key in record for key in ['dr_no', 'report_date', 'victim_age', 'victim_sex', 'crm_cd_desc']):
                    metrics['invalid_records'] += 1
                    print(f"[{timestamp}] Registro incompleto omitido")
                    continue
                
                # Insertar en PostgreSQL
                if insert_crime_record(conn, record):
                    metrics['successful_inserts'] += 1
                    print(f"[{timestamp}] Registro insertado correctamente")
                else:
                    metrics['failed_inserts'] += 1
                
                # Commit cada 100 mensajes o cada 5 segundos
                current_time = time.time()
                if (metrics['total_messages'] % 100 == 0 or 
                    current_time - metrics['last_commit_time'] > 5):
                    consumer.commit()
                    metrics['last_commit_time'] = current_time
                    print(f"[{timestamp}] Commit de offsets realizado")
                    
            except json.JSONDecodeError as e:
                print(f"[{timestamp}] ERROR decodificando mensaje: {e}")
            except Exception as e:
                print(f"[{timestamp}] ERROR procesando mensaje: {e}")
                
    except KeyboardInterrupt:
        print("\nDeteniendo el consumidor...")
    finally:
        print_summary(metrics)
        consumer.close()

def print_summary(metrics: Dict[str, Any]) -> None:
    """Imprime un resumen de las métricas."""
    duration = time.time() - metrics['start_time']
    print("\n" + "="*50)
    print("RESUMEN DE CONSUMO")
    print("="*50)
    print(f"Total mensajes procesados: {metrics['total_messages']}")
    print(f"Inserciones exitosas: {metrics['successful_inserts']}")
    print(f"Inserciones fallidas: {metrics['failed_inserts']}")
    print(f"Registros inválidos: {metrics['invalid_records']}")
    print(f"Tiempo total: {duration:.2f} segundos")
    print(f"Velocidad: {metrics['total_messages']/max(duration, 0.1):.2f} mensajes/segundo")
    print("="*50 + "\n")

def main():
    """Función principal."""
    print("Iniciando consumidor Kafka-PostgreSQL...")
    
    conn = None
    consumer = None
    
    try:
        # Crear conexión a la base de datos
        conn = create_db_connection()
        
        # Crear consumidor Kafka
        consumer = create_kafka_consumer()
        
        # Procesar mensajes
        process_messages(consumer, conn)
        
   
