import json
from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql
from typing import Dict, Any

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
    "enable_auto_commit": False
}

TOPIC_NAME = "crime"

def create_db_connection():
    """Crea y retorna una conexión a la base de datos."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False  # Usaremos transacciones explícitas
        return conn
    except psycopg2.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        raise

def create_kafka_consumer():
    """Crea y retorna un consumidor Kafka configurado."""
    return KafkaConsumer(
        TOPIC_NAME,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        **KAFKA_CONFIG
    )

def insert_crime_record(conn, record: Dict[str, Any]) -> None:
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
                record.get('victim_sex'),  # Nota: Hay un error tipográfico en el JSON original ("victim_sex")
                record.get('crm_cd_desc')
            ))
            conn.commit()
            print(f"Registro insertado/actualizado: DR_NO {record.get('dr_no')}")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error al insertar registro DR_NO {record.get('dr_no')}: {e}")

def process_messages(consumer, conn):
    """Procesa mensajes del consumidor Kafka."""
    try:
        for message in consumer:
            try:
                record = message.value
                print(f"Procesando mensaje: DR_NO {record.get('dr_no')}")
                
                # Validación básica del registro
                if not all(key in record for key in ['dr_no', 'report_date', 'victim_age', 'victim_sex', 'crm_cd_desc']):
                    print(f"Registro incompleto omitido: {record}")
                    continue
                
                insert_crime_record(conn, record)
                
                # Commit del offset solo después de insertar en DB
                consumer.commit()
                
            except json.JSONDecodeError as e:
                print(f"Error decodificando mensaje: {e}")
            except Exception as e:
                print(f"Error procesando mensaje: {e}")
                
    except KeyboardInterrupt:
        print("Deteniendo el consumidor...")
    finally:
        consumer.close()

def main():
    """Función principal."""
    print("Iniciando consumidor Kafka...")
    
    conn = None
    consumer = None
    
    try:
        # Crear conexión a la base de datos
        conn = create_db_connection()
        
        # Crear consumidor Kafka
        consumer = create_kafka_consumer()
        
        print(f"Escuchando en el tópico: {TOPIC_NAME}")
        process_messages(consumer, conn)
        
    except Exception as e:
        print(f"Error en el proceso principal: {e}")
    finally:
        # Cerrar conexiones siempre
        if conn is not None:
            conn.close()
            print("Conexión a la base de datos cerrada.")
        if consumer is not None:
            consumer.close()
            print("Consumidor Kafka cerrado.")

if __name__ == '__main__':
    main()
