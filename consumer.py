import json
import psycopg2
from kafka import KafkaConsumer
from psycopg2 import sql

KAFKA_BROKER = 'localhost:9092'  
TOPIC_NAME = 'crimes_data'

DB_CONFIG = {
    'host': 'ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech',
    'database': 'crimes',
    'user': 'crimes_owner',
    'password': 'npg_QUkH7TfKZlF8',
    'sslmode': 'require'
}

def create_consumer():
    """Crea y retorna un consumidor Kafka"""
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='crimes-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def create_table_if_not_exists(conn):
    """Crea la tabla si no existe"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS crimes (
        dr_no BIGINT PRIMARY KEY,
        report_date TIMESTAMP,
        victim_age INTEGER,
        victim_sex VARCHAR(10),
        crm_cd_desc TEXT
    )
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
    conn.commit()

def insert_data(conn, data):
    """Inserta datos en la base de datos"""
    insert_query = sql.SQL("""
    INSERT INTO crimes (dr_no, report_date, victim_age, victim_sex, crm_cd_desc)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (dr_no) DO NOTHING
    """)
    
    report_date = data['report_date'].replace(' 12:00:00 AM', '')
    
    with conn.cursor() as cursor:
        cursor.execute(insert_query, (
            data['dr_no'],
            report_date,
            data['victim_age'],
            data['victim_sex'],
            data['crm_cd_desc']
        ))
    conn.commit()

def main():
    print("Conectando a PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    create_table_if_not_exists(conn)
    
    print("Iniciando consumidor Kafka...")
    consumer = create_consumer()
    
    print(f"Escuchando mensajes del tópico {TOPIC_NAME}...")
    try:
        for message in consumer:
            crime_data = message.value
            print(f"Recibido: {crime_data['dr_no']}")
            insert_data(conn, crime_data)
    except KeyboardInterrupt:
        print("Deteniendo consumidor...")
    finally:
        consumer.close()
        conn.close()
        print("Conexiones cerradas.")

if __name__ == "__main__":
    main()
