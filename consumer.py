from kafka import KafkaConsumer
import psycopg2
import json
from psycopg2.extras import execute_batch

def create_db_connection():
    try:
        return psycopg2.connect(
            host="ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech",
            database="crimes",
            user="crimes_owner",
            password="npg_QUkH7TfKZlF8",
            connect_timeout=10
        )
    except psycopg2.Error as e:
        print(f"Error de conexión a DB: {e}")
        return None

def create_consumer():
    return KafkaConsumer(
        'crimes',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        max_poll_records=100,  # Procesar de 100 en 100
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000
    )

def process_messages(consumer, conn):
    if not conn:
        return

    insert_query = """
    INSERT INTO crimes (dr_no, report_date, victim_age, victim_sex, crm_cd_desc)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (dr_no) DO NOTHING
    """
    
    batch_size = 50
    batch = []
    
    try:
        with conn.cursor() as cur:
            for message in consumer:
                crime = message.value
                batch.append((
                    crime.get('dr_no'),
                    crime.get('report_date'),
                    crime.get('victim_age'),
                    crime.get('victim_sex'),
                    crime.get('crm_cd_desc')
                ))
                
                if len(batch) >= batch_size:
                    execute_batch(cur, insert_query, batch)
                    conn.commit()
                    print(f"Insertado lote de {len(batch)} registros")
                    batch = []
            
            # Insertar los últimos registros
            if batch:
                execute_batch(cur, insert_query, batch)
                conn.commit()
                print(f"Insertado último lote de {len(batch)} registros")
                
    except Exception as e:
        print(f"Error en batch: {e}")
        conn.rollback()

def main():
    conn = create_db_connection()
    consumer = create_consumer()
    
    try:
        process_messages(consumer, conn)
    finally:
        if conn:
            conn.close()
        consumer.close()

if __name__ == "__main__":
    main()
