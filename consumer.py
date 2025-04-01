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
        connect_timeout=10,
        sslmode="require",
        sslrootcert="/etc/ssl/certs/ca-certificates.crt",
        options="endpoint=ep-curly-recipe-a50hnh5z-pooler",  
        keepalives=1, 
        keepalives_idle=30, 
        keepalives_interval=10,
        keepalives_count=5
    )
except psycopg2.Error as e:
    print(f"Error de conexión a DB: {e}")
    return None

def create_consumer():
    return KafkaConsumer(
        'crimes',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        max_poll_records=200,  # Aumentado para mejor rendimiento
        auto_offset_reset='earliest',
        consumer_timeout_ms=30000
    )

def validate_crime_data(crime):
    """Valida y limpia los datos antes de insertar"""
    try:
        return (
            int(crime['dr_no']),
            str(crime.get('report_date', ''))[:100],
            int(crime['victim_age']) if crime.get('victim_age') else None,
            str(crime.get('victim_sex', ''))[:1],  # Solo almacenamos 'M'/'F'
            str(crime.get('crm_cd_desc', ''))[:100]
        )
    except (KeyError, ValueError) as e:
        print(f"Dato inválido: {crime}. Error: {str(e)}")
        return None

def process_messages(consumer, conn):
    if not conn:
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
    
    try:
        with conn.cursor() as cur:
            for message in consumer:
                print(f"🟠 Recibido mensaje: {message.value}")  # Ver mensajes recibidos
                
                crime_data = validate_crime_data(message.value)
                if crime_data:
                    batch.append(crime_data)
                
                if len(batch) >= batch_size:
                    execute_batch(cur, insert_query, batch)
                    conn.commit()
                    print(f"✅ Insertados {len(batch)} registros en DB")
                    batch = []
            
            if batch:
                execute_batch(cur, insert_query, batch)
                conn.commit()
                print(f"✅ Último lote: {len(batch)} registros insertados")
                
    except Exception as e:
        print(f"🔴 Error en lote: {str(e)}")
        conn.rollback()

def main():
    conn = create_db_connection()
    if conn:
        print("✅ Conexión exitosa a Neon.tech")
    else:
        print("❌ Falló la conexión")

    consumer = create_consumer()
    
    try:
        process_messages(consumer, conn)
    finally:
        if conn:
            conn.close()
        consumer.close()

if __name__ == "__main__":
    main()
