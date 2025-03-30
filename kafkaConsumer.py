from kafka import KafkaConsumer
import psycopg2
import os
import json
import time

def create_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        database=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        sslmode="require"
    )

def setup_database():
    conn = create_connection()
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
    conn.close()

def consume_messages():
    consumer = KafkaConsumer(
        'postgres-crimes',
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        auto_offset_reset='earliest',
        value_deserializer=json.loads
    )
    
    conn = create_connection()
    
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
            print(f"Inserted: {data['DR_NO']}")
        except Exception as e:
            print(f"Error inserting {data['DR_NO']}: {str(e)}")
            conn.rollback()

if __name__ == "__main__":
    setup_database()
    while True:
        try:
            consume_messages()
        except Exception as e:
            print(f"Consumer error: {str(e)}, retrying in 10 seconds...")
            time.sleep(10)
