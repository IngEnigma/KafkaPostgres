from kafka import KafkaConsumer
import json
import psycopg2

TOPIC = 'crimes_topic'
DB_CONFIG = {
    "host": "ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech",
    "database": "crimes",
    "user": "crimes_owner",
    "password": "npg_QUkH7TfKZlF8"
}

def insert_to_db(data):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
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
        print("Error al insertar:", e)
    finally:
        cur.close()
        conn.close()

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    group_id='grupo_crimes',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumidor esperando mensajes...")

for message in consumer:
    print("Recibido:", message.value)
    insert_to_db(message.value)
