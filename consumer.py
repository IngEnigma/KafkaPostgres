from kafka import KafkaConsumer
import psycopg2
import json

# Configuración de PostgreSQL
conn = psycopg2.connect(
    host="ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech",
    database="crimes",
    user="crimes_owner",
    password="npg_QUkH7TfKZlF8"
)

consumer = KafkaConsumer(
    'crimes',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    
cur = conn.cursor()

for message in consumer:
    crime = message.value
    try:
        cur.execute("""
            INSERT INTO crimes (dr_no, report_date, victim_age, victim_sex, crm_cd_desc)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (dr_no) DO NOTHING
        """, (
            crime['dr_no'],
            crime['report_date'],
            crime['victim_age'],
            crime['victim_sex'],
            crime['crm_cd_desc']
        ))
        conn.commit()
        print(f"Insertado: {crime['dr_no']}")
    except Exception as e:
        print(f"Error con {crime['dr_no']}: {e}")
        conn.rollback()
