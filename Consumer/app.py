from kafka import KafkaConsumer
import psycopg2
import os
import json

KAFKA_TOPIC = "crime_data"
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id="crime_group"
)

conn = psycopg2.connect(
    host=os.getenv("PGHOST"),
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD")
)
cur = conn.cursor()

print("Waiting for messages...")

for message in consumer:
    data = message.value
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
        print("Inserted:", data)
    except Exception as e:
        conn.rollback()
        print("Error inserting:", data, "Error:", e)
