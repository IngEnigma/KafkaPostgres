import psycopg2
from kafka import KafkaConsumer
import json

print('Connecting to PostgreSQL ...')

PGHOST = 'ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech'
PGDATABASE = 'crimes'
PGUSER = 'crimes_owner'
PGPASSWORD = 'npg_QUkH7TfKZlF8'

try:
    conn = psycopg2.connect(
        host=PGHOST,
        database=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD
    )
    cur = conn.cursor()
    print("PostgreSQL connected successfully!")
except Exception as e:
    print("Could not connect to PostgreSQL:", e)

consumer = KafkaConsumer('crime', bootstrap_servers=['localhost:9092'])

for msg in consumer:
    record = json.loads(msg.value)

    dr_no = record.get('DR_NO')
    report_date = record.get('report_date')
    victim_age = record.get('victim_age')
    victim_sex = record.get('victim_sex')
    crm_cd_desc = record.get('crm_cd_desc')

    print(f"DR_NO: {dr_no}, Report Date: {report_date}, Victim Age: {victim_age}, Victim Sex: {victim_sex}, Crime Description: {crm_cd_desc}")

    try:
        sql = """
        INSERT INTO crimes (dr_no, report_date, victim_age, victim_sex, crm_cd_desc) 
        VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(sql, (dr_no, report_date, victim_age, victim_sex, crm_cd_desc))
        conn.commit()
    except Exception as e:
        print("Error inserting into PostgreSQL:", e)

conn.close()
