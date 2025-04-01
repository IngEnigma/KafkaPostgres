from kafka import KafkaConsumer
import psycopg2
import json

def create_db_connection():
    """Crea y retorna una conexión a la base de datos PostgreSQL."""
    try:
        return psycopg2.connect(
            host="ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech",
            database="crimes",
            user="crimes_owner",
            password="npg_QUkH7TfKZlF8"
        )
    except psycopg2.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def create_consumer():
    """Crea y retorna un consumidor de Kafka."""
    return KafkaConsumer(
        'crimes',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def process_messages(consumer, conn):
    """Procesa los mensajes del tópico de Kafka e inserta en PostgreSQL."""
    if conn is None:
        print("No se pudo establecer conexión con la base de datos.")
        return
    
    cur = conn.cursor()
    
    for message in consumer:
        crime = message.value
        try:
            cur.execute(
                """
                INSERT INTO crimes (dr_no, report_date, victim_age, victim_sex, crm_cd_desc)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (dr_no) DO NOTHING
                """,
                (
                    crime.get('dr_no'),
                    crime.get('report_date'),
                    crime.get('victim_age'),
                    crime.get('victim_sex'),
                    crime.get('crm_cd_desc')
                )
            )
            conn.commit()
            print(f"Insertado: {crime.get('dr_no', 'ID no disponible')}")
        except Exception as e:
            print(f"Error con {crime.get('dr_no', 'ID no disponible')}: {e}")
            conn.rollback()

def main():
    conn = create_db_connection()
    consumer = create_consumer()
    
    try:
        process_messages(consumer, conn)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
