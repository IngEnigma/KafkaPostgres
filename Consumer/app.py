from kafka import KafkaConsumer, KafkaError
import psycopg2
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_TOPIC = "crime_data"
KAFKA_SERVER = "kafka:9092"

# Credenciales PostgreSQL (hardcoded como pediste)
PGHOST = "ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech"
PGDATABASE = "crimes"
PGUSER = "crimes_owner"
PGPASSWORD = "npg_QUkH7TfKZlF8"

# Conexión a PostgreSQL
try:
    conn = psycopg2.connect(
        host=PGHOST,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD
    )
    cur = conn.cursor()
    logger.info("Connected to PostgreSQL.")
except Exception as e:
    logger.error(f"Database connection error: {e}")
    raise

# Kafka Consumer
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="crime_group"
    )
    logger.info(f"Kafka consumer initialized and subscribed to topic '{KAFKA_TOPIC}'.")
except KafkaError as e:
    logger.error(f"Kafka consumer initialization error: {e}")
    raise

# Procesamiento
logger.info("Waiting for messages...")

for message in consumer:
    data = message.value
    try:
        required = ["dr_no", "report_date", "victim_age", "victim_sex", "crm_cd_desc"]
        if not all(field in data for field in required):
            logger.warning(f"Incomplete data skipped: {data}")
            continue

        cur.execute("""
            INSERT INTO crimes (dr_no, report_date, victim_age, victim_sex, crm_cd_desc)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            data["dr_no"],
            data["report_date"],
            data["victim_age"],
            data["victim_sex"],
            data["crm_cd_desc"]
        ))
        conn.commit()
        logger.info(f"Inserted record: {data['dr_no']}")

    except psycopg2.Error as db_error:
        conn.rollback()
        logger.error(f"Database insert error: {db_error}")
    except Exception as ex:
        logger.error(f"Unexpected error: {ex}")
