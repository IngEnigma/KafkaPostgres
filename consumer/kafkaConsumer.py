from kafka import KafkaConsumer
import psycopg2
import json
import sys
from psycopg2 import sql
from psycopg2.extras import execute_batch
from datetime import datetime

class NeonCrimeConsumer:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        
    def __enter__(self):
        """Establece conexión a Neon.tech"""
        try:
            self.conn = psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                sslmode='require'
            )
            self.conn.autocommit = False
            print("Conexión a Neon.tech establecida!")
            self._setup_database()
            return self
        except Exception as e:
            print(f"Error conectando a Neon.tech: {e}")
            sys.exit(1)
            
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
            print("Conexión a Neon.tech cerrada")
            
    def _setup_database(self):
        """Crea la tabla si no existe"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS crimes (
                        DR_NO BIGINT PRIMARY KEY,
                        report_date TIMESTAMP,
                        victim_age INTEGER,
                        victim_sex VARCHAR(2),
                        crm_cd_desc TEXT,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_crimes_date ON crimes(report_date);
                    CREATE INDEX IF NOT EXISTS idx_crimes_victim_age ON crimes(victim_age);
                """)
                self.conn.commit()
                print("Esquema de base de datos verificado")
        except Exception as e:
            print(f"Error configurando base de datos: {e}")
            self.conn.rollback()
            raise
            
    def process_messages(self, consumer, batch_size=50):
        """Procesa mensajes de Kafka en lotes"""
        batch = []
        last_commit = datetime.now()
        
        for message in consumer:
            try:
                crime = message.value
                
                # Validar campos requeridos
                if not all(k in crime for k in ['DR_NO', 'report_date', 'victim_age', 'victim_sex', 'crm_cd_desc']):
                    print(f"Mensaje inválido omitido: {crime}")
                    continue
                    
                # Convertir fecha
                try:
                    report_date = datetime.strptime(crime['report_date'], '%m/%d/%Y %I:%M:%S %p')
                except ValueError:
                    report_date = None
                    
                batch.append((
                    crime['DR_NO'],
                    report_date,
                    crime['victim_age'],
                    crime['victim_sex'],
                    crime['crm_cd_desc']
                ))
                
                # Insertar por lotes para mejor performance
                if len(batch) >= batch_size or (datetime.now() - last_commit).seconds >= 5:
                    self._insert_batch(batch)
                    batch = []
                    last_commit = datetime.now()
                    
            except json.JSONDecodeError as e:
                print(f"Error decodificando JSON: {e}")
            except Exception as e:
                print(f"Error procesando mensaje: {e}")
                
        # Insertar cualquier resto en el batch
        if batch:
            self._insert_batch(batch)
            
    def _insert_batch(self, batch):
        """Inserta un lote de registros en PostgreSQL"""
        try:
            with self.conn.cursor() as cur:
                execute_batch(
                    cur,
                    """
                    INSERT INTO crimes (DR_NO, report_date, victim_age, victim_sex, crm_cd_desc)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (DR_NO) DO NOTHING
                    """,
                    batch,
                    page_size=len(batch)
                self.conn.commit()
                print(f"Insertado lote de {len(batch)} registros")
        except Exception as e:
            print(f"Error insertando lote: {e}")
            self.conn.rollback()

if __name__ == "__main__":
    # Configuración de Neon.tech
    DB_CONFIG = {
        'host': 'ep-curly-recipe-a50hnh5z-pooler.us-east-2.aws.neon.tech',
        'database': 'crimes',
        'user': 'crimes_owner',
        'password': 'npg_QUkH7TfKZlF8'
    }
    
    # Configuración Kafka
    KAFKA_CONFIG = {
        'bootstrap_servers': ['localhost:9092'],
        'group_id': 'neon-crime-consumers',
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': False,
        'consumer_timeout_ms': 60000,
        'max_poll_records': 1000
    }
    
    try:
        with NeonCrimeConsumer(DB_CONFIG) as consumer:
            kafka_consumer = KafkaConsumer(
                'crime_records',
                **KAFKA_CONFIG,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            
            print("Iniciando consumo de mensajes...")
            consumer.process_messages(kafka_consumer)
            
    except KeyboardInterrupt:
        print("\nDeteniendo consumidor...")
    except Exception as e:
        print(f"Error fatal: {e}")
    finally:
        print("Proceso terminado")
