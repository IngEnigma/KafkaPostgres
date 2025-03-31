from kafka import KafkaProducer
import json
import requests
import time
from datetime import datetime

class CrimeDataProducer:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            compression_type='gzip',  # Compresión para reducir tamaño de mensajes
            retries=5,
            request_timeout_ms=30000
        )
        
    def fetch_crime_data(self, url):
        """Obtiene datos de crímenes desde la URL"""
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            for line in response.iter_lines():
                if line:
                    yield json.loads(line)
        except Exception as e:
            print(f"Error fetching data: {e}")
            raise
            
    def produce_to_kafka(self, topic, data_url, batch_size=10):
        """Envía datos a Kafka en lotes"""
        batch = []
        start_time = datetime.now()
        
        for i, crime in enumerate(self.fetch_crime_data(data_url)):
            try:
                # Validar datos básicos
                if not all(k in crime for k in ['DR_NO', 'report_date', 'victim_age', 'victim_sex', 'crm_cd_desc']):
                    print(f"Dato incompleto omitido: {crime}")
                    continue
                    
                batch.append(crime)
                
                if len(batch) >= batch_size:
                    self._send_batch(topic, batch)
                    batch = []
                    print(f"Enviado lote {i+1}...")
                    
            except Exception as e:
                print(f"Error procesando crimen {i}: {e}")
                
        # Enviar cualquier resto en el batch
        if batch:
            self._send_batch(topic, batch)
            
        print(f"Producción completada. Tiempo total: {datetime.now() - start_time}")
        
    def _send_batch(self, topic, batch):
        """Envía un lote de mensajes a Kafka"""
        for crime in batch:
            try:
                self.producer.send(topic, value=crime)
            except Exception as e:
                print(f"Error enviando crimen {crime['DR_NO']}: {e}")
        
        self.producer.flush()
        
    def close(self):
        self.producer.close()

if __name__ == "__main__":
    # Configuración
    KAFKA_SERVERS = ['localhost:9092']
    TOPIC = 'crime_records'
    DATA_URL = 'https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/part-00000-8b3d0568-ab40-4980-a6e8-e7e006621725-c000.json'
    
    # Iniciar productor
    producer = CrimeDataProducer(KAFKA_SERVERS)
    
    try:
        print("Iniciando producción de datos de crímenes...")
        producer.produce_to_kafka(TOPIC, DATA_URL)
    except KeyboardInterrupt:
        print("\nDeteniendo productor...")
    except Exception as e:
        print(f"Error fatal: {e}")
    finally:
        producer.close()
