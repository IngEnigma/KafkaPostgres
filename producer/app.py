import logging
from fastapi import FastAPI, HTTPException
from confluent_kafka import Producer
import requests
import json
import os

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configuración de Kafka (puede sobreescribirse con variables de entorno)
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "client.id": "kafka-crime-producer",
}

# URL del JSONL (podría ser un parámetro en el POST)
JSONL_URL = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"

# Tópico de Kafka
KAFKA_TOPIC = "crimes"

# Inicialización de FastAPI
app = FastAPI()

# Inicialización del productor Kafka
producer = Producer(KAFKA_CONFIG)

def delivery_report(err, msg):
    """Callback para confirmar la entrega del mensaje en Kafka."""
    if err:
        logger.error(f"Error al enviar mensaje a Kafka: {err}")
    else:
        logger.info(f"Mensaje enviado al tópico {msg.topic()} [partición {msg.partition()}]")

def send_to_kafka(message: str):
    """Envía un mensaje al tópico de Kafka con manejo de errores."""
    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            value=message.encode("utf-8"),
            callback=delivery_report,
        )
        producer.flush()  # Asegura que el mensaje se envía inmediatamente
    except Exception as e:
        logger.error(f"Error en el productor Kafka: {e}")
        raise HTTPException(status_code=500, detail="Error al enviar mensaje a Kafka")

@app.post("/produce-crimes")
async def produce_crimes():
    """
    Endpoint que:
    1. Descarga el JSONL desde la URL.
    2. Parsea cada línea.
    3. Envía cada registro al tópico de Kafka.
    """
    try:
        # Descarga el JSONL
        response = requests.get(JSONL_URL)
        response.raise_for_status()  # Lanza error si HTTP != 200
        
        # Procesa cada línea
        lines = response.text.strip().split("\n")
        valid_records = 0
        
        for line in lines:
            if not line.strip():
                continue  # Ignora líneas vacías
            
            try:
                # Valida que sea JSON válido (opcional)
                json.loads(line)
                send_to_kafka(line)
                valid_records += 1
            except json.JSONDecodeError:
                logger.warning(f"Línea inválida (no JSON): {line[:50]}...")
                continue
        
        return {
            "status": "success",
            "message": f"Se enviaron {valid_records}/{len(lines)} registros a Kafka",
        }
    
    except requests.RequestException as e:
        logger.error(f"Error al descargar el JSONL: {e}")
        raise HTTPException(status_code=400, detail="No se pudo descargar el archivo JSONL")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
