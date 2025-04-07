# Usa una imagen ligera de Python
FROM python:3.9-slim

WORKDIR /app

# Copia los archivos necesarios
COPY ./producer/requirements.txt .
COPY ./producer/app.py .

# Instala dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Variables de entorno (opcionales, pueden sobreescribirse en docker-compose)
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV JSONL_URL=https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl
ENV KAFKA_TOPIC=crimes

# Puerto expuesto (FastAPI usa 8000 por defecto)
EXPOSE 8000

# Comando para iniciar la API
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
