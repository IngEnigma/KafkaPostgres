# Dockerfile
FROM python:3.10-slim-bullseye

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar aplicaciones
COPY producer.py consumer.py ./

# Copiar dependencias y scripts
COPY requirements.txt run.sh ./

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Exponer puertos necesarios
EXPOSE 9092 5432

# Permisos de ejecución para el script
RUN chmod +x run.sh

# Comando por defecto (se puede sobreescribir)
CMD ["./run.sh"]
