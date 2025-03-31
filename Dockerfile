FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN wget -O /usr/local/bin/rpk https://github.com/redpanda-data/redpanda/releases/download/v23.3.8/rpk-linux-amd64
RUN chmod +x /usr/local/bin/rpk

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x start.sh

CMD ["./start.sh"]
