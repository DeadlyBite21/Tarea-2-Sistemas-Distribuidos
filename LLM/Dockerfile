FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instalar wait-for-it para esperar que Kafka esté listo antes de arrancar el servicio
RUN apt-get update && apt-get install -y wget && \
    wget -O /usr/local/bin/wait-for-it https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh && \
    chmod +x /usr/local/bin/wait-for-it && \
    rm -rf /var/lib/apt/lists/*

COPY . .

# Variables de entorno para Kafka
ENV BOOTSTRAP_SERVERS=kafka:9092
ENV TOPIC_IN=questions.pending
ENV TOPIC_OK=answers.success
ENV TOPIC_ERR_OVERLOAD=answers.error.overload
ENV TOPIC_ERR_QUOTA=answers.error.quota

EXPOSE 8003

# Espera a Kafka antes de iniciar uvicorn
CMD ["wait-for-it", "kafka:9092", "--timeout=60", "--", "uvicorn", "llm_service:app", "--host", "0.0.0.0", "--port", "8003"]

