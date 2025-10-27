# En generator/app.py
import os
import pandas as pd
from kafka import KafkaProducer
import json
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("batch_generator")

# Config
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_TO_PRODUCE = "questions.llm" # Tópico para el LLM
DATASET_PATH = "/app/train.csv"     # Ruta dentro del contenedor

def connect_kafka_producer():
    """Intenta conectarse a Kafka con reintentos."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Productor de Kafka conectado.")
            return producer
        except Exception as e:
            logger.warning(f"No se pudo conectar a Kafka: {e}. Reintentando en 5s...")
            time.sleep(5)

def load_and_send_data(producer):
    try:
        df = pd.read_csv(DATASET_PATH)
        df_trimmed = df.head(1000) # Limita a 1000 para la prueba
        
        logger.info(f"Enviando {len(df_trimmed)} preguntas a Kafka ({TOPIC_TO_PRODUCE})...")
        
        for index, row in df_trimmed.iterrows():
            message = {
                "id": f"q_{index}",
                "question": row['question'], # Asumiendo que la columna se llama 'question'
                "timestamp": time.time(),
                "attempt": 1,
                "regens": 0
            }
            producer.send(TOPIC_TO_PRODUCE, message)
        
        producer.flush()
        logger.info("Envío de preguntas completado.")
        
    except FileNotFoundError:
        logger.error(f"ERROR: No se encontró el dataset en {DATASET_PATH}")
    except Exception as e:
        logger.error(f"Error al cargar o enviar datos: {e}")

if __name__ == "__main__":
    producer = connect_kafka_producer()
    load_and_send_data(producer)