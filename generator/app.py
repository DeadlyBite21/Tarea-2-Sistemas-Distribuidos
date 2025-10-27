import os
import pandas as pd
from kafka import KafkaProducer
import json
import time
import logging
import random
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("periodic_generator")

# --- Configuración ---
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_TO_PRODUCE = "questions.llm"  # Tópico para el LLM
DATASET_PATH = "/app/train.csv"      # Ruta dentro del contenedor
GENERATION_INTERVAL_SECONDS = 5      # Envía una pregunta cada 5 segundos

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

def load_questions_from_csv():
    """Carga las preguntas del CSV a una lista en memoria."""
    try:
        column_names = ['id_num', 'question', 'context', 'answer']
        df = pd.read_csv(DATASET_PATH, header=None, names=column_names)
        
        # Filtra preguntas vacías o inválidas
        questions_list = df['question'].dropna().astype(str).str.strip().tolist()
        
        if not questions_list:
            logger.error("No se encontraron preguntas válidas en el dataset.")
            return None
            
        logger.info(f"Cargadas {len(questions_list)} preguntas del dataset.")
        return questions_list
        
    except FileNotFoundError:
        logger.error(f"ERROR: No se encontró el dataset en {DATASET_PATH}")
        return None
    except Exception as e:
        logger.error(f"Error al cargar o procesar el CSV: {e}")
        return None

def run_generator_loop(producer, questions):
    """Bucle infinito para generar tráfico."""
    while True:
        try:
            # 1. Elige una pregunta aleatoria
            question_text = random.choice(questions)
            
            # 2. Crea un nuevo mensaje con un ID único
            message = {
                "id": str(uuid.uuid4()), # ID único para esta nueva solicitud
                "question": question_text,
                "timestamp": time.time(),
                "attempt": 1,
                "regens": 0
            }
            
            # 3. Envía a Kafka
            producer.send(TOPIC_TO_PRODUCE, message)
            producer.flush() # Asegura que se envíe inmediatamente
            
            logger.info(f"→ Pregunta enviada (ID: {message['id']}): {question_text[:50]}...")
            
            # 4. Espera
            time.sleep(GENERATION_INTERVAL_SECONDS)
            
        except Exception as e:
            logger.error(f"Error en el bucle del generador: {e}")
            time.sleep(5) # Espera antes de reintentar el bucle

if __name__ == "__main__":
    questions = load_questions_from_csv()
    if questions:
        producer = connect_kafka_producer()
        run_generator_loop(producer, questions)
    else:
        logger.error("El generador no puede iniciar sin preguntas. Terminando.")