# En: generator/app.py
from typing import Optional
import os
import pandas as pd
from kafka import KafkaProducer
import json
import time
import logging
import random
import uuid
import httpx # Para hacer peticiones HTTP a la BDD

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("smart_generator")

# --- Configuración ---
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_TO_PRODUCE = "questions.llm"
DATASET_PATH = "/app/train.csv"
GENERATION_INTERVAL_SECONDS = 5
# URL del servicio BDD (desde docker-compose)
BDD_SERVICE_URL = os.getenv("STORAGE_SERVICE_URL", "http://bdd:8000")

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

def check_question_in_bdd(question: str) -> Optional[bool]:
    """
    Consulta a la BDD si la pregunta ya existe.
    Devuelve:
    - True: si existe (Cache Hit)
    - False: si NO existe (Cache Miss - 404)
    - None: si hay un error de conexión
    """
    try:
        url = f"{BDD_SERVICE_URL}/check"
        response = httpx.get(url, params={"question": question}, timeout=5.0)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("score") is not None:
                logger.info(f"CACHE HIT: La BDD ya tiene la pregunta (Score: {data['score']}).")
                return True # ¡Existe!
        
        elif response.status_code == 404:
            logger.info("CACHE MISS: La BDD no tiene la pregunta.")
            return False # No existe
            
        else:
            logger.warning(f"Error al consultar la BDD (Status: {response.status_code}).")
            return None # Error (ej. 500 en la BDD)

    except httpx.RequestError as e:
        logger.error(f"Error de conexión al consultar la BDD en {e.request.url!r}: {e}.")
        return None # Error de conexión
    """
    Consulta a la BDD si la pregunta ya existe Y tiene un score.
    Devuelve True si existe, False si no.
    """
    try:
        url = f"{BDD_SERVICE_URL}/check"
        # El endpoint /check de la BDD espera un parámetro 'question'
        response = httpx.get(url, params={"question": question}, timeout=5.0)
        
        # 200 OK = La BDD la tiene
        if response.status_code == 200:
            data = response.json()
            if data.get("score") is not None:
                logger.info(f"CACHE HIT: La BDD ya tiene la pregunta (Score: {data['score']}).")
                return True # ¡Existe!
        
        # 404 Not Found = La BDD no la tiene
        elif response.status_code == 404:
            logger.info("CACHE MISS: La BDD no tiene la pregunta.")
            return False # No existe
            
        else:
            # Otro error (ej. 500)
            logger.warning(f"Error al consultar la BDD (Status: {response.status_code}). Se asumirá que no existe.")
            return False

    except httpx.RequestError as e:
        logger.error(f"Error de conexión al consultar la BDD en {e.request.url!r}: {e}. Reintentando...")
        time.sleep(2) # Espera si la BDD está caída
        return False

def run_generator_loop(producer, questions):
    """Bucle infinito para generar tráfico."""
    while True:
        try:
            # 1. Elige una pregunta aleatoria
            question_text = random.choice(questions)
            
            # 2. ----- ¡LÓGICA CORREGIDA! -----
            bdd_status = check_question_in_bdd(question_text)
            
            if bdd_status is True:
                # 1. BDD la tiene. Saltamos.
                pass
            
            elif bdd_status is None:
                # 2. Error de conexión. Pausamos y reintentamos el bucle.
                logger.warning("Pausando generador, no se pudo conectar a la BDD. Reintentando...")
                
            else: 
                # 3. BDD status es False (404). La enviamos a Kafka.
                message = {
                    "id": str(uuid.uuid4()),
                    "question": question_text,
                    "timestamp": time.time(),
                    "attempt": 1,
                    "regens": 0
                }
                
                producer.send(TOPIC_TO_PRODUCE, message)
                producer.flush()
                logger.info(f"→ NUEVA PREGUNTA enviada a Kafka (ID: {message['id']}): {question_text[:50]}...")
            
            # 4. Espera
            time.sleep(GENERATION_INTERVAL_SECONDS)
            
        except Exception as e:
            logger.error(f"Error grave en el bucle del generador: {e}")
            time.sleep(5)
    """Bucle infinito para generar tráfico."""
    while True:
        try:
            # 1. Elige una pregunta aleatoria
            question_text = random.choice(questions)
            
            # 2. ----- ¡LÓGICA CLAVE DE LA TAREA! -----
            # Consulta a la BDD ANTES de enviar a Kafka
            if check_question_in_bdd(question_text):
                # Si la BDD ya la tiene, no hace nada y vuelve al inicio del bucle
                time.sleep(GENERATION_INTERVAL_SECONDS)
                continue 
            
            # 3. Si no está en la BDD, la envía a Kafka
            message = {
                "id": str(uuid.uuid4()),
                "question": question_text,
                "timestamp": time.time(),
                "attempt": 1,
                "regens": 0
            }
            
            producer.send(TOPIC_TO_PRODUCE, message)
            producer.flush()
            
            logger.info(f"→ NUEVA PREGUNTA enviada a Kafka (ID: {message['id']}): {question_text[:50]}...")
            
            # 4. Espera
            time.sleep(GENERATION_INTERVAL_SECONDS)
            
        except Exception as e:
            logger.error(f"Error en el bucle del generador: {e}")
            time.sleep(5)

if __name__ == "__main__":
    logger.info("Iniciando Generador de Tráfico Inteligente...")
    logger.info(f"Consultando BDD en: {BDD_SERVICE_URL}")
    
    questions = load_questions_from_csv()
    if questions:
        producer = connect_kafka_producer()
        run_generator_loop(producer, questions)
    else:
        logger.error("El generador no puede iniciar sin preguntas. Terminando.")