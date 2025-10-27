import os
import pandas as pd
import json
import time
import logging
import random
import httpx # Sigue siendo necesario
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("simple_generator")

# --- Configuración ---
DATASET_PATH = "/app/train.csv"
GENERATION_INTERVAL_SECONDS = 5
# URL del servicio BDD (desde docker-compose)
BDD_SERVICE_URL = os.getenv("STORAGE_SERVICE_URL", "http://bdd:8000")

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

def send_question_to_gateway(question: str):
    """
    Envía la pregunta al servicio BDD (gateway) y loguea la respuesta.
    """
    try:
        url = f"{BDD_SERVICE_URL}/process_question"
        # Envía la pregunta en el cuerpo (body) de un POST
        response = httpx.post(url, json={"question": question}, timeout=10.0)
        
        if response.status_code == 200:
            # 200 OK = Cache Hit
            logger.info(f"Respuesta (Cache Hit): {question[:40]}...")
        elif response.status_code == 202:
            # 202 Accepted = Encolado en Kafka
            logger.info(f"Respuesta (Encolado): {question[:40]}...")
        else:
            # Error del servidor BDD
            logger.warning(f"Error del Gateway (Status: {response.status_code}): {response.text}")

    except httpx.RequestError as e:
        logger.error(f"Error de conexión al BDD-Gateway en {e.request.url!r}: {e}.")

def run_generator_loop(questions):
    """Bucle infinito para generar tráfico."""
    logger.info("Iniciando bucle de generación de tráfico...")
    client = httpx.Client() # Usamos un cliente persistente
    
    while True:
        try:
            question_text = random.choice(questions)
            
            # Simplemente envía la pregunta al gateway
            send_question_to_gateway(question_text)
            
            time.sleep(GENERATION_INTERVAL_SECONDS)
            
        except Exception as e:
            logger.error(f"Error grave en el bucle del generador: {e}")
            time.sleep(5)

if __name__ == "__main__":
    logger.info("Iniciando Generador de Tráfico (Simple)...")
    logger.info(f"Gateway de BDD en: {BDD_SERVICE_URL}")
    
    questions = load_questions_from_csv()
    if questions:
        # Pequeña espera para que la BDD inicie primero
        logger.info("Esperando 5s a que inicie la BDD...")
        time.sleep(5)
        run_generator_loop(questions)
    else:
        logger.error("El generador no puede iniciar sin preguntas. Terminando.")