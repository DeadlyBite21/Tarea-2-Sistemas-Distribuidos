import json
import asyncio
import logging
import os
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import google.generativeai as genai
# Importamos las excepciones específicas de Google para enrutar errores
from google.api_core import exceptions as google_exceptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =======================
# Configuración (vía variables de entorno)
# =======================

# --- Kafka ---
# CORREGIDO: Usamos variables y el puerto correcto 9092
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "questions.llm") # Tópico de entrada
CONSUMER_GROUP_ID = "llm_consumer_group"

# --- Tópicos de Salida (Éxito y Errores) ---
# Esta es la lógica clave de enrutamiento requerida por la tarea
TOPIC_SUCCESS = os.getenv("TOPIC_SUCCESS", "questions.answers")
TOPIC_ERROR_QUOTA = os.getenv("TOPIC_ERROR_QUOTA", "llm.errors.quota")
TOPIC_ERROR_OVERLOAD = os.getenv("TOPIC_ERROR_OVERLOAD", "llm.errors.overload")

# --- Gemini ---
# CORREGIDO: Clave de API eliminada. Debe ser inyectada por Docker Compose.
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
if not GOOGLE_API_KEY:
    logger.error("FATAL: La variable de entorno GOOGLE_API_KEY no está configurada.")
    raise ValueError("GOOGLE_API_KEY no está configurada. El servicio no puede iniciar.")

genai.configure(api_key=GOOGLE_API_KEY)
# CORREGIDO: 'gemini-1.5-flash' es un modelo real.
GEMINI_MODEL = genai.GenerativeModel('gemini-1.5-flash')

# Variables globales para los clientes de Kafka
kafka_consumer: Optional[AIOKafkaConsumer] = None
kafka_producer: Optional[AIOKafkaProducer] = None

# =======================
# Ciclo de vida de Kafka
# =======================

async def init_kafka():
    """Inicializar consumidor y productor de Kafka"""
    global kafka_consumer, kafka_producer
    
    try:
        kafka_consumer = AIOKafkaConsumer(
            TOPIC_REQUESTS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            group_id=CONSUMER_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        kafka_producer = AIOKafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks="all" # 'all' para mayor durabilidad
        )
        
        await kafka_consumer.start()
        await kafka_producer.start()
        
        logger.info(f"Kafka consumer/producer iniciado. Escuchando en {TOPIC_REQUESTS}")
        
        # Iniciar el loop de consumo en una tarea de fondo
        asyncio.create_task(consume_loop())
        
    except Exception as e:
        logger.error(f"Error fatal al inicializar Kafka: {e}")
        raise

async def close_kafka():
    """Cerrar conexiones de Kafka"""
    global kafka_consumer, kafka_producer
    if kafka_consumer:
        await kafka_consumer.stop()
    if kafka_producer:
        await kafka_producer.stop()
    logger.info("Conexiones de Kafka cerradas")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestión del ciclo de vida de la aplicación FastAPI"""
    await init_kafka()
    yield
    await close_kafka()

# =======================
# Lógica Principal del Servicio
# =======================

async def consume_loop():
    """Loop principal para consumir mensajes de Kafka"""
    logger.info("Iniciando loop de consumo...")
    try:
        async for message in kafka_consumer:
            logger.info(f"Mensaje recibido (Tópico: {message.topic}, Offset: {message.offset})")
            await process_message(message.value)
    except asyncio.CancelledError:
        logger.info("Loop de consumo cancelado.")
    except Exception as e:
        logger.error(f"Error crítico en consume_loop: {e}", exc_info=True)

async def generate_gemini_response(question: str) -> str:
    """
    Genera una respuesta usando Gemini.
    CORREGIDO: Esta función ahora DEJA QUE LOS ERRORES (ej. 429, 500) ocurran
    para que 'process_message' pueda atraparlos y enrutarlos.
    """
    try:
        # CORREGIDO: Usar el método async nativo de la librería
        response = await GEMINI_MODEL.generate_content_async(question)
        
        if response and response.text:
            return response.text.strip()
        else:
            # Esto es un fallo de generación (ej. filtro de seguridad), no un error de API
            logger.warning(f"Gemini no devolvió texto para la pregunta: {question}")
            raise ValueError("Respuesta de Gemini vacía o bloqueada por filtros.")
    except Exception as e:
        logger.error(f"Error en generate_gemini_response: {e}")
        # Re-lanza la excepción para que process_message la maneje
        raise e

async def process_message(message_data: Dict[str, Any]):
    """
    Procesa el mensaje, llama a Gemini y ENRUTA la respuesta
    al tópico de Kafka correcto (éxito, reintento-cuota, reintento-sobrecarga).
    Esta es la lógica clave requerida en la Tarea 2[cite: 38, 49, 51].
    """
    question = message_data.get('question', '')
    msg_id = message_data.get('id', 'unknown-id')

    if not question:
        logger.warning("Mensaje sin 'question' recibido, descartando.")
        return

    logger.info(f"Procesando (ID: {msg_id}): {question[:50]}...")
    
    try:
        # 1. Intenta generar la respuesta
        response_text = await generate_gemini_response(question)
        
        # 2. ÉXITO: Enviar al tópico de respuestas
        answer_message = {
            'id': msg_id,
            'question': question,
            'answer': response_text,
            'timestamp': message_data.get('timestamp', ''),
            'original_message': message_data # Útil para Flink
        }
        
        await kafka_producer.send(TOPIC_SUCCESS, answer_message)
        logger.info(f"Éxito (ID: {msg_id}). Respuesta enviada a {TOPIC_SUCCESS}")

    # 3. ERROR DE CUOTA/LÍMITE (HTTP 429)
    except google_exceptions.ResourceExhausted as e:
        logger.warning(f"Error de Cuota/Límite (429) (ID: {msg_id}). Enviando a {TOPIC_ERROR_QUOTA}.")
        # Reenviamos el mensaje ORIGINAL para el reintento
        await kafka_producer.send(TOPIC_ERROR_QUOTA, message_data) 
    
    # 4. ERROR DE SOBRECARGA DEL SERVIDOR (HTTP 500, 503)
    except (google_exceptions.InternalServerError, google_exceptions.ServiceUnavailable) as e:
        logger.warning(f"Error de Sobrecarga (500/503) (ID: {msg_id}). Enviando a {TOPIC_ERROR_OVERLOAD}.")
        # Reenviamos el mensaje ORIGINAL para el reintento
        await kafka_producer.send(TOPIC_ERROR_OVERLOAD, message_data)
    
    # 5. OTROS ERRORES (ej. ValueError, BadRequest, etc.)
    except Exception as e:
        logger.error(f"Error irrecuperable (ID: {msg_id}): {e}. Mensaje descartado.")
        # Opcional: Enviar a un tópico de "fallos" (Dead-Letter Queue)
        # await kafka_producer.send('questions.failed', message_data)

# =======================
# Endpoints de la API
# =======================

app = FastAPI(
    title="LLM Service (Consumidor)",
    description="Servicio que consume de Kafka, genera respuestas con Gemini y enruta a tópicos de éxito o error.",
    version="1.1.0 (Corregido)",
    lifespan=lifespan
)

@app.get("/health")
async def health_check():
    """Endpoint de verificación de salud"""
    is_consumer_running = kafka_consumer is not None and not kafka_consumer.closed()
    is_producer_running = kafka_producer is not None
    return {
        "status": "healthy",
        "service": "llm-consumer",
        "kafka_consumer_running": is_consumer_running,
        "kafka_producer_running": is_producer_running
    }

@app.get("/")
async def root():
    """Endpoint raíz"""
    return {
        "message": "LLM Service - Consumidor de Kafka",
        "consuming_from": TOPIC_REQUESTS,
        "publishing_to": {
            "success": TOPIC_SUCCESS,
            "quota_errors": TOPIC_ERROR_QUOTA,
            "overload_errors": TOPIC_ERROR_OVERLOAD
        }
    }

if __name__ == "__main__":
    import uvicorn
    # El puerto 8003 es el puerto INTERNO del contenedor
    uvicorn.run(app, host="0.0.0.0", port=8003)