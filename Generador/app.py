import json
import asyncio
import logging
import uuid
import os
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer
import random
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("generator")

# =======================
# Config (vía variables)
# =======================
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "questions.pending")
STORAGE_SERVICE_URL = os.getenv("STORAGE_SERVICE_URL", "http://storage:8000")

# Preguntas de respaldo si falla el storage
FALLBACK_QUESTIONS: List[str] = [
    "What is a distributed system?",
    "Explain at-least-once vs exactly-once semantics.",
    "How does Kafka ensure durability?",
    "What is the role of a JobManager in Flink?",
    "Compare push vs pull-based backpressure.",
    "What is partitioning and why does it matter in Kafka?",
]

kafka_producer: Optional[AIOKafkaProducer] = None

# =======================
# Utilidades
# =======================
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def fetch_questions_from_storage(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Intenta obtener preguntas desde storage (/qa).
    Devuelve una lista de items con campos: question_id, question, answer, score, created_at (según storage/api.py).
    """
    url = f"{STORAGE_SERVICE_URL}/qa"
    params = {"limit": str(limit)}
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                raise RuntimeError(f"storage /qa status={resp.status}")
            data = await resp.json()
            if not isinstance(data, list):
                raise RuntimeError("storage /qa payload inesperado (se esperaba lista)")
            return data

async def get_random_question_from_storage() -> str:
    """
    Toma una pregunta desde storage si hay resultados; si no, usa fallback local.
    """
    try:
        rows = await fetch_questions_from_storage(limit=50)
        candidates = [r.get("question", "") for r in rows if r.get("question")]
        if candidates:
            q = random.choice(candidates).strip()
            if q:
                return q
        logger.warning("Storage sin preguntas válidas; usando fallback.")
    except Exception as e:
        logger.warning(f"No se pudo obtener preguntas de storage: {e}; usando fallback.")

    return random.choice(FALLBACK_QUESTIONS)

async def send_to_kafka(message: Dict[str, Any]) -> None:
    """
    Envía un mensaje al tópico configurado, con key=id y headers de trazabilidad.
    """
    global kafka_producer
    if not kafka_producer:
        raise RuntimeError("Kafka producer no disponible")

    key_bytes = message["id"].encode("utf-8")
    headers = [
        ("trace-id", message["id"].encode("utf-8")),
        ("source", b"generator-service"),
        ("sent-at", now_iso().encode("utf-8")),
        # Puedes agregar ("content-type", b"application/json")
    ]
    await kafka_producer.send_and_wait(
        TOPIC_REQUESTS,
        value=message,
        key=key_bytes,
        headers=headers,
    )

# =======================
# Ciclo de vida FastAPI
# =======================
async def init_kafka():
    global kafka_producer
    try:
        kafka_producer = AIOKafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",                 # mayor garantía
            linger_ms=10,               # un pequeño batch
            request_timeout_ms=15000,
            retry_backoff_ms=500,
        )
        await kafka_producer.start()
        logger.info(f"Kafka producer iniciado ({BOOTSTRAP_SERVERS}) → topic={TOPIC_REQUESTS}")
    except Exception as e:
        logger.error(f"Error al iniciar Kafka: {e}")
        raise

async def close_kafka():
    global kafka_producer
    if kafka_producer:
        await kafka_producer.stop()
        kafka_producer = None
    logger.info("Kafka producer detenido")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_kafka()
    try:
        yield
    finally:
        await close_kafka()

app = FastAPI(
    title="Generator Service",
    description="Servicio generador de tráfico de preguntas (Tarea 2)",
    version="1.1.0",
    lifespan=lifespan,
)

# =======================
# Endpoints
# =======================
@app.post("/generate")
async def generate_question():
    try:
        # 1. SELECCIONA UNA PREGUNTA (Tu lógica de fallback o de un CSV es mejor aquí)
        #    Para este ejemplo, usaré tu lógica actual, pero idealmente
        #    la fuente de preguntas debería ser externa (un archivo, etc.)
        question = await get_random_question_from_storage() # O mejor: get_question_from_csv()

        # 2. CONSULTA AL STORAGE (BDD) SI YA EXISTE
        url = f"{STORAGE_SERVICE_URL}/check" # Necesitas un endpoint en la BDD
        params = {"question": question}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # 3. SI EXISTE (con score), NO LA ENVÍES A KAFKA
                    if data.get("processed") and data.get("score") is not None:
                        logger.info(f"Pregunta ya procesada (Score: {data['score']}). Saltando envío a Kafka.")
                        return {"status": "skipped", "message": "Question already processed", "data": data}
                elif resp.status != 404:
                    # Si no es un 404 (no encontrado), es un error
                    logger.warning(f"Error al consultar storage/check: {resp.status}")
                    # Decide si continuar o fallar. Continuemos por ahora.
        
        # 4. SI NO EXISTE, ENVÍA A KAFKA
        msg = {
            "id": str(uuid.uuid4()),
            "question": question,
            "timestamp": now_iso(),
            "attempt": 0,
            "regens": 0,
        }
        await send_to_kafka(msg)
        logger.info(f"→ Nueva pregunta enviada a {TOPIC_REQUESTS}: {question}")
        return {"status": "success", "topic": TOPIC_REQUESTS, "question": question, "id": msg["id"]}

    except Exception as e:
        logger.error(f"Error /generate: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate/custom")
async def generate_custom_question(payload: Dict[str, Any]):
    try:
        q = (payload.get("question") or "").strip()
        if not q:
            raise HTTPException(status_code=400, detail="Pregunta no puede estar vacía")
        msg = {
            "id": str(uuid.uuid4()),
            "question": q,
            "timestamp": now_iso(),
            "attempt": 0,
            "regens": 0,
        }
        await send_to_kafka(msg)
        logger.info(f"→ pregunta personalizada enviada a {TOPIC_REQUESTS}: {q}")
        return {"status": "success", "topic": TOPIC_REQUESTS, "question": q, "id": msg["id"]}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error /generate/custom: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate/batch")
async def generate_batch_questions(batch_data: Dict[str, Any]):
    try:
        # acepta num_questions o count; clamp 1..10000
        count = batch_data.get("num_questions", batch_data.get("count", 5))
        count = int(max(1, min(int(count), 10_000)))

        ids: List[str] = []
        # backpressure suave para no saturar
        for _ in range(count):
            q = await get_random_question_from_storage()
            msg = {
                "id": str(uuid.uuid4()),
                "question": q,
                "timestamp": now_iso(),
                "attempt": 0,
                "regens": 0,
            }
            await send_to_kafka(msg)
            ids.append(msg["id"])
            # micro-sleep evita monopolizar el event-loop cuando count es grande
            await asyncio.sleep(0)

        logger.info(f"→ batch enviado: {count} mensajes a {TOPIC_REQUESTS}")
        return {"status": "success", "topic": TOPIC_REQUESTS, "count": count, "ids": ids[:100]}
    except Exception as e:
        logger.error(f"Error /generate/batch: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "generator",
        "kafka_connected": kafka_producer is not None,
        "bootstrap": BOOTSTRAP_SERVERS,
        "topic": TOPIC_REQUESTS,
    }

@app.get("/")
async def root():
    return {
        "message": "Generator Service - Tarea 2",
        "endpoints": {
            "POST /generate": "Genera 1 pregunta desde storage/fallback",
            "POST /generate/custom": "Genera 1 pregunta personalizada {question}",
            "POST /generate/batch": "Genera N preguntas {num_questions}",
            "GET /health": "Estado del servicio",
        },
        "config": {
            "BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
            "TOPIC_REQUESTS": TOPIC_REQUESTS,
            "STORAGE_SERVICE_URL": STORAGE_SERVICE_URL,
        },
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
