import json
import asyncio
import logging
import os
from typing import Optional, Dict, Any
from fastapi import FastAPI
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import google.generativeai as genai
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("llm-service")

# =======================
# Config vía entorno
# =======================
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_IN = os.getenv("TOPIC_IN", "questions.pending")
TOPIC_OK = os.getenv("TOPIC_OK", "answers.success")
TOPIC_ERR_OVERLOAD = os.getenv("TOPIC_ERR_OVERLOAD", "answers.error.overload")
TOPIC_ERR_QUOTA = os.getenv("TOPIC_ERR_QUOTA", "answers.error.quota")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")  # define esto en tu .env
if not GOOGLE_API_KEY:
    logger.warning("GOOGLE_API_KEY no está definido (Gemini fallará al invocar).")

genai.configure(api_key=GOOGLE_API_KEY)

kafka_consumer: Optional[AIOKafkaConsumer] = None
kafka_producer: Optional[AIOKafkaProducer] = None
consumer_task: Optional[asyncio.Task] = None

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# =======================
# Gemini
# =======================
async def generate_gemini_response(prompt: str) -> str:
    """
    El SDK es sincrónico; se ejecuta en executor para no bloquear el loop.
    """
    model = genai.GenerativeModel("gemini-2.5-flash")
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, lambda: model.generate_content(prompt))
    txt = (getattr(resp, "text", "") or "").strip()
    return txt or "Lo siento, no pude generar una respuesta para esa pregunta."

def classify_error_for_topic(exc: Exception) -> str:
    """
    Heurística mínima para enrutar el error:
      - 429 / 'quota' / 'rate limit' / 'exceeded'  -> quota
      - 500/503 / 'timeout' / 'unavailable' / 'overload' -> overload
      - por defecto -> overload
    """
    s = str(exc).lower()
    if "429" in s or "quota" in s or "rate limit" in s or "exceeded" in s:
        return TOPIC_ERR_QUOTA
    if "503" in s or "500" in s or "unavailable" in s or "timeout" in s or "overload" in s or "overloaded" in s:
        return TOPIC_ERR_OVERLOAD
    return TOPIC_ERR_OVERLOAD

# =======================
# Kafka helpers
# =======================
async def send_json(topic: str, value: Dict[str, Any], key: Optional[str] = None, trace_id: Optional[str] = None):
    if not kafka_producer:
        raise RuntimeError("Kafka producer no disponible")
    key_bytes = key.encode("utf-8") if key else None
    headers = [
        ("trace-id", (trace_id or value.get("id", "")).encode("utf-8")),
        ("source", b"llm-service"),
        ("sent-at", now_iso().encode("utf-8")),
    ]
    await kafka_producer.send_and_wait(
        topic,
        value=value,
        key=key_bytes,
        headers=headers,
    )

# =======================
# Procesamiento
# =======================
async def process_message(msg: Dict[str, Any]):
    """
    msg esperado: { id, question, timestamp, attempt?, regens? }
    Produce:
      - answers.success: {question_id, question, answer}
      - answers.error.overload|quota: {question_id, question, error, attempt}
    """
    q = (msg.get("question") or "").strip()
    qid = msg.get("id") or msg.get("question_id") or ""
    if not q:
        logger.warning("Mensaje recibido sin 'question'; se ignora.")
        return

    try:
        logger.info(f"Procesando {qid}: {q[:80]}...")
        answer = await generate_gemini_response(q)
        out = {
            "question_id": qid,
            "question": q,
            "answer": answer,
            "timestamp": msg.get("timestamp") or now_iso(),
        }
        await send_json(TOPIC_OK, out, key=qid, trace_id=qid)
        logger.info(f"✓ success → {TOPIC_OK} ({qid})")

    except Exception as e:
        topic_err = classify_error_for_topic(e)
        out = {
            "question_id": qid,
            "question": q,
            "error": str(e),
            "attempt": int(msg.get("attempt", 0)) + 1,
            "timestamp": msg.get("timestamp") or now_iso(),
        }
        await send_json(topic_err, out, key=qid, trace_id=qid)
        logger.warning(f"↻ routed error → {topic_err} ({qid}): {e}")

async def consume_loop():
    try:
        assert kafka_consumer is not None
        async for record in kafka_consumer:
            try:
                await process_message(record.value)
            except Exception as inner:
                logger.error(f"Error procesando record: {inner}")
    except asyncio.CancelledError:
        logger.info("consume_loop cancelado")
    except Exception as e:
        logger.error(f"Fallo en consume_loop: {e}")

# =======================
# Ciclo de vida FastAPI
# =======================
async def init_kafka():
    global kafka_consumer, kafka_producer, consumer_task
    kafka_consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id="worker-llm",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=10,
        request_timeout_ms=15000,
        retry_backoff_ms=500,
    )
    await kafka_consumer.start()
    await kafka_producer.start()
    logger.info(f"Kafka listo. IN={TOPIC_IN} OUT=({TOPIC_OK}, {TOPIC_ERR_OVERLOAD}, {TOPIC_ERR_QUOTA})")
    # Lanza el consumo asíncrono
    consumer_task = asyncio.create_task(consume_loop())

async def close_kafka():
    global kafka_consumer, kafka_producer, consumer_task
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except Exception:
            pass
    if kafka_consumer:
        await kafka_consumer.stop()
        kafka_consumer = None
    if kafka_producer:
        await kafka_producer.stop()
        kafka_producer = None
    logger.info("Conexiones Kafka cerradas")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_kafka()
    try:
        yield
    finally:
        await close_kafka()

app = FastAPI(
    title="LLM Service",
    description="Servicio de generación de respuestas (Gemini) para Tarea 2",
    version="1.1.0",
    lifespan=lifespan,
)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "llm",
        "kafka_connected": (kafka_consumer is not None) and (kafka_producer is not None),
        "bootstrap": BOOTSTRAP_SERVERS,
        "in": TOPIC_IN,
        "out_ok": TOPIC_OK,
        "out_err_overload": TOPIC_ERR_OVERLOAD,
        "out_err_quota": TOPIC_ERR_QUOTA,
    }

@app.get("/")
async def root():
    return {"message": "LLM Service - Tarea 2 (Gemini→Kafka)"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("llm_service:app", host="0.0.0.0", port=8003, reload=False)
