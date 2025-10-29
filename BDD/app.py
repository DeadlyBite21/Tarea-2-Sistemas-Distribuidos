import os
import json
import threading
import time
import logging
import uuid 
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse 
from sqlalchemy import create_engine, Column, String, Float, Integer
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy.exc import OperationalError
from kafka import KafkaConsumer, KafkaProducer 
from kafka.errors import NoBrokersAvailable
from pydantic import BaseModel 
from contextlib import asynccontextmanager

# =================================================================
# Configuración
# =================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("storage_gateway_service")

DB_URL = os.getenv("DB_URL", "sqlite:///./data/data.db")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")

# Tópicos
TOPIC_CONSUME_VALIDATED = "questions.validated" 
TOPIC_PRODUCE_NEW = "questions.llm"         

# Variable global para el productor
kafka_producer: KafkaProducer = None

os.makedirs(os.path.dirname(DB_URL.split("///")[-1]), exist_ok=True)
engine = create_engine(DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class QuestionAnswer(Base):
    __tablename__ = "processed_questions"
    id = Column(Integer, primary_key=True, index=True)
    question = Column(String, unique=True, index=True)
    answer = Column(String)
    score = Column(Float, index=True)

def create_db_and_tables():
    logger.info("Verificando y creando tablas de la BDD...")
    Base.metadata.create_all(bind=engine)
    logger.info("Tablas de la BDD listas.")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def consume_results_from_kafka():
    logger.info("Iniciando hilo consumidor de Kafka...")
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_CONSUME_VALIDATED,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                group_id='storage_consumer_group',
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            logger.info(f"Consumidor conectado. Escuchando tópico: {TOPIC_CONSUME_VALIDATED}")
            for message in consumer:
                try:
                    data = message.value
                    if not all(k in data for k in ['question', 'answer', 'score']):
                        continue
                    db = SessionLocal()
                    try:
                        existing_qa = db.query(QuestionAnswer).filter(QuestionAnswer.question == data['question']).first()
                        if existing_qa:
                            existing_qa.answer = data['answer']
                            existing_qa.score = data['score']
                            logger.info(f"Consumidor: Actualizando pregunta existente: {data['question'][:20]}...")
                        else:
                            new_qa = QuestionAnswer(
                                question=data['question'],
                                answer=data['answer'],
                                score=data['score']
                            )
                            db.add(new_qa)
                            logger.info(f"Consumidor: Guardando nueva pregunta: {data['question'][:20]}...")
                        db.commit()
                    except Exception as e:
                        db.rollback()
                    finally:
                        db.close()
                except Exception as e:
                    logger.error(f"Error en bucle consumidor: {e}")

        except Exception as e:
            logger.error(f"Error al conectar consumidor de Kafka: {e}. Reintentando...")
            time.sleep(5)

def create_kafka_producer():
    """Función para conectar el productor de Kafka con reintentos."""
    global kafka_producer
    while True:
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Productor de Kafka (para BDD) conectado.")
            return
        except NoBrokersAvailable:
            logger.warning("No se pudo conectar el productor de Kafka. Reintentando en 5s...")
            time.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Iniciando lifespan (startup)...")
    create_db_and_tables()
    consumer_thread = threading.Thread(target=consume_results_from_kafka, daemon=True)
    consumer_thread.start()
    producer_thread = threading.Thread(target=create_kafka_producer, daemon=True)
    producer_thread.start()
    
    yield
    
    logger.info("Cerrando lifespan (shutdown)...")
    if kafka_producer:
        kafka_producer.close()

app = FastAPI(title="Servicio de Almacenamiento y Gateway", lifespan=lifespan)

class QuestionRequest(BaseModel):
    question: str

@app.post("/process_question")
async def process_question(request: QuestionRequest, db: Session = Depends(get_db)):
    """
    Este es el endpoint principal (gateway).
    1. Revisa si la pregunta está en la BDD (caché).
    2. Si está, la devuelve (200 OK).
    3. Si no está, la envía a Kafka y devuelve (202 Accepted).
    """
    global kafka_producer
    question_text = request.question.strip()

    try:
        result = db.query(QuestionAnswer).filter(QuestionAnswer.question == question_text).first()
        
        if result and result.score is not None:
            logger.info(f"GATEWAY (HIT): Pregunta encontrada en BDD. {question_text[:30]}...")
            return {
                "status": "cache_hit",
                "processed": True,
                "question": result.question,
                "answer": result.answer,
                "score": result.score
            }
        else:
            logger.info(f"GATEWAY (MISS): Encolando en Kafka. {question_text[:30]}...")
            
            if not kafka_producer:
                logger.error("Productor de Kafka no está listo. No se puede encolar.")
                raise HTTPException(status_code=503, detail="Servicio de encolado no disponible")

            message = {
                "id": str(uuid.uuid4()),
                "question": question_text,
                "timestamp": time.time(),
                "attempt": 1,
                "regens": 0
            }
            
            kafka_producer.send(TOPIC_PRODUCE_NEW, message)
            kafka_producer.flush()
            
            return JSONResponse(
                status_code=202, 
                content={"status": "queued", "id": message["id"]}
            )

    except OperationalError as e:
        logger.error(f"Error de base de datos en /process_question: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    except Exception as e:
        logger.error(f"Error inesperado en /process_question: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/qa")
async def get_all_questions(limit: int = 50, db: Session = Depends(get_db)):
    return db.query(QuestionAnswer).limit(limit).all()

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "storage-gateway"}