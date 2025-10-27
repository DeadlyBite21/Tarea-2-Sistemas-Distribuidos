# En: BDD/app.py

import os
import json
import threading
import time
import logging
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, Column, String, Float, Integer, inspect
from sqlalchemy.orm import sessionmaker, Session
from kafka import KafkaConsumer
from contextlib import asynccontextmanager # <-- 1. Importar asynccontextmanager
from sqlalchemy.exc import OperationalError

# --- CORRECCIÓN DE ALCHEMY ---
from sqlalchemy.orm import declarative_base # <-- 2. Importar desde .orm

# =================================================================
# Configuración (Leído desde variables de entorno)
# =================================================================

# ... (sin cambios aquí) ...
DB_URL = os.getenv("DB_URL", "sqlite:///./data/data.db")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_TO_CONSUME = os.getenv("TOPIC_RESULTS", "questions.validated")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("storage_service")

# =================================================================
# SECCIÓN 1: Configuración de la Base de Datos (SQLAlchemy)
# =================================================================

os.makedirs(os.path.dirname(DB_URL.split("///")[-1]), exist_ok=True)
engine = create_engine(DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- CORRECCIÓN DE ALCHEMY ---
Base = declarative_base() # <-- 3. Esta llamada ahora es correcta

class QuestionAnswer(Base):
    # ... (sin cambios en el modelo) ...
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
    # ... (sin cambios aquí) ...
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =================================================================
# SECCIÓN 2: Lógica del Consumidor de Kafka (en un Hilo)
# =================================================================

def consume_results_from_kafka():
    # ... (sin cambios en esta función) ...
    logger.info("Iniciando hilo consumidor de Kafka...")
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_TO_CONSUME,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                group_id='storage_consumer_group',
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            
            logger.info(f"Consumidor de Kafka conectado. Escuchando tópico: {TOPIC_TO_CONSUME}")
            
            for message in consumer:
                try:
                    data = message.value
                    logger.info(f"Mensaje recibido: {data.get('id')}")

                    if not all(k in data for k in ['question', 'answer', 'score']):
                        logger.warning(f"Mensaje incompleto, saltando: {data}")
                        continue

                    db = SessionLocal()
                    try:
                        existing_qa = db.query(QuestionAnswer).filter(QuestionAnswer.question == data['question']).first()
                        
                        if existing_qa:
                            existing_qa.answer = data['answer']
                            existing_qa.score = data['score']
                            logger.info(f"Actualizando pregunta existente: {data['question'][:20]}...")
                        else:
                            new_qa = QuestionAnswer(
                                question=data['question'],
                                answer=data['answer'],
                                score=data['score']
                            )
                            db.add(new_qa)
                            logger.info(f"Guardando nueva pregunta: {data['question'][:20]}...")
                        
                        db.commit()
                    except Exception as e:
                        logger.error(f"Error al procesar mensaje en BDD: {e}")
                        db.rollback()
                    finally:
                        db.close()
                
                except json.JSONDecodeError:
                    logger.error(f"Error al decodificar JSON del mensaje: {message.value}")
                except Exception as e:
                    logger.error(f"Error inesperado en el bucle del consumidor: {e}")

        except Exception as e:
            logger.error(f"Error al conectar consumidor de Kafka: {e}. Reintentando en 5 segundos...")
            time.sleep(5)

# =================================================================
# SECCIÓN 3: API Web (FastAPI) y Lifespan
# =================================================================

# --- CORRECCIÓN DE FASTAPI ---
# 4. Definir el 'lifespan'
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Iniciando lifespan (startup)...")
    
    # 1. Crea las tablas de la BDD
    create_db_and_tables()
    
    # 2. Inicia el consumidor de Kafka en un hilo
    consumer_thread = threading.Thread(target=consume_results_from_kafka, daemon=True)
    consumer_thread.start()
    
    # La aplicación se ejecuta
    yield
    
    # Código de 'shutdown' (opcional)
    logger.info("Cerrando lifespan (shutdown)...")

# 5. Pasar el lifespan a la app
app = FastAPI(title="Servicio de Almacenamiento (BDD)", lifespan=lifespan)

# --- FIN DE LA CORRECCIÓN ---

@app.get("/check")
async def check_question(question: str, db: Session = Depends(get_db)):
    """
    Endpoint para que el Generador de Tráfico verifique si una pregunta
    ya fue procesada y tiene un score.
    """
    try:
        # 1. Intenta la consulta
        result = db.query(QuestionAnswer).filter(QuestionAnswer.question == question).first()
        
        if result:
            # 2. ENCONTRADO (200 OK)
            if result.score is not None:
                return {
                    "processed": True,
                    "question": result.question,
                    "answer": result.answer,
                    "score": result.score
                }
            else:
                # Encontrada pero sin score (raro)
                # Le decimos al generador que no está procesada (devuelve 404)
                raise HTTPException(status_code=404, detail="Question found but score is missing.")
        
        else:
            # 3. NO ENCONTRADO (404 Not Found)
            # ¡Esto NO es un error! Es la respuesta esperada.
            # Simplemente levantamos la excepción 404 y dejamos que FastAPI la envíe.
            raise HTTPException(status_code=404, detail="Question not processed")

    except OperationalError as e:
        # 4. ERROR REAL (500 Internal Server)
        # Ej. si la base de datos está bloqueada o el archivo .db se corrompió.
        logger.error(f"Error de base de datos en /check: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    
    except HTTPException:
        # 5. Re-lanza las excepciones HTTPException (como la 404)
        raise
        
    except Exception as e:
        # 6. Otro error inesperado
        logger.error(f"Error inesperado en /check: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/qa")
async def get_all_questions(limit: int = 50, db: Session = Depends(get_db)):
    # ... (sin cambios en este endpoint) ...
    questions = db.query(QuestionAnswer).limit(limit).all()
    return questions

@app.get("/health")
async def health_check():
    # ... (sin cambios en este endpoint) ...
    return {"status": "healthy", "service": "storage"}

# --- SE ELIMINA LA SECCIÓN 4 (@app.on_event("startup")) ---