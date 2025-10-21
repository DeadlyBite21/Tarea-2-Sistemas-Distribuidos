import os
import json
import threading
import time
import logging
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, Column, String, Float, Integer, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from kafka import KafkaConsumer

# =================================================================
# Configuración (Leído desde variables de entorno)
# =================================================================

# URL de la BDD (SQLite persistido en un volumen)
DB_URL = os.getenv("DB_URL", "sqlite:///./data/data.db")

# Kafka
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_TO_CONSUME = os.getenv("TOPIC_RESULTS", "questions.validated") # Tópico donde Flink publica

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("storage_service")

# =================================================================
# SECCIÓN 1: Configuración de la Base de Datos (SQLAlchemy)
# =================================================================

# Asegura que el directorio para la DB exista
os.makedirs(os.path.dirname(DB_URL.split("///")[-1]), exist_ok=True)

# Configuración de SQLAlchemy
engine = create_engine(DB_URL, connect_args={"check_same_thread": False}) # check_same_thread es para SQLite
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Modelo de la tabla
class QuestionAnswer(Base):
    __tablename__ = "processed_questions"
    id = Column(Integer, primary_key=True, index=True)
    question = Column(String, unique=True, index=True)
    answer = Column(String)
    score = Column(Float, index=True)
    # Puedes añadir más campos como 'source_llm', 'timestamp', etc.

# Función para crear la tabla si no existe
def create_db_and_tables():
    logger.info("Verificando y creando tablas de la BDD...")
    Base.metadata.create_all(bind=engine)
    logger.info("Tablas de la BDD listas.")

# Dependencia de FastAPI para obtener una sesión de BDD
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =================================================================
# SECCIÓN 2: Lógica del Consumidor de Kafka (en un Hilo)
# =================================================================

def consume_results_from_kafka():
    """
    Esta función corre en un hilo separado, escuchando mensajes de Flink
    y guardándolos en la BDD.
    """
    logger.info("Iniciando hilo consumidor de Kafka...")
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_TO_CONSUME,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                group_id='storage_consumer_group', # ID de grupo para que Kafka recuerde dónde quedamos
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
                        # Revisa si la pregunta ya existe
                        existing_qa = db.query(QuestionAnswer).filter(QuestionAnswer.question == data['question']).first()
                        
                        if existing_qa:
                            # Si existe, actualiza la respuesta y el score
                            existing_qa.answer = data['answer']
                            existing_qa.score = data['score']
                            logger.info(f"Actualizando pregunta existente: {data['question'][:20]}...")
                        else:
                            # Si no existe, crea una nueva entrada
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
# SECCIÓN 3: API Web (FastAPI)
# =================================================================

app = FastAPI(title="Servicio de Almacenamiento (BDD)")

@app.get("/check")
async def check_question(question: str, db: Session = Depends(get_db)):
    """
    Endpoint para que el Generador de Tráfico verifique si una pregunta
    ya fue procesada y tiene un score. 
    """
    try:
        result = db.query(QuestionAnswer).filter(QuestionAnswer.question == question).first()
        
        if result:
            if result.score is not None:
                # ¡Encontrada y con score!
                return {
                    "processed": True,
                    "question": result.question,
                    "answer": result.answer,
                    "score": result.score
                }
            else:
                # Encontrada pero sin score (no debería pasar si Flink siempre lo añade)
                return {"processed": False, "message": "Question found but score is missing."}
        else:
            # No encontrada
            raise HTTPException(status_code=404, detail="Question not processed")
            
    except Exception as e:
        logger.error(f"Error en /check: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@app.get("/qa")
async def get_all_questions(limit: int = 50, db: Session = Depends(get_db)):
    """
    Endpoint (extra) para obtener preguntas y que el generador las use.
    """
    questions = db.query(QuestionAnswer).limit(limit).all()
    return questions

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "storage"}

# =================================================================
# SECCIÓN 4: Ciclo de Vida de la App (FastAPI)
# =================================================================

@app.on_event("startup")
def on_startup():
    # 1. Crea las tablas de la BDD al iniciar
    create_db_and_tables()
    
    # 2. Inicia el consumidor de Kafka en un hilo separado
    #    'daemon=True' asegura que el hilo muera cuando la app principal (FastAPI) muera.
    consumer_thread = threading.Thread(target=consume_results_from_kafka, daemon=True)
    consumer_thread.start()