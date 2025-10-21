import os
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types

# =================================================================
# Configuración (Leído desde variables de entorno)
# =================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flink_job")

# --- Kafka ---
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
CONSUMER_GROUP_ID = "flink_scoring_group"

# --- Tópicos ---
# 1. Tópico de ENTRADA (desde el LLM)
TOPIC_INPUT = os.getenv("TOPIC_INPUT", "questions.answers")
# 2. Tópico de SALIDA (para BDD)
TOPIC_OUTPUT_VALIDATED = os.getenv("TOPIC_OUTPUT_VALIDATED", "questions.validated")
# 3. Tópico de FEEDBACK (para Regeneración)
TOPIC_OUTPUT_REGENERATE = os.getenv("TOPIC_OUTPUT_REGENERATE", "questions.llm")

# --- Lógica de la Tarea ---
# [cite: 41, 77]
SCORE_THRESHOLD = float(os.getenv("SCORE_THRESHOLD", 0.62))
# 
MAX_REGENERATIONS = int(os.getenv("MAX_REGENERATIONS", 3))

logger.info(f"Iniciando Flink Job...")
logger.info(f"  Consumiendo de: {TOPIC_INPUT}")
logger.info(f"  Publicando (OK) en: {TOPIC_OUTPUT_VALIDATED}")
logger.info(f"  Publicando (Regen) en: {TOPIC_OUTPUT_REGENERATE}")
logger.info(f"  Umbral de Score: {SCORE_THRESHOLD}")
logger.info(f"  Máx. Regeneraciones: {MAX_REGENERATIONS}")

# =================================================================
# Lógica de Negocio (¡AQUÍ PONES TU SCORE!)
# =================================================================

def calculate_score(message: dict) -> float:
    """
    
    AQUÍ DEBES IMPLEMENTAR LA LÓGICA DE SCORE DE TU TAREA 1.
    
    Esta función recibe el mensaje completo desde el LLM
    (que contiene 'question' y 'answer') y debe devolver un score.
    
    Como placeholder, usamos una lógica simple. 
    ¡DEBES REEMPLAZAR ESTO!
    """
    question = message.get("question", "")
    answer = message.get("answer", "")
    
    if not question or not answer:
        return 0.0
    
    # --- INICIO PLACEHOLDER ---
    # Ejemplo: score basado en la longitud de la respuesta
    score = min(len(answer) / 200.0, 1.0)
    # --- FIN PLACEHOLDER ---
    
    return float(score)

# =================================================================
# Definición de Clases de Mapeo (Transformaciones)
# =================================================================

class Scorer(object):
    """
    Clase que aplica el score al mensaje.
    """
    def __call__(self, message: dict) -> dict:
        # 1. Calcula el score
        score = calculate_score(message)
        
        # 2. Añade el score al mensaje
        message['score'] = score
        
        # 3. Log
        logger.info(f"Scored (ID: {message.get('id', 'N/A')}, Score: {score:.2f})")
        return message

class PrepareForPersistence(object):
    """
    Formatea el mensaje para guardarlo en la BDD.
    El servicio BDD espera: {'question', 'answer', 'score'}
    """
    def __call__(self, message: dict) -> str:
        output = {
            "id": message.get("id"),
            "question": message.get("question"),
            "answer": message.get("answer"),
            "score": message.get("score")
        }
        return json.dumps(output) # Serializa a string JSON

class PrepareForRegeneration(object):
    """
    Formatea el mensaje para enviarlo de vuelta al LLM.
    Incrementa el contador de regeneración para evitar bucles. 
    """
    def __call__(self, message: dict) -> str:
        # El mensaje original del generador está anidado
        original_msg = message.get("original_message", {})
        
        # Incrementa el contador
        regens = original_msg.get("regens", 0) + 1
        original_msg["regens"] = regens
        
        # Añade info de contexto
        original_msg["last_score"] = message.get("score")
        
        logger.warning(f"Regenerando (ID: {message.get('id')}, Score: {message.get('score'):.2f}, Intento: {regens})")
        return json.dumps(original_msg) # Serializa a string JSON

# =================================================================
# Job Principal de PyFlink
# =================================================================

def run_flink_job():
    # 1. Configurar el entorno de Flink
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # ¡Importante! Debes decirle a Flink dónde encontrar el JAR del conector de Kafka
    # Tu docker-compose se encargará de esto si usas la imagen oficial de Flink.
    # Si ejecutas local, necesitas descargar el JAR: 'flink-sql-connector-kafka-X.X.X.jar'
    # y ponerlo en la carpeta /opt/flink/lib (en el Docker)
    
    # 2. Definir el ESQUEMA del Tópico de Entrada (desde el LLM)
    # Basado en tu llm/app.py corregido
    input_type_info = Types.ROW_NAMED(
        ["id", "question", "answer", "timestamp", "original_message"],
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.PICKLED_BYTE_ARRAY()] 
        # Usamos PICKLED_BYTE_ARRAY para el JSON anidado 'original_message'
        # ¡Corrección! JsonRowDeserializationSchema no maneja bien JSON anidado.
        # Es MUCHO más simple tratar todo el mensaje como un STRING y parsearlo
        # nosotros mismos.
    )
    
    # 2 (Corregido). Definir el Origen (Source)
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_topics(TOPIC_INPUT) \
        .set_group_id(CONSUMER_GROUP_ID) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(kafka_source, "Kafka Source")

    # 3. Aplicar Transformaciones
    
    # Paso 3a: Parsear el JSON string a un dict
    parsed_stream = stream.map(lambda s: json.loads(s), output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # Paso 3b: Aplicar el Score 
    scored_stream = parsed_stream.map(Scorer(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # 4. Dividir el Stream (Routing) [cite: 41, 42]
    
    # 4a. Stream de ALTA CALIDAD (para BDD)
    high_score_stream = scored_stream.filter(lambda msg: msg.get("score", 0.0) >= SCORE_THRESHOLD)
    
    # 4b. Stream de BAJA CALIDAD (para Regeneración)
    #  Se añade el chequeo de MAX_REGENERATIONS
    low_score_stream = scored_stream.filter(
        lambda msg: msg.get("score", 0.0) < SCORE_THRESHOLD and \
                    msg.get("original_message", {}).get("regens", 0) < MAX_REGENERATIONS
    )

    # 5. Formatear los streams de salida
    
    # 5a. Formatear para BDD
    persistence_stream = high_score_stream.map(PrepareForPersistence(), output_type=Types.STRING())
    
    # 5b. Formatear para Regeneración
    feedback_stream = low_score_stream.map(PrepareForRegeneration(), output_type=Types.STRING())

    # 6. Definir los Destinos (Sinks)
    
    # 6a. Sink para BDD
    sink_persist = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaSink.builder.RecordSerializer.builder()
                .set_topic(TOPIC_OUTPUT_VALIDATED)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
        
    # 6b. Sink para Regeneración
    sink_feedback = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaSink.builder.RecordSerializer.builder()
                .set_topic(TOPIC_OUTPUT_REGENERATE)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    # 7. Conectar los streams a los sinks
    persistence_stream.sink_to(sink_persist).name("Sink to BDD")
    feedback_stream.sink_to(sink_feedback).name("Sink to Regenerate")

    # 8. Ejecutar el Job
    env.execute("Flink Scoring and Feedback Job")

if __name__ == "__main__":
    run_flink_job()