import os
import re
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

# Lista de "stop words" SOLO en inglés (puedes expandirla)
STOP_WORDS_EN = set([
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", 
    "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", 
    "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", 
    "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", 
    "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", 
    "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", 
    "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", 
    "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", 
    "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", 
    "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", 
    "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", 
    "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now", "?"
])

# Frases genéricas/malas SOLO en inglés
BAD_PHRASES_EN = [
    "i don't know", "search google", "good question", "try this link", 
    "check this out", "i guess", "maybe", "sorry"
]

def normalize_text(text):
    """Limpia el texto: minúsculas, sin puntuación extraña."""
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", "", text) # Quita puntuación
    text = re.sub(r"\s+", " ", text).strip() # Normaliza espacios
    return text

def calculate_score(message: dict) -> float:
    """
    Calcula un score heurístico para la calidad de la respuesta (EN INGLÉS).
    Combina longitud, coincidencia de palabras clave, formato y frases malas.
    Devuelve un float entre 0.0 y 1.0.
    """
    question = message.get("question", "")
    answer = message.get("answer", "")
    
    # Scores iniciales
    score_length = 0.0
    score_keywords = 0.0
    score_format = 0.0
    score_bad_phrases = 1.0 

    # --- 1. Score de Longitud ---
    len_ans = len(answer)
    if 50 <= len_ans <= 600: score_length = 1.0
    elif 20 <= len_ans < 50: score_length = 0.5
    elif len_ans > 600: score_length = 0.3
    else: score_length = 0.1 

    # --- 2. Score de Palabras Clave ---
    if question and answer:
        clean_q = normalize_text(question)
        clean_a = normalize_text(answer)
        
        q_words = set(clean_q.split()) - STOP_WORDS_EN # Usar stop words en inglés
        a_words = set(clean_a.split())
        
        if q_words: 
            matched_keywords = q_words.intersection(a_words)
            score_keywords = len(matched_keywords) / len(q_words)
            if score_keywords > 0: score_keywords = min(score_keywords + 0.2, 1.0) 
        else:
            score_keywords = 0.1 

    # --- 3. Score de Formato Básico ---
    if answer:
        if answer[0].isupper() and answer[-1] in ".!?": score_format = 1.0
        elif answer[0].isupper() or answer[-1] in ".!?": score_format = 0.5
        else: score_format = 0.1

    # --- 4. Penalización por Frases Malas ---
    clean_a_lower = answer.lower() 
    for phrase in BAD_PHRASES_EN: # Usar frases malas en inglés
        if phrase in clean_a_lower:
            score_bad_phrases = 0.0 
            logger.warning(f"Respuesta (ID: {message.get('id', 'N/A')}) contiene frase mala: '{phrase}'")
            break

    # --- 5. Combinación Ponderada ---
    weight_length = 0.3
    weight_keywords = 0.4
    weight_format = 0.3

    final_score = (weight_length * score_length +
                   weight_keywords * score_keywords +
                   weight_format * score_format)
    
    final_score *= score_bad_phrases
    final_score = max(0.0, min(final_score, 1.0))
    
    return float(final_score)

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