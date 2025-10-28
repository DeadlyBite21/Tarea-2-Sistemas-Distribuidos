import os
import re
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
# --- Importaciones ajustadas ---
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema, # Usado en Sinks
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
from pyflink.common import Row


# =================================================================
# Configuración (Sin cambios)
# =================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flink_job")

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
CONSUMER_GROUP_ID = "flink_scoring_group"

TOPIC_INPUT = os.getenv("TOPIC_INPUT", "questions.answers")
TOPIC_OUTPUT_VALIDATED = os.getenv("TOPIC_OUTPUT_VALIDATED", "questions.validated")
TOPIC_OUTPUT_REGENERATE = os.getenv("TOPIC_OUTPUT_REGENERATE", "questions.llm")

SCORE_THRESHOLD = float(os.getenv("SCORE_THRESHOLD", 0.62))
MAX_REGENERATIONS = int(os.getenv("MAX_REGENERATIONS", 3))

logger.info(f"Iniciando Flink Job...")
logger.info(f"  Consumiendo de: {TOPIC_INPUT}")
logger.info(f"  Publicando (OK) en: {TOPIC_OUTPUT_VALIDATED}")
logger.info(f"  Publicando (Regen) en: {TOPIC_OUTPUT_REGENERATE}")
logger.info(f"  Umbral de Score: {SCORE_THRESHOLD}")
logger.info(f"  Máx. Regeneraciones: {MAX_REGENERATIONS}")

# =================================================================
# Lógica de Negocio (calculate_score, normalize_text, etc. - Sin cambios)
# =================================================================

# ... (Pega aquí tus funciones calculate_score, normalize_text, STOP_WORDS_EN, BAD_PHRASES_EN) ...


# =================================================================
# Definición de Clases de Mapeo (Operan sobre dicts parseados)
# =================================================================

class Scorer(object):
    """
    Parsea el JSON string, aplica el score y devuelve el dict actualizado.
    """
    def __call__(self, json_string: str) -> dict:
        try:
            message = json.loads(json_string)
            # Pasamos solo lo necesario a calculate_score
            score_input = {
                "id": message.get("id"),
                "question": message.get("question"),
                "answer": message.get("answer"),
                # Pasar original_message si tu score lo necesita
                "original_message": message.get("original_message") 
            }
            score = calculate_score(score_input)
            message['score'] = score # Añade el score al diccionario completo
            logger.info(f"Scored (ID: {message.get('id', 'N/A')}, Score: {score:.2f})")
            return message
        except Exception as e:
            logger.error(f"Error parseando o puntuando JSON: {e} - String: {json_string[:100]}")
            # Devuelve un dict vacío o con error para filtrarlo después si es necesario
            return {"error": str(e), "original_string": json_string} 


class PrepareForPersistence(object):
    """
    Formatea el dict para guardarlo en la BDD y lo serializa a JSON string.
    """
    def __call__(self, message: dict) -> str:
        # Filtra mensajes de error del paso anterior
        if "error" in message:
            return json.dumps({}) # Devuelve JSON vacío para ignorar

        output = {
            "id": message.get("id"),
            "question": message.get("question"),
            "answer": message.get("answer"),
            "score": message.get("score")
        }
        return json.dumps(output, ensure_ascii=False)

class PrepareForRegeneration(object):
    """
    Formatea el dict para enviarlo de vuelta al LLM y lo serializa a JSON string.
    """
    def __call__(self, message: dict) -> str:
        # Filtra mensajes de error
        if "error" in message:
             return json.dumps({}) # Ignorar

        original_msg = message.get("original_message", {})
        
        regens = original_msg.get("regens", 0) + 1
        original_msg["regens"] = regens
        original_msg["last_score"] = message.get("score")
        
        logger.warning(f"Regenerando (ID: {message.get('id')}, Score: {message.get('score'):.2f}, Intento: {regens})")
        return json.dumps(original_msg, ensure_ascii=False)

# =================================================================
# Job Principal de PyFlink (Simplificado)
# =================================================================

def run_flink_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # --- CAMBIO 1: Source lee JSON como String ---
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_topics(TOPIC_INPUT) \
        .set_group_id(CONSUMER_GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # --- CAMBIO 2: Watermark simple ---
    wm_strategy = WatermarkStrategy.no_watermarks()

    # --- CAMBIO 3: Stream inicial es de Strings ---
    stream = env.from_source(kafka_source, wm_strategy, "Kafka Source")

    # --- CAMBIO 4: Parsear y puntuar ---
    # El output_type ahora es un diccionario genérico
    scored_stream = stream.map(Scorer(), output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY()))

    # 5. Dividir el Stream (Routing - Opera sobre dicts)
    high_score_stream = scored_stream.filter(lambda msg: "error" not in msg and msg.get("score", 0.0) >= SCORE_THRESHOLD)
    low_score_stream = scored_stream.filter(
        lambda msg: "error" not in msg and \
                    msg.get("score", 0.0) < SCORE_THRESHOLD and \
                    msg.get("original_message", {}).get("regens", 0) < MAX_REGENERATIONS
    )

    # 6. Formatear los streams de salida (Devuelven JSON Strings)
    persistence_stream = high_score_stream.map(PrepareForPersistence(), output_type=Types.STRING())
    feedback_stream = low_score_stream.map(PrepareForRegeneration(), output_type=Types.STRING())

    # 7. Definir los Sinks (Usan SimpleStringSchema)
    sink_persist = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(TOPIC_OUTPUT_VALIDATED)
                .set_value_serialization_schema(SimpleStringSchema()) # Envía string
                .build()
        ) \
        .build()
        
    sink_feedback = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(TOPIC_OUTPUT_REGENERATE)
                .set_value_serialization_schema(SimpleStringSchema()) # Envía string
                .build()
        ) \
        .build()

    # 8. Conectar streams a sinks
    # Filtramos los JSON vacíos resultantes de errores de parseo/score
    persistence_stream.filter(lambda s: s != '{}').sink_to(sink_persist).name("Sink to BDD")
    feedback_stream.filter(lambda s: s != '{}').sink_to(sink_feedback).name("Sink to Regenerate")

    # 9. Ejecutar
    env.execute("Flink Scoring and Feedback Job")

if __name__ == "__main__":
    run_flink_job()