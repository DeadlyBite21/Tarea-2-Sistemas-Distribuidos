import os
import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("retry_quota")

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_INPUT = "llm.errors.quota"  
TOPIC_OUTPUT = "questions.llm" 
RETRY_DELAY_SECONDS = 60          

def create_kafka_client(client_type):
    """Crea un cliente Kafka (Productor o Consumidor) con reintentos."""
    while True:
        try:
            if client_type == "consumer":
                client = KafkaConsumer(
                    TOPIC_INPUT,
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    auto_offset_reset='earliest',
                    group_id='retry_quota_group',
                    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
                )
            elif client_type == "producer":
                client = KafkaProducer(
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
            logger.info(f"Kafka {client_type} conectado exitosamente.")
            return client
        except NoBrokersAvailable:
            logger.warning(f"No se pudo conectar a Kafka ({BOOTSTRAP_SERVERS}). Reintentando en 5 segundos...")
            time.sleep(5)

def main():
    consumer = create_kafka_client("consumer")
    producer = create_kafka_client("producer")

    logger.info(f"Escuchando en '{TOPIC_INPUT}' para errores de CUOTA...")

    for message in consumer:
        try:
            data = message.value
            msg_id = data.get('id', 'N/A')
            
            logger.warning(f"Error de CUOTA recibido (ID: {msg_id}). Esperando {RETRY_DELAY_SECONDS}s...")
            
            time.sleep(RETRY_DELAY_SECONDS)
            
            producer.send(TOPIC_OUTPUT, data)
            logger.info(f"Re-encolado (ID: {msg_id}) en '{TOPIC_OUTPUT}' post-cuota.")
            
        except Exception as e:
            logger.error(f"Error procesando mensaje en retry_quota: {e}")

if __name__ == "__main__":
    main()