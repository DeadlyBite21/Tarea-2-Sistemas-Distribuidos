import os
import pandas as pd
from kafka import KafkaProducer
import json

def trim_and_load_data(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)
    # Trim to 12000 lines
    df_trimmed = df.head(12000)
    return df_trimmed

def send_to_kafka(data):
    kafka_server = os.environ.get('KAFKA_SERVER', 'localhost:9092')
    producer = KafkaProducer(bootstrap_servers=kafka_server,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for index, row in data.iterrows():
        producer.send('your_topic', row.to_dict())
    producer.flush()

if __name__ == "__main__":
    trimmed_data = trim_and_load_data('train.csv')
    send_to_kafka(trimmed_data)