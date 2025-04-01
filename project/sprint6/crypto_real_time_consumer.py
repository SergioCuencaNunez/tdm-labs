# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import json
import requests

KAFKA_BROKER = '192.168.80.34:9092'
KAFKA_TOPIC = 'gittba_SOL'

ELASTIC_URL = 'http://192.168.80.34:9200/gittba_sol/_doc/'

# Crea el KafkaConsumer
consumer = KafkaConsumer(
    bootstrap_servers=[KAFKA_BROKER],
    group_id='gittba_group_sol',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

# Asigna topic y partición
consumer.assign([TopicPartition(KAFKA_TOPIC, 0)])

# Lee los mensajes
records = consumer.poll(timeout_ms=5000)

# Procesa los mensajes
for topic_data, consumer_records in records.items():
    print("TopicPartition:", topic_data)
    for record in consumer_records:
        try:
            key = record.key
            value = record.value

            print(f"key: {key}")
            print(f"value: {value}")

            # Validación: solo si price y volume son numéricos
            if isinstance(value.get("price"), (int, float)) and isinstance(value.get("volume"), (int, float)):
                value["@timestamp"] = "2025-03-31T00:00:00Z"
                doc_id = str(record.offset)

                response = requests.put(f"{ELASTIC_URL}{doc_id}", json=value)
                print(f"Documento insertado (id={doc_id}): {response.status_code} - {response.text}")
            else:
                print("Mensaje descartado: price o volume no disponibles o no numéricos.")

        except Exception as e:
            print(f"Error procesando mensaje: {e}")

# Cierra el consumidor
consumer.close()
