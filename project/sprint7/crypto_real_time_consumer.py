# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import json
import requests
from datetime import datetime

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

while True:
    records = consumer.poll(timeout_ms=1000)
    for topic_data, consumer_records in records.items():
        for record in consumer_records:
            try:
                key = record.key
                value = record.value

                # Validación: solo si price y volume son numéricos
                if isinstance(value.get("price"), (int, float)) and isinstance(value.get("volume"), (int, float)):
                    # Inserta la fecha actual en formato ISO para @timestamp
                    value["@timestamp"] = datetime.utcnow().isoformat() + "Z"

                    # Inserta el documento en Elastic con ID = offset
                    doc_id = str(record.offset)
                    response = requests.put(f"{ELASTIC_URL}{doc_id}", json=value)

                    print(f"[{value['@timestamp']}] Insertado (id={doc_id}) → status {response.status_code}")
                else:
                    print("Mensaje descartado: price o volume inválidos")

            except Exception as e:
                print(f"Error procesando mensaje: {e}")
