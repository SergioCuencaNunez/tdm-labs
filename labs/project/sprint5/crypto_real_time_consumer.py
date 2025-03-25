# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# Crea el KafkaConsumer
consumer = KafkaConsumer(
    bootstrap_servers=['192.168.80.34:9092'],
    group_id='gittba_group_sol',
    auto_offset_reset='earliest'
)

# Asigna topic y partición
consumer.assign([TopicPartition('gittba_SOL', 0)])

# Lee los mensajes
records = consumer.poll(timeout_ms=5000)

# Procesa los mensajes
for topic_data, consumer_records in records.items():
    print("TopicPartition:", topic_data)
    for consumer_record in consumer_records:
        print("key:       " + str(consumer_record.key.decode('utf-8')))
        print("value:     " + str(consumer_record.value.decode('utf-8')))
        print("offset:    " + str(consumer_record.offset))
        print("timestamp: " + str(consumer_record.timestamp))

# Cierra el consumidor
consumer.close()
