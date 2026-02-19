import os
import json
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = os.getenv("KAFKA_TOPIC")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='cdc-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer started...")

for message in consumer:
    print("Received Event:")
    print(json.dumps(message.value, indent=2))