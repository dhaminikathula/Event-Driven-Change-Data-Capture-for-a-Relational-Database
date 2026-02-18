import json
import time
import logging
from kafka import KafkaProducer
from config import Config

logger = logging.getLogger(__name__)

class ReliableKafkaProducer:

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            retries=0
        )

    def send(self, key, value):
        retry_delay = 1

        for attempt in range(Config.MAX_RETRIES):
            try:
                future = self.producer.send(
                    Config.KAFKA_TOPIC,
                    key=key,
                    value=value
                )
                future.get(timeout=10)
                logger.info(f"Event published: {value['event_id']}")
                return

            except Exception as e:
                logger.warning(f"Retry {attempt+1} failed: {e}")
                time.sleep(retry_delay)
                retry_delay *= 2

        logger.error("Max retries exceeded")
        raise Exception("Kafka publish failed")

    def close(self):
        self.producer.flush()
        self.producer.close()