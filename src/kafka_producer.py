import json
import logging
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import Config

logger = logging.getLogger(__name__)


class ReliableKafkaProducer:

    def __init__(self, max_retries=10, delay=3):
        retries = 0

        while retries < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    retries=5,
                    acks="all",
                    linger_ms=5,
                )
                logger.info("Connected to Kafka")
                return

            except Exception as e:
                retries += 1
                logger.warning(
                    f"Kafka connection failed (attempt {retries}/{max_retries}): {e}"
                )
                time.sleep(delay)

        raise Exception("Failed to connect to Kafka after retries")

    def send(self, key, value):
        try:
            future = self.producer.send(
                Config.KAFKA_TOPIC,
                key=str(key).encode("utf-8"),
                value=value,
            )
            record_metadata = future.get(timeout=10)
            logger.info(f"Event published: {value['event_id']}")

        except KafkaError as e:
            logger.error(f"Kafka send failed: {e}")

    def close(self):
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka producer closed")
