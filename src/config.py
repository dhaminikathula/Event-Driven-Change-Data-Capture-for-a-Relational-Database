import os

class Config:
    DB_HOST = os.getenv("CDC_DB_HOST")
    DB_PORT = int(os.getenv("CDC_DB_PORT", 3306))
    DB_USER = os.getenv("CDC_DB_USER")
    DB_PASSWORD = os.getenv("CDC_DB_PASSWORD")
    DB_NAME = os.getenv("CDC_DB_NAME")
    TABLE_NAME = os.getenv("CDC_TABLE_NAME")

    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

    POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", 5))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))