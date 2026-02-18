import time
import logging
import signal
import sys

from db_client import MySQLClient
from kafka_producer import ReliableKafkaProducer
from cdc_processor import build_event
from state_manager import StateManager
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

db = MySQLClient()
producer = ReliableKafkaProducer()


def shutdown_handler(signum, frame):
    logger.info("Shutting down gracefully...")
    producer.close()
    db.close()
    sys.exit(0)


signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)


def run():
    db.connect()

    state = StateManager.load_state()
    last_updated = state.get("last_updated")
    last_id = state.get("last_id", 0)

    # In-memory snapshot for UPDATE detection
    previous_rows = {}

    while True:
        logger.info("Polling for changes...")

        records = db.fetch_changes(last_updated, last_id)

        if records:
            for record in records:
                record_id = record["id"]

                # Determine operation type
                if record.get("is_deleted"):
                    operation = "DELETE"
                    old_data = previous_rows.get(record_id)
                elif record_id not in previous_rows:
                    operation = "INSERT"
                    old_data = None
                else:
                    operation = "UPDATE"
                    old_data = previous_rows.get(record_id)

                event = build_event(
                    record=record,
                    table_name=Config.TABLE_NAME,
                    operation_type=operation,
                    old_data=old_data
                )

                producer.send(record_id, event)

                # Update snapshot
                previous_rows[record_id] = record

                # Advance watermark deterministically
                last_updated = record["last_updated"]
                last_id = record_id

            # Save state after processing batch
            StateManager.save_state(last_updated, last_id)

        time.sleep(Config.POLL_INTERVAL)


if __name__ == "__main__":
    run()
