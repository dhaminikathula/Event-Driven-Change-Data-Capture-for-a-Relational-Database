import json
from unittest.mock import MagicMock, patch
from src.kafka_producer import ReliableKafkaProducer


def test_kafka_producer_send_success():
    mock_producer = MagicMock()

    # Patch KafkaProducer inside our class
    with patch("src.kafka_producer.KafkaProducer") as MockKafka:
        instance = MockKafka.return_value
        instance.send.return_value.get.return_value = MagicMock()

        producer = ReliableKafkaProducer()

        test_event = {
            "event_id": "123",
            "event_version": "1.0",
            "operation_type": "INSERT"
        }

        producer.send(key=1, value=test_event)

        # Assert send was called
        instance.send.assert_called_once()

        # Validate topic argument
        args, kwargs = instance.send.call_args
        assert kwargs["value"] == test_event
