import uuid
from datetime import datetime
from decimal import Decimal


def sanitize_record(record: dict) -> dict:
    """
    Convert non-JSON-serializable types (Decimal, datetime) 
    into JSON-safe formats.
    """
    sanitized = {}

    for key, value in record.items():
        if isinstance(value, Decimal):
            sanitized[key] = float(value)
        elif isinstance(value, datetime):
            sanitized[key] = value.isoformat()
        else:
            sanitized[key] = value

    return sanitized


def build_event(record, table_name, operation_type, old_data=None):
    sanitized_record = sanitize_record(record)
    sanitized_old = sanitize_record(old_data) if old_data else None

    event = {
        "event_id": str(uuid.uuid4()),
        "event_version": "1.0",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "mysql-cdc-service",
        "table_name": table_name,
        "operation_type": operation_type,
        "primary_keys": {"id": sanitized_record["id"]},
        "payload": {
            "old_data": sanitized_old,
            "new_data": sanitized_record if operation_type != "DELETE" else None
        }
    }

    return event
