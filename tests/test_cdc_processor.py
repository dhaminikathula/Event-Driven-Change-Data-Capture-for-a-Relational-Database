from src.cdc_processor import build_event

def test_build_insert_event():
    record = {
        "id": 1,
        "name": "Test",
        "price": 100,
        "stock": 10,
        "last_updated": "2026-01-01T00:00:00"
    }

    event = build_event(
        record=record,
        table_name="products",
        operation_type="INSERT",
        old_data=None
    )

    assert event["operation_type"] == "INSERT"
    assert event["payload"]["new_data"]["name"] == "Test"
