import json

from validation import validate

VALID_EVENT = {
    "schema_version": "1.0",
    "event_id": "e-1",
    "key": "card-1",
    "event_time": "2026-08-22T14:00:00Z",
    "customer_id": "cust-1",
    "merchant_id": "merch-1",
    "amount": 10000,
    "currency": "PYG",
    "country": "PY",
    "channel": "POS",
    "status": "APPROVED",
}


def _raw(event: dict) -> bytes:
    return json.dumps(event).encode("utf-8")


def test_valid_event_passes():
    ok, event, reason = validate(_raw(VALID_EVENT))
    assert ok is True
    assert reason is None
    assert event["event_id"] == "e-1"


def test_invalid_json_is_rejected():
    ok, event, reason = validate(b"{no-es-json")
    assert ok is False
    assert event is None
    assert "json_invalido" in reason


def test_missing_required_field_is_rejected():
    event = dict(VALID_EVENT)
    del event["event_id"]
    ok, _, reason = validate(_raw(event))
    assert ok is False
    assert "contrato_invalido" in reason


def test_unsupported_currency_is_rejected():
    event = dict(VALID_EVENT, currency="USD")
    ok, _, reason = validate(_raw(event))
    assert ok is False
    assert "contrato_invalido" in reason


def test_negative_amount_is_rejected():
    event = dict(VALID_EVENT, amount=-500)
    ok, _, reason = validate(_raw(event))
    assert ok is False


def test_unsupported_schema_version_is_rejected():
    event = dict(VALID_EVENT, schema_version="2.0")
    ok, _, reason = validate(_raw(event))
    assert ok is False


def test_malformed_event_time_is_rejected():
    event = dict(VALID_EVENT, event_time="ayer a la tarde")
    ok, _, reason = validate(_raw(event))
    assert ok is False
    assert "invalido" in reason
