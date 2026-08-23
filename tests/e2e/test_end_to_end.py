"""Smoke test end-to-end: productor -> Kafka -> Beam -> Kafka -> consumidor.

Requiere el entorno real levantado (`scripts/start.sh` o `scripts/start.ps1`,
o al menos `docker compose up -d kafka kafka-init beam_pipeline`) escuchando
en localhost:29092. Si Kafka no esta disponible, el test se salta en lugar de
fallar, para no romper `pytest tests/unit tests/streaming` (esas si corren
siempre, sin dependencias externas).

Publica un escenario minimo con evento normal, un duplicado exacto (mismo
event_id), un evento tardio dentro de la politica y un evento invalido, y
verifica el recorrido completo observando fraud.alerts e invalid.events.
"""

import json
import socket
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = "localhost:29092"
RAW_TOPIC = "transactions.raw"
ALERTS_TOPIC = "fraud.alerts"
INVALID_TOPIC = "invalid.events"
POLL_TIMEOUT_SECONDS = 90


def _kafka_is_reachable() -> bool:
    host, port = BOOTSTRAP_SERVERS.split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=2):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _kafka_is_reachable(),
    reason=f"Kafka no disponible en {BOOTSTRAP_SERVERS}; levantar el entorno con scripts/start.* antes de correr el smoke test e2e.",
)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _event(card: str, event_time: datetime, **overrides) -> dict:
    event = {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "key": card,
        "event_time": _iso(event_time),
        "customer_id": "cust-e2e",
        "merchant_id": "merch-e2e",
        "amount": 10_000,
        "currency": "PYG",
        "country": "PY",
        "channel": "POS",
        "status": "APPROVED",
    }
    event.update(overrides)
    return event


def test_normal_duplicate_late_and_invalid_events_flow_end_to_end():
    card = f"card-e2e-{uuid.uuid4().hex[:8]}"
    start = datetime.now(timezone.utc)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    # 6 eventos normales -> deberian disparar HIGH_FREQUENCY para esta tarjeta.
    normal_events = [_event(card, start + timedelta(seconds=i)) for i in range(6)]
    for event in normal_events:
        producer.send(RAW_TOPIC, key=card, value=event)

    # Duplicado exacto (mismo event_id) del primer evento: no debe sumar al conteo.
    producer.send(RAW_TOPIC, key=card, value=dict(normal_events[0]))

    # Tardio pero dentro de la politica (unos segundos de atraso respecto del ultimo event_time).
    late_event = _event(card, start + timedelta(seconds=3))
    producer.send(RAW_TOPIC, key=card, value=late_event)

    # Invalido: falta event_id y el monto es negativo.
    invalid_payload = {
        "schema_version": "1.0",
        "key": card,
        "event_time": _iso(start),
        "customer_id": "cust-e2e",
        "merchant_id": "merch-e2e",
        "amount": -1,
        "currency": "PYG",
        "country": "PY",
        "channel": "POS",
        "status": "APPROVED",
    }
    producer.send(RAW_TOPIC, key=card, value=invalid_payload)
    producer.flush()

    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        INVALID_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id=f"e2e-test-{uuid.uuid4().hex[:8]}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,
    )

    alerts_by_id = {}
    invalid_seen = []
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    while time.time() < deadline and not (alerts_by_id and invalid_seen):
        for message in consumer:
            if message.topic == ALERTS_TOPIC:
                alert = message.value
                if alert.get("key") == card:
                    alerts_by_id[alert["alert_id"]] = alert
            elif message.topic == INVALID_TOPIC:
                invalid_seen.append(message.value)

    assert alerts_by_id, "no se recibio ninguna alerta para la tarjeta de prueba dentro del timeout"
    card_alerts = [a for a in alerts_by_id.values() if a["alert_type"] == "HIGH_FREQUENCY"]
    assert card_alerts, "se esperaba al menos una alerta HIGH_FREQUENCY"
    # El duplicado no debe haberse contado: como maximo 7 eventos unicos
    # (6 normales + 1 tardio dentro de la politica) entraron a la ventana.
    assert all(a["transaction_count"] <= 7 for a in card_alerts)
    assert invalid_seen, "se esperaba al menos un evento en invalid.events"
