"""Generadores de escenarios para el productor sintetico.

Cada perfil produce una secuencia de "acciones" de envio. Una accion es una
tupla (delay_seconds, event_dict): delay_seconds es cuanto debe esperar el
productor antes de publicar ese evento (permite simular ritmo real), y
event_dict ya tiene event_time asignado segun el escenario que se quiere
demostrar (a tiempo, duplicado, tardio dentro/fuera del lateness permitido).

Todo es determinista a partir de un seed: mismo seed + mismo perfil producen
la misma secuencia de eventos, para que un escenario se pueda repetir en la
demostracion y en el smoke test end-to-end.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

SCHEMA_VERSION = "1.0"
CURRENCY = "PYG"
COUNTRIES = ["PY", "AR", "BR", "UY", "CL"]
CHANNELS = ["POS", "WEB", "ATM"]
STATUSES = ["APPROVED", "APPROVED", "APPROVED", "PENDING", "DECLINED"]

# Horizonte de "tardio permitido" que declara el pipeline (allowed_lateness).
# Se usa aca solo para poder generar deliberadamente un evento *dentro* y otro
# *fuera* de ese horizonte y as{i} demostrar ambos comportamientos.
ALLOWED_LATENESS_SECONDS = 120


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _base_event(rng: random.Random, card_id: str, event_time: datetime, **overrides) -> dict:
    event = {
        "schema_version": SCHEMA_VERSION,
        "event_id": str(uuid.uuid4()),
        "key": card_id,
        "event_time": _iso(event_time),
        "customer_id": f"cust-{rng.randint(1, 200):04d}",
        "merchant_id": f"merch-{rng.randint(1, 500):04d}",
        "amount": rng.randint(20_000, 900_000),
        "currency": CURRENCY,
        "country": rng.choice(COUNTRIES),
        "channel": rng.choice(CHANNELS),
        "status": rng.choice(STATUSES),
    }
    event.update(overrides)
    return event


def _cards(rng: random.Random, n: int) -> list:
    return [f"card-{rng.randint(100000, 999999)}" for _ in range(n)]


def normal(rng: random.Random, count: int, start: datetime):
    """Trafico normal: eventos en orden, sin duplicados, sin retraso."""
    cards = _cards(rng, max(3, count // 8))
    t = start
    for _ in range(count):
        t = t + timedelta(seconds=rng.uniform(0.2, 1.5))
        card = rng.choice(cards)
        yield 0.05, _base_event(rng, card, t)


def duplicates(rng: random.Random, count: int, start: datetime):
    """Cada evento tiene una probabilidad de ser reenviado (mismo event_id)
    poco despues, simulando un reintento de red o del propio productor."""
    cards = _cards(rng, max(3, count // 8))
    t = start
    for _ in range(count):
        t = t + timedelta(seconds=rng.uniform(0.2, 1.5))
        card = rng.choice(cards)
        event = _base_event(rng, card, t)
        yield 0.05, event
        if rng.random() < 0.35:
            # Reenvio del mismo event_id: debe ser ignorado por el dedup.
            yield rng.uniform(0.1, 2.0), dict(event)


def late(rng: random.Random, count: int, start: datetime):
    """Genera eventos que llegan fuera de orden: una parte con retraso dentro
    del allowed_lateness (deben actualizar la ventana ya emitida) y otra parte
    con retraso mayor al permitido (deben quedar registrados como descartados).
    """
    cards = _cards(rng, max(3, count // 8))
    t = start
    for _ in range(count):
        t = t + timedelta(seconds=rng.uniform(0.3, 1.2))
        card = rng.choice(cards)
        roll = rng.random()
        if roll < 0.15:
            # Tardio pero dentro del horizonte permitido (60-100s de atraso).
            event_time = t - timedelta(seconds=rng.uniform(60, 100))
            event = _base_event(rng, card, event_time)
        elif roll < 0.25:
            # Tardio por encima del horizonte permitido (debe descartarse).
            event_time = t - timedelta(seconds=rng.uniform(150, 400))
            event = _base_event(rng, card, event_time)
        else:
            event = _base_event(rng, card, t)
        yield 0.05, event


def alerting(rng: random.Random, count: int, start: datetime):
    """Concentra actividad sobre pocas tarjetas para disparar deliberadamente
    las 4 reglas (frecuencia, monto, multi-pais, multi-comercio) y poder
    mostrar alertas reales en la demo, ademas del trafico normal."""
    hot_cards = _cards(rng, 2)
    t = start
    for _ in range(count):
        t = t + timedelta(seconds=rng.uniform(1, 4))
        card = rng.choice(hot_cards)
        event = _base_event(
            rng,
            card,
            t,
            amount=rng.randint(1_000_000, 3_000_000),
            country=rng.choice(COUNTRIES),
            merchant_id=f"merch-{rng.randint(1, 500):04d}",
        )
        yield 0.2, event


PROFILES = {
    "normal": normal,
    "duplicates": duplicates,
    "late": late,
    "alerting": alerting,
}
