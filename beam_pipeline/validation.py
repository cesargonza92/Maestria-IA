"""Validacion del contrato de evento (contracts/transaction_schema.json).

Un evento que no cumple el contrato -- JSON invalido, campo faltante, tipo
incorrecto, moneda no soportada, version de esquema desconocida, event_time
no parseable -- se marca como invalido junto con el motivo, para que el
pipeline lo enrute a `invalid.events` en lugar de detener todo el flujo.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import jsonschema

_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "contracts" / "transaction_schema.json"
_SCHEMA = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
_VALIDATOR = jsonschema.Draft7Validator(_SCHEMA)

EVENT_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def parse_event_time(value: str) -> datetime:
    """Parsea event_time (ISO 8601 UTC, sufijo Z) a un datetime aware en UTC."""
    return datetime.strptime(value, EVENT_TIME_FORMAT).replace(tzinfo=timezone.utc)


def validate(raw: bytes):
    """Valida un mensaje crudo de Kafka contra el contrato.

    Retorna (es_valido, evento_o_None, motivo_o_None).
    """
    try:
        event = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
        return False, None, f"json_invalido: {exc}"

    if not isinstance(event, dict):
        return False, None, "json_invalido: se esperaba un objeto"

    errors = sorted(_VALIDATOR.iter_errors(event), key=lambda e: list(e.path))
    if errors:
        reason = "; ".join(f"{list(e.path)}: {e.message}" for e in errors[:3])
        return False, event, f"contrato_invalido: {reason}"

    try:
        parse_event_time(event["event_time"])
    except (ValueError, KeyError) as exc:
        return False, event, f"event_time_invalido: {exc}"

    return True, event, None
