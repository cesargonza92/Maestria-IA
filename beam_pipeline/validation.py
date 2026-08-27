"""Validacion del contrato de evento.

Un evento que no cumple el contrato -- JSON invalido, campo faltante, tipo
incorrecto, moneda no soportada, version de esquema desconocida, event_time
no parseable -- se marca como invalido junto con el motivo, para que el
pipeline lo enrute a `invalid.events` en lugar de detener todo el flujo.

El esquema esta embebido (en vez de leerse de contracts/transaction_schema.json
en disco) para que este modulo funcione igual sin importar donde se ejecute:
en el proceso local del DirectRunner, o en un worker remoto de Flink
(imagen apache/beam_python3.11_sdk, instalado via --setup_file), que no tiene
acceso al arbol de directorios del repositorio, solo a los modulos empaquetados.
contracts/transaction_schema.json sigue siendo la referencia legible para
la documentacion; este dict debe mantenerse identico a ese archivo.
"""

import json
from datetime import datetime, timezone

import jsonschema

_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "transaction_event",
    "description": "Contrato de evento de transaccion de pago sintetica. Version 1.0.",
    "type": "object",
    "required": [
        "schema_version",
        "event_id",
        "key",
        "event_time",
        "customer_id",
        "merchant_id",
        "amount",
        "currency",
        "country",
        "channel",
        "status",
    ],
    "properties": {
        "schema_version": {
            "type": "string",
            "description": "Version del contrato. Unica version soportada por el pipeline: 1.0.",
            "enum": ["1.0"],
        },
        "event_id": {
            "type": "string",
            "description": "Identificador unico y estable del evento, usado para deduplicacion.",
            "minLength": 1,
        },
        "key": {
            "type": "string",
            "description": "Identificador de tarjeta tokenizado. Tambien es la clave de particionamiento Kafka.",
            "minLength": 1,
        },
        "event_time": {
            "type": "string",
            "description": "Momento en que ocurrio la operacion, UTC, formato ISO 8601 (ej: 2026-08-22T14:30:00Z).",
            "format": "date-time",
        },
        "customer_id": {"type": "string", "minLength": 1},
        "merchant_id": {"type": "string", "minLength": 1},
        "amount": {
            "type": "integer",
            "description": "Monto en la unidad minima de la moneda (ej: PYG sin decimales). El tope "
            "superior es una validacion de sanidad basica (una transaccion real por encima de esto "
            "es indicio de dato corrupto o malicioso, no de una cifra bancaria real), no un limite "
            "de negocio.",
            "minimum": 0,
            "maximum": 1_000_000_000,
        },
        "currency": {
            "type": "string",
            "description": "Codigo ISO 4217. La primera version solo admite PYG.",
            "enum": ["PYG"],
        },
        "country": {
            "type": "string",
            "description": "Codigo de pais de dos letras (ISO 3166-1 alpha-2).",
            "pattern": "^[A-Z]{2}$",
        },
        "channel": {"type": "string", "enum": ["POS", "WEB", "ATM"]},
        "status": {
            "type": "string",
            "description": "Estado de la transaccion de entrada tal como la reporta la fuente.",
            "enum": ["APPROVED", "PENDING", "DECLINED"],
        },
    },
    "additionalProperties": True,
}
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
