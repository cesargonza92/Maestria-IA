"""Agregacion incremental por tarjeta+ventana y evaluacion de reglas.

Los umbrales son parametros del prototipo (seccion 6 de la propuesta) y no
representan reglas bancarias reales: sirven para demostrar de forma clara
los conceptos de agregacion incremental (CombineFn), ventanas y alertas.
"""

from datetime import datetime, timezone

import apache_beam as beam

RULE_HIGH_FREQUENCY = "HIGH_FREQUENCY"
RULE_HIGH_AMOUNT = "HIGH_AMOUNT"
RULE_MULTI_COUNTRY = "MULTI_COUNTRY"
RULE_MULTI_MERCHANT = "MULTI_MERCHANT"

MIN_TRANSACTIONS_HIGH_FREQUENCY = 6
MIN_AMOUNT_SUM_HIGH_AMOUNT = 5_000_000
MIN_COUNTRIES_MULTI_COUNTRY = 2
MIN_MERCHANTS_MULTI_MERCHANT = 4


class CardWindowStatsFn(beam.CombineFn):
    """CombineFn incremental: no materializa la lista de eventos de la
    ventana, solo mantiene contadores y conjuntos acotados por tarjeta."""

    def create_accumulator(self):
        return {"count": 0, "amount_sum": 0, "countries": set(), "merchants": set()}

    def add_input(self, accumulator, event):
        accumulator["count"] += 1
        accumulator["amount_sum"] += event["amount"]
        accumulator["countries"].add(event["country"])
        accumulator["merchants"].add(event["merchant_id"])
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["count"] += acc["count"]
            merged["amount_sum"] += acc["amount_sum"]
            merged["countries"] |= acc["countries"]
            merged["merchants"] |= acc["merchants"]
        return merged

    def extract_output(self, accumulator):
        return accumulator


def evaluate_rules(stats: dict) -> list:
    """Devuelve la lista de alert_type disparados por estas estadisticas.
    Una tarjeta puede disparar mas de una regla en la misma ventana."""
    triggered = []
    if stats["count"] >= MIN_TRANSACTIONS_HIGH_FREQUENCY:
        triggered.append(RULE_HIGH_FREQUENCY)
    if stats["amount_sum"] > MIN_AMOUNT_SUM_HIGH_AMOUNT:
        triggered.append(RULE_HIGH_AMOUNT)
    if len(stats["countries"]) >= MIN_COUNTRIES_MULTI_COUNTRY:
        triggered.append(RULE_MULTI_COUNTRY)
    if len(stats["merchants"]) >= MIN_MERCHANTS_MULTI_MERCHANT:
        triggered.append(RULE_MULTI_MERCHANT)
    return triggered


def _iso(dt) -> str:
    return dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if dt.tzinfo is None else dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_alert_id(card: str, window_start_iso: str, window_end_iso: str, alert_type: str) -> str:
    """Clave estable de salida: permite que un reintento o un pane posterior
    de la misma ventana actualice (upsert) la misma alerta en vez de crear
    una nueva -- ver seccion 8 de la propuesta."""
    return f"{card}|{window_start_iso}|{window_end_iso}|{alert_type}"


class EvaluateRulesFn(beam.DoFn):
    """Evalua las reglas sobre el resultado combinado de la ventana y emite
    0..N alertas. La clave de salida (alert_id) es estable frente a
    reintentos o panes posteriores de la misma ventana: permite upsert."""

    def process(self, element, window=beam.DoFn.WindowParam):
        card, stats = element
        window_start = _iso(window.start.to_utc_datetime())
        window_end = _iso(window.end.to_utc_datetime())
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        for alert_type in evaluate_rules(stats):
            alert_id = build_alert_id(card, window_start, window_end, alert_type)
            yield {
                "alert_id": alert_id,
                "key": card,
                "window_start": window_start,
                "window_end": window_end,
                "alert_type": alert_type,
                "transaction_count": stats["count"],
                "amount_sum": stats["amount_sum"],
                "countries": sorted(stats["countries"]),
                "merchant_count": len(stats["merchants"]),
                "generated_at": generated_at,
            }
