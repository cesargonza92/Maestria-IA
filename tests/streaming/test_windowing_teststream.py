"""Pruebas de tiempo de evento, ventanas y politica de datos tardios, con
TestStream sobre DirectRunner.

Ejercitan la MISMA configuracion que produccion (`pipeline.window_transform`
y `pipeline.TooLateGateFn`): FixedWindows de 60s, trigger de watermark con
disparo tardio simple, modo ACCUMULATING, allowed_lateness=120s, y el gate
explicito de tardanza. No pasan por Kafka ni por la validacion (esas capas
ya se prueban en tests/unit): el foco aca es exclusivamente la semantica
temporal.

Nota: se verifico empiricamente que el DirectRunner de Beam Python de esta
version no dispara un pane por cada avance de watermark dentro de una sola
ejecucion de TestStream, sino que materializa el resultado final al drenar
el pipeline. Por eso las pruebas comparan el/los resultado(s) finales (lo
que efectivamente veria un consumidor que materializa por alert_id) en
lugar de contar paneles intermedios -- ver docs/documento_tecnico.md,
seccion de limites conocidos.
"""

import json

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from pipeline import TooLateGateFn, window_transform
from rules import CardWindowStatsFn, EvaluateRulesFn

CARD = "card-1"
WINDOW_START = "1970-01-01T00:00:00Z"
WINDOW_END = "1970-01-01T00:01:00Z"


def _event(amount=1000, country="PY", merchant_id="m1"):
    return {"key": CARD, "amount": amount, "country": country, "merchant_id": merchant_id}


def _strip_generated_at(alert: dict) -> dict:
    return {k: v for k, v in alert.items() if k != "generated_at"}


def _expected_pane(transaction_count, amount_sum):
    return {
        "alert_id": f"{CARD}|{WINDOW_START}|{WINDOW_END}|HIGH_FREQUENCY",
        "key": CARD,
        "window_start": WINDOW_START,
        "window_end": WINDOW_END,
        "alert_type": "HIGH_FREQUENCY",
        "transaction_count": transaction_count,
        "amount_sum": amount_sum,
        "countries": ["PY"],
        "merchant_count": 1,
    }


def test_on_time_window_triggers_a_single_alert():
    """6 eventos en la misma ventana de 60s disparan HIGH_FREQUENCY (umbral=6)."""
    test_stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(_event(), t) for t in (0, 5, 10, 15, 20, 25)])
        .advance_watermark_to_infinity()
    )

    with TestPipeline() as p:
        alerts = (
            p
            | test_stream
            | "KeyByCard" >> beam.Map(lambda e: (e["key"], e))
            | "Window" >> window_transform()
            | "AggregateByCard" >> beam.CombinePerKey(CardWindowStatsFn())
            | "EvaluateRules" >> beam.ParDo(EvaluateRulesFn())
            | "StripGeneratedAt" >> beam.Map(_strip_generated_at)
        )
        assert_that(alerts, equal_to([_expected_pane(6, 6000)]))


def test_late_event_within_policy_is_included_in_the_final_alert():
    """Un evento fuera de orden, cuyo atraso respecto del ultimo event_time
    visto para la tarjeta esta dentro de la politica (ventana+lateness),
    pasa el gate y se refleja en el resultado final (count=7, no 6)."""
    test_stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(_event(), t) for t in (0, 5, 10, 15, 20, 30)])
        .add_elements([TimestampedValue(_event(), 25)])  # fuera de orden, atraso de 5s: dentro de politica
        .advance_watermark_to_infinity()
    )

    with TestPipeline() as p:
        alerts = (
            p
            | test_stream
            | "KeyByCardForGate" >> beam.Map(lambda e: (e["key"], e))
            | "TooLateGate" >> beam.ParDo(TooLateGateFn()).with_outputs(
                TooLateGateFn.TOO_LATE_TAG, main="on_time"
            )
        )
        on_time = alerts.on_time

        results = (
            on_time
            | "KeyByCard" >> beam.Map(lambda e: (e["key"], e))
            | "Window" >> window_transform()
            | "AggregateByCard" >> beam.CombinePerKey(CardWindowStatsFn())
            | "EvaluateRules" >> beam.ParDo(EvaluateRulesFn())
            | "StripGeneratedAt" >> beam.Map(_strip_generated_at)
        )
        assert_that(results, equal_to([_expected_pane(7, 7000)]), label="on_time_result")
        assert_that(alerts[TooLateGateFn.TOO_LATE_TAG], equal_to([]), label="no_dropped_events")


def test_event_beyond_policy_is_dropped_before_reaching_the_window():
    """Un evento cuyo atraso respecto del ultimo event_time visto para la
    tarjeta supera (ventana+lateness) no llega a la ventana/agregacion: el
    resultado final no lo incluye y aparece en la salida `too_late`."""
    threshold = TooLateGateFn().threshold_seconds
    test_stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(_event(), t) for t in (0, 5, 10, 15, 20, 25)])
        .add_elements([TimestampedValue(_event(), 25 + threshold + 1)])  # avanza el maximo visto
        .add_elements([TimestampedValue(_event(), 10)])  # ahora queda muy atras del maximo: se descarta
        .advance_watermark_to_infinity()
    )

    with TestPipeline() as p:
        gated = (
            p
            | test_stream
            | "KeyByCardForGate" >> beam.Map(lambda e: (e["key"], e))
            | "TooLateGate" >> beam.ParDo(TooLateGateFn()).with_outputs(
                TooLateGateFn.TOO_LATE_TAG, main="on_time"
            )
        )
        too_late = gated[TooLateGateFn.TOO_LATE_TAG]

        results = (
            gated.on_time
            | "KeyByCard" >> beam.Map(lambda e: (e["key"], e))
            | "Window" >> window_transform()
            | "AggregateByCard" >> beam.CombinePerKey(CardWindowStatsFn())
            | "EvaluateRules" >> beam.ParDo(EvaluateRulesFn())
            | "StripGeneratedAt" >> beam.Map(_strip_generated_at)
        )
        # El evento que actualiza el maximo visto (ts=206) cae en una ventana
        # distinta ([180,240)) y por si solo no dispara ninguna regla; el
        # resultado de la primera ventana debe seguir en count=6 (el evento
        # ts=10 fue descartado por el gate, no llego a sumarse).
        assert_that(results, equal_to([_expected_pane(6, 6000)]), label="first_window_unaffected")

        lag = (25 + threshold + 1) - 10
        expected_too_late = {
            "raw": json.dumps(_event()),
            "reason": (
                f"descartado_por_tardio: atraso={lag:.0f}s respecto del ultimo "
                f"event_time visto de la tarjeta > limite={threshold}s "
                f"(ventana=60s + allowed_lateness=120s)"
            ),
        }
        assert_that(too_late, equal_to([expected_too_late]), label="exactly_one_dropped")