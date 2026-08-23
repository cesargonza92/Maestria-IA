"""Escenario de duplicados: un event_id repetido debe emitirse una sola vez.

Se usa TestPipeline (DirectRunner en modo batch) porque DeduplicateByEventId
es un DoFn con estado y timers; no se puede probar como funcion pura, pero
el escenario en si (duplicado -> se ignora) no depende de la semantica de
watermark de streaming, alcanza con una corrida batch simple.
"""

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dedup import DeduplicateByEventId

EVENT_A = {"event_id": "e1", "key": "card-1", "amount": 1000}
EVENT_A_RETRY = {"event_id": "e1", "key": "card-1", "amount": 1000}  # mismo event_id, reenvio
EVENT_B = {"event_id": "e2", "key": "card-1", "amount": 2000}


def test_duplicate_event_id_is_emitted_only_once():
    with TestPipeline() as p:
        result = (
            p
            | beam.Create([EVENT_A, EVENT_A_RETRY, EVENT_B])
            | "KeyByEventId" >> beam.Map(lambda e: (e["event_id"], e))
            | "Dedup" >> beam.ParDo(DeduplicateByEventId())
        )
        assert_that(result, equal_to([EVENT_A, EVENT_B]))


def test_events_with_different_event_id_are_all_emitted():
    events = [{"event_id": f"e{i}", "key": "card-1", "amount": i} for i in range(5)]
    with TestPipeline() as p:
        result = (
            p
            | beam.Create(events)
            | "KeyByEventId" >> beam.Map(lambda e: (e["event_id"], e))
            | "Dedup" >> beam.ParDo(DeduplicateByEventId())
        )
        assert_that(result, equal_to(events))
