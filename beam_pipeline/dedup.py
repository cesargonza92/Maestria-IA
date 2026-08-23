"""Deduplicacion de eventos por event_id, con horizonte acotado.

Antes de agrupar por tarjeta, los eventos se reordenan por event_id (seccion
8 de la propuesta). Se guarda un indicador de "visto" por event_id durante
DEDUP_HORIZON_SECONDS (10 minutos); una segunda copia del mismo event_id
dentro de ese horizonte se descarta. Pasado el horizonte, el estado se
limpia mediante un timer y un event_id repetido se trataria como nuevo --
esta es la limitacion de dedup declarada explicitamente (no es exactly-once
global, es una ventana de deduplicacion acotada).
"""

import apache_beam as beam
from apache_beam.coders import BooleanCoder
from apache_beam.metrics import Metrics
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec, TimerSpec, on_timer
from apache_beam.utils.timestamp import Duration

DEDUP_HORIZON_SECONDS = 600  # 10 minutos


class DeduplicateByEventId(beam.DoFn):
    """Espera elementos (event_id, event) y emite `event` solo la primera vez
    que se ve ese event_id dentro del horizonte de deduplicacion."""

    SEEN = ReadModifyWriteStateSpec("seen", BooleanCoder())
    EXPIRE_TIMER = TimerSpec("expire", TimeDomain.WATERMARK)

    def __init__(self, horizon_seconds: int = DEDUP_HORIZON_SECONDS):
        self.horizon_seconds = horizon_seconds

    def process(
        self,
        element,
        timestamp=beam.DoFn.TimestampParam,
        seen=beam.DoFn.StateParam(SEEN),
        expire_timer=beam.DoFn.TimerParam(EXPIRE_TIMER),
    ):
        _, event = element
        if seen.read():
            Metrics.counter("dedup", "duplicates_dropped").inc()
            return
        seen.write(True)
        expire_timer.set(timestamp + Duration(seconds=self.horizon_seconds))
        Metrics.counter("dedup", "unique_events").inc()
        yield event

    @on_timer(EXPIRE_TIMER)
    def expire(self, seen=beam.DoFn.StateParam(SEEN)):
        seen.clear()
