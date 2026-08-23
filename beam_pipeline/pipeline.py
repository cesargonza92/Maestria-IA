"""Pipeline principal: transactions.raw -> Beam -> transactions.processed /
fraud.alerts / invalid.events.

Flujo (ver docs/arquitectura.md):
  1. Lectura desde Kafka (KafkaIO).
  2. Parseo + validacion de contrato -> valido / invalido (salida lateral).
  3. Asignacion de tiempo de evento (event_time, no processing time).
  4. Politica de datos tardios (`TooLateGateFn`): se descarta explicitamente
     un evento cuyo event_time atrasa en mas de (ventana + allowed_lateness)
     al event_time mas reciente visto hasta el momento para esa tarjeta, y
     se registra el motivo en invalid.events. Esto se implementa a nivel de
     aplicacion -- en vez de confiar unicamente en el `allowed_lateness` del
     propio `WindowInto` -- porque el DirectRunner de Beam Python (el runner
     usado en la demo local y en las pruebas) no garantiza el descarte de
     datos tardios por watermark: es una limitacion conocida de ese runner,
     verificada empiricamente (ver docs/documento_tecnico.md, limites). En
     un runner de produccion (Flink, Dataflow) el `allowed_lateness` del
     WindowInto ya lo haria de forma nativa; se deja configurado igual como
     segunda linea de defensa.
  5. Deduplicacion por event_id (horizonte acotado, ver dedup.py).
  6. Publicacion de transactions.processed (validos + deduplicados).
  7. Re-keying por tarjeta, ventana fija de 60s con allowed_lateness=120s,
     trigger de watermark + trigger tardio simple, modo ACCUMULATING (ver
     `window_transform`), agregacion incremental (CombinePerKey) y
     evaluacion de reglas.
  8. Publicacion de fraud.alerts (clave estable = upsert / idempotencia).

Runner por defecto: DirectRunner. Se declara semantica de entrega
"at-least-once" con deduplicacion acotada e idempotencia en la salida por
clave estable; no se afirma exactly-once end-to-end.

Modelo de ejecucion -- micro-lotes acotados, no un job de streaming
indefinido: se verifico empiricamente en este entorno (Beam 2.61.0,
DirectRunner, Python 3.11, Docker) que (a) dejar que cada transformacion
KafkaIO arranque su propio expansion service (comportamiento por defecto) y
(b) una lectura NO acotada (`--streaming`, sin `max_read_time`) hacen que el
pipeline se cuelgue indefinidamente sin entregar ningun dato al lado Python,
incluso sin ningun DoFn propio de por medio. Usando una unica instancia de
expansion service compartida (ver `io_kafka.py`) y una lectura ACOTADA
(`max_read_time`) se verifico al menos una corrida real completa de punta a
punta contra Kafka (validos en transactions.processed, job terminado
limpio). Estas mitigaciones REDUCEN pero no eliminan por completo la
inestabilidad: en corridas posteriores con lotes mas grandes el pipeline
volvio a quedarse sin avanzar en el mismo punto del arranque del worker, sin
error visible. Es una limitacion especifica del DirectRunner de Python
combinado con transformaciones cross-language, no del diseño del pipeline
(un runner de produccion como Flink no la tiene). Por eso la garantia de
correccion se sostiene en las pruebas por capas con TestPipeline/TestStream
(aisladas de Kafka), y el recorrido con Kafka real se documenta como
best-effort. Ver docs/documento_tecnico.md, seccion de limites, para el
detalle completo.
"""

import argparse
import json
import logging
import uuid

import apache_beam as beam
from apache_beam.coders import FloatCoder
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import trigger
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.window import FixedWindows, TimestampedValue

from dedup import DeduplicateByEventId
from io_kafka import read_topic, write_topic
from rules import CardWindowStatsFn, EvaluateRulesFn
from validation import parse_event_time, validate

log = logging.getLogger("beam_pipeline")

WINDOW_SECONDS = 60
ALLOWED_LATENESS_SECONDS = 120
TOO_LATE_THRESHOLD_SECONDS = WINDOW_SECONDS + ALLOWED_LATENESS_SECONDS


def window_transform():
    """Ventana fija de 60s: dispara al pasar el watermark y admite un
    disparo tardio simple mientras el dato llegue dentro de
    allowed_lateness=120s. Modo ACCUMULATING: cada pane reemplaza al
    anterior con el resultado completo recalculado (permite upsert por
    alert_id en el consumidor). Se expone como funcion para que la prueba de
    TestStream ejercite exactamente la misma configuracion que produccion."""
    return beam.WindowInto(
        FixedWindows(WINDOW_SECONDS),
        trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),
        accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        allowed_lateness=ALLOWED_LATENESS_SECONDS,
    )


class TooLateGateFn(beam.DoFn):
    """Descarta explicitamente un evento demasiado tardio para una tarjeta.

    Mantiene, por tarjeta, el event_time mas reciente visto (una
    aproximacion de progreso temporal local, no el watermark real de Beam).
    Un evento cuyo event_time atrasa a ese maximo en mas de
    `threshold_seconds` se envia a la salida lateral `too_late` en vez de
    seguir hacia la deduplicacion/ventana. Es deterministico y depende solo
    de los event_time de los datos, no del reloj de pared: se puede probar
    con TestStream sin depender de peculiaridades del runner."""

    TOO_LATE_TAG = "too_late"
    MAX_EVENT_TIME_SEEN = ReadModifyWriteStateSpec("max_event_time_seen", FloatCoder())

    def __init__(self, threshold_seconds: int = TOO_LATE_THRESHOLD_SECONDS):
        self.threshold_seconds = threshold_seconds

    def process(
        self,
        element,
        timestamp=beam.DoFn.TimestampParam,
        max_seen=beam.DoFn.StateParam(MAX_EVENT_TIME_SEEN),
    ):
        _, event = element
        ts = float(timestamp)
        current_max = max_seen.read()
        if current_max is None or ts > current_max:
            max_seen.write(ts)
            current_max = ts

        lag = current_max - ts
        if lag > self.threshold_seconds:
            Metrics.counter("pipeline", "dropped_too_late").inc()
            yield beam.pvalue.TaggedOutput(
                self.TOO_LATE_TAG,
                {
                    "raw": json.dumps(event),
                    "reason": (
                        f"descartado_por_tardio: atraso={lag:.0f}s respecto del ultimo "
                        f"event_time visto de la tarjeta > limite={self.threshold_seconds}s "
                        f"(ventana={WINDOW_SECONDS}s + allowed_lateness={ALLOWED_LATENESS_SECONDS}s)"
                    ),
                },
            )
        else:
            yield event


class ParseAndValidateFn(beam.DoFn):
    """Parsea el valor crudo de Kafka y separa validos de invalidos."""

    VALID_TAG = "valid"
    INVALID_TAG = "invalid"

    def process(self, kv):
        _, raw_value = kv
        ok, event, reason = validate(raw_value)
        if ok:
            Metrics.counter("pipeline", "valid_events").inc()
            yield beam.pvalue.TaggedOutput(self.VALID_TAG, event)
        else:
            Metrics.counter("pipeline", "invalid_events").inc()
            yield beam.pvalue.TaggedOutput(
                self.INVALID_TAG,
                {
                    "raw": raw_value.decode("utf-8", errors="replace"),
                    "reason": reason,
                },
            )


class AssignEventTimeFn(beam.DoFn):
    """Asigna el timestamp de Beam a partir de event_time del dominio (no del
    momento de procesamiento)."""

    def process(self, event):
        ts = parse_event_time(event["event_time"]).timestamp()
        yield TimestampedValue(event, ts)


def build_pipeline(pipeline, args):
    raw = read_topic(
        pipeline,
        args.bootstrap_servers,
        args.input_topic,
        args.consumer_group,
        max_read_time_seconds=args.max_read_time_seconds,
    )

    parsed = raw | "ParseValidate" >> beam.ParDo(ParseAndValidateFn()).with_outputs(
        ParseAndValidateFn.VALID_TAG, ParseAndValidateFn.INVALID_TAG
    )
    valid_events = parsed[ParseAndValidateFn.VALID_TAG]
    invalid_events = parsed[ParseAndValidateFn.INVALID_TAG]

    timestamped = valid_events | "AssignEventTime" >> beam.ParDo(AssignEventTimeFn())

    gated = (
        timestamped
        | "KeyByCardForGate" >> beam.Map(lambda e: (e["key"], e))
        | "TooLateGate" >> beam.ParDo(TooLateGateFn()).with_outputs(
            TooLateGateFn.TOO_LATE_TAG, main="on_time"
        )
    )
    on_time = gated.on_time
    too_late = gated[TooLateGateFn.TOO_LATE_TAG]

    deduped = (
        on_time
        | "KeyByEventId" >> beam.Map(lambda e: (e["event_id"], e))
        | "Dedup" >> beam.ParDo(DeduplicateByEventId())
    )

    write_topic(
        deduped
        | "EncodeProcessed" >> beam.Map(lambda e: (e["event_id"].encode("utf-8"), json.dumps(e).encode("utf-8"))),
        args.bootstrap_servers,
        args.processed_topic,
        "WriteProcessed",
    )

    alerts = (
        deduped
        | "KeyByCard" >> beam.Map(lambda e: (e["key"], e))
        | "Window" >> window_transform()
        | "AggregateByCard" >> beam.CombinePerKey(CardWindowStatsFn())
        | "EvaluateRules" >> beam.ParDo(EvaluateRulesFn())
    )

    write_topic(
        alerts
        | "EncodeAlerts" >> beam.Map(lambda a: (a["alert_id"].encode("utf-8"), json.dumps(a).encode("utf-8"))),
        args.bootstrap_servers,
        args.alerts_topic,
        "WriteAlerts",
    )

    combined_invalid = (invalid_events, too_late) | "FlattenInvalid" >> beam.Flatten()
    write_topic(
        combined_invalid
        | "EncodeInvalid" >> beam.Map(lambda e: (str(uuid.uuid4()).encode("utf-8"), json.dumps(e).encode("utf-8"))),
        args.bootstrap_servers,
        args.invalid_topic,
        "WriteInvalid",
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--input-topic", default="transactions.raw")
    parser.add_argument("--processed-topic", default="transactions.processed")
    parser.add_argument("--alerts-topic", default="fraud.alerts")
    parser.add_argument("--invalid-topic", default="invalid.events")
    parser.add_argument("--consumer-group", default="beam-pipeline")
    parser.add_argument(
        "--max-read-time-seconds",
        type=int,
        default=90,
        help="Duracion del micro-lote de lectura (ver nota de modelo de ejecucion arriba).",
    )
    return parser.parse_known_args(argv)


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s beam %(levelname)s %(message)s")
    known_args, pipeline_args = parse_args(argv)
    pipeline_args += ["--save_main_session"]
    options = PipelineOptions(pipeline_args)
    log.info("Iniciando micro-lote (%ss) con args=%s", known_args.max_read_time_seconds, vars(known_args))
    with beam.Pipeline(options=options) as pipeline:
        build_pipeline(pipeline, known_args)
    log.info("Micro-lote completo.")


if __name__ == "__main__":
    main()
