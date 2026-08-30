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

Runner: **Flink** por defecto (`--runner=flink`), streaming real (sin cota de
tiempo de lectura). Tambien se soporta **DirectRunner** (`--runner=direct`)
para pruebas rapidas locales sin levantar el cluster de Flink -- ese modo
corre como micro-lotes acotados (`--max-read-time-seconds`), un workaround
necesario solo para el DirectRunner (ver mas abajo). Se declara semantica de
entrega "at-least-once" con deduplicacion acotada e idempotencia en la salida
por clave estable; no se afirma exactly-once end-to-end.

Por que Flink es el runner por defecto -- se verifico empiricamente en este
entorno (Beam 2.61.0, DirectRunner, Python 3.11, Docker) que el DirectRunner
combinado con KafkaIO cross-language se cuelga o queda inestable de forma
intermitente (memoria, puertos efimeros de varias JVMs) sin entregar datos,
incluso con una unica instancia de expansion service compartida y lectura
acotada como mitigacion (ver `io_kafka.py`). Es una limitacion especifica de
esa combinacion runner+cross-language, no del diseño del pipeline: con Flink
como runner real (via `PortableRunner`, sometido al job server oficial de
Beam) el pipeline corre como streaming genuino, sin el workaround de
micro-lotes. Ver docs/documento_tecnico.md, seccion de limites, para el
detalle completo (incluye la corrida que reprodujo el cuelgue del
DirectRunner en vivo, motivo de esta migracion).
"""

import os
import argparse
import json
import logging
import typing
import uuid
from pathlib import Path

import apache_beam as beam
from apache_beam.coders import FloatCoder
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import trigger
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec, TimerSpec, on_timer
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.utils.timestamp import Duration

# Tipo de salida explicito para los pasos "Encode*" que preceden a cada
# WriteToKafka (transformacion cross-language en Java): sin esta pista, Beam
# infiere un coder de Python (pickle) para la tupla de bytes en vez del
# coder portable que el lado Java necesita para decodificar, y falla (o, en
# el runner de Flink, queda atascado sin error visible) en el cruce
# Python->Java -- verificado empiricamente (ver docs/documento_tecnico.md).
KAFKA_RECORD_TYPE = typing.Tuple[bytes, bytes]

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
    con TestStream sin depender de peculiaridades del runner.

    El "maximo visto" se acota de dos formas para no depender de que los
    datos de entrada sean siempre bien comportados:

    - `max_future_skew_seconds`: un solo evento no puede empujar el maximo
      mas alla de ese margen de una sola vez. Sin este limite, un unico
      event_time anomalo (error de reloj del cliente, dato malicioso) deja
      el maximo corrompido para siempre, y CUALQUIER evento legitimo
      posterior de esa tarjeta pareceria "demasiado atrasado" respecto de
      ese valor corrupto -- un problema real dado que este pipeline procesa
      justamente el tipo de dato adversarial que un sistema antifraude
      deberia tolerar.
    - `ttl_seconds`: sin actividad de la tarjeta durante ese lapso, el
      estado se limpia via timer (mismo patron que `DeduplicateByEventId`
      en dedup.py). Sin esto, el estado crece sin limite -- una entrada por
      cada tarjeta distinta vista alguna vez -- durante toda la vida del
      job de streaming."""

    TOO_LATE_TAG = "too_late"
    MAX_EVENT_TIME_SEEN = ReadModifyWriteStateSpec("max_event_time_seen", FloatCoder())
    EXPIRE_TIMER = TimerSpec("expire", TimeDomain.WATERMARK)

    DEFAULT_MAX_FUTURE_SKEW_SECONDS = 300
    DEFAULT_TTL_SECONDS = 3600  # 1 hora de inactividad de la tarjeta

    def __init__(
        self,
        threshold_seconds: int = TOO_LATE_THRESHOLD_SECONDS,
        max_future_skew_seconds: int = DEFAULT_MAX_FUTURE_SKEW_SECONDS,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
    ):
        self.threshold_seconds = threshold_seconds
        self.max_future_skew_seconds = max_future_skew_seconds
        self.ttl_seconds = ttl_seconds

    def process(
        self,
        element,
        timestamp=beam.DoFn.TimestampParam,
        max_seen=beam.DoFn.StateParam(MAX_EVENT_TIME_SEEN),
        expire_timer=beam.DoFn.TimerParam(EXPIRE_TIMER),
    ):
        _, event = element
        ts = float(timestamp)
        current_max = max_seen.read()
        if current_max is None:
            current_max = ts
        elif ts > current_max:
            current_max = min(ts, current_max + self.max_future_skew_seconds)
        max_seen.write(current_max)
        expire_timer.set(timestamp + Duration(seconds=self.ttl_seconds))

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

    @on_timer(EXPIRE_TIMER)
    def expire(self, max_seen=beam.DoFn.StateParam(MAX_EVENT_TIME_SEEN)):
        max_seen.clear()


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
    # La cota de lectura (max_read_time) es solo un workaround del
    # DirectRunner; con Flink el pipeline lee de forma continua (streaming
    # real), asi que se omite.
    max_read_time_seconds = args.max_read_time_seconds if args.runner == "direct" else None
    raw = read_topic(
        pipeline,
        args.bootstrap_servers,
        args.input_topic,
        args.consumer_group,
        max_read_time_seconds=max_read_time_seconds,
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
        | "EncodeProcessed"
        >> beam.Map(lambda e: (e["event_id"].encode("utf-8"), json.dumps(e).encode("utf-8"))).with_output_types(
            KAFKA_RECORD_TYPE
        ),
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
        | "EncodeAlerts"
        >> beam.Map(lambda a: (a["alert_id"].encode("utf-8"), json.dumps(a).encode("utf-8"))).with_output_types(
            KAFKA_RECORD_TYPE
        ),
        args.bootstrap_servers,
        args.alerts_topic,
        "WriteAlerts",
    )

    combined_invalid = (invalid_events, too_late) | "FlattenInvalid" >> beam.Flatten()
    write_topic(
        combined_invalid
        | "EncodeInvalid"
        >> beam.Map(lambda e: (str(uuid.uuid4()).encode("utf-8"), json.dumps(e).encode("utf-8"))).with_output_types(
            KAFKA_RECORD_TYPE
        ),
        args.bootstrap_servers,
        args.invalid_topic,
        "WriteInvalid",
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    #parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    )
    parser.add_argument("--input-topic", default="transactions.raw")
    parser.add_argument("--processed-topic", default="transactions.processed")
    parser.add_argument("--alerts-topic", default="fraud.alerts")
    parser.add_argument("--invalid-topic", default="invalid.events")
    parser.add_argument("--consumer-group", default="beam-pipeline")
    parser.add_argument(
        "--runner",
        choices=["flink", "direct"],
        default="flink",
        help="'flink' (por defecto): streaming real via PortableRunner sobre un cluster Flink. "
        "'direct': DirectRunner local en micro-lotes acotados, para pruebas rapidas sin Flink.",
    )
    parser.add_argument(
        "--job-endpoint",
        default=os.getenv(
            "FLINK_JOB_ENDPOINT",
            os.getenv("BEAM_JOB_ENDPOINT", "localhost:8099"),
        ),
        help="Endpoint del job server de Beam para Flink (solo --runner=flink).",
    )
    parser.add_argument(
        "--environment-type",
        default="DOCKER",
        choices=["DOCKER", "PROCESS"],
        help="Ambiente de ejecucion de los workers de Flink (solo --runner=flink). "
        "DOCKER (por defecto): un contenedor efimero por worker, simetrico para Python y "
        "para el entorno Java nativo de KafkaIO -- requiere --network=host entre el "
        "El runner Flink requiere conectividad mediante red host entre el "
        "TaskManager y los contenedores SDK Harness. Esta configuracion fue "
        "validada con Windows + WSL2 + Docker Desktop; en otros entornos debe "
        "verificarse que Docker soporte correctamente --network host."
        "realmente; verificado empiricamente (ver limites en docs/documento_tecnico.md). "
        "PROCESS: subproceso local en el propio TaskManager -- funciona para los DoFn de "
        "Python (ver flink/taskmanager.Dockerfile) pero deja sin resolver el entorno Java "
        "de KafkaIO, que seguiria requiriendo DOCKER; no se uso como default por eso.",
    )
    parser.add_argument(
        "--max-read-time-seconds",
        type=int,
        default=90,
        help="Duracion del micro-lote de lectura, solo aplica con --runner=direct.",
    )
    return parser.parse_known_args(argv)


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s beam %(levelname)s %(message)s")
    known_args, pipeline_args = parse_args(argv)

    if known_args.runner == "flink":
        pipeline_args += [
            "--runner=PortableRunner",
            f"--job_endpoint={known_args.job_endpoint}",
            f"--environment_type={known_args.environment_type}",
            f"--setup_file={Path(__file__).resolve().parent / 'setup.py'}",
            "--streaming",
            "--save_main_session",
            # Requerido para que la lectura basada en SDF de KafkaIO
            # (ReadSourceDescriptors) confirme progreso y emita datos en
            # Flink: sin checkpointing periodico el operador queda "vivo"
            # (CPU alto) pero nunca finaliza el primer elemento -- verificado
            # empiricamente (0 registros de salida indefinidamente).
            "--checkpointing_interval=10000",
        ]
        if known_args.environment_type == "PROCESS":
            # El TaskManager (ver flink/taskmanager.Dockerfile) tiene copiado
            # el runtime de Python+Beam de la imagen oficial del SDK; el
            # binario /opt/apache/beam/boot arranca el worker como
            # subproceso local, sin Docker-in-Docker ni networking especial.
            # Se usa --environment_config (JSON) en vez de --environment_option:
            # este ultimo dispara una validacion del job server que falla
            # ("environment type 'null'") cuando el pipeline tiene mas de un
            # environment (aca hay dos: el de KafkaIO via expansion service,
            # y el propio de los DoFn en Python) -- verificado empiricamente.
            pipeline_args.append('--environment_config={"command": "/opt/apache/beam/boot"}')
        log.info(
            "Iniciando pipeline de streaming sobre Flink (job_endpoint=%s, environment_type=%s)",
            known_args.job_endpoint, known_args.environment_type,
        )
    else:
        pipeline_args += ["--save_main_session"]
        log.info("Iniciando micro-lote (%ss) con DirectRunner", known_args.max_read_time_seconds)

    options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=options) as pipeline:
        build_pipeline(pipeline, known_args)
    log.info("Pipeline finalizado.")


if __name__ == "__main__":
    main()
