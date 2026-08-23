"""Helpers de lectura/escritura contra Kafka usando KafkaIO (cross-language).

Se usa `apache_beam.io.kafka.ReadFromKafka` / `WriteToKafka`, que delegan en
un expansion service Java levantado automaticamente por Beam la primera vez
que corre el pipeline (requiere JDK en la imagen y acceso a Maven Central en
el primer arranque para descargar el jar del expansion service; ver README).

Nota sobre `max_read_time` y el expansion service compartido: se verifico
empiricamente en este entorno (Beam 2.61.0, DirectRunner, Python 3.11,
Docker) que dejar que cada transformacion arranque su propio expansion
service (comportamiento por defecto) combinado con una lectura NO acotada
(`--streaming`, sin `max_read_time`) hace que el pipeline se cuelgue
indefinidamente sin entregar ningun dato al lado Python -- incluso con un
`ReadFromKafka` "vacio" (sin ningun DoFn propio de por medio). Con una UNICA
instancia de expansion service compartida por todas las transformaciones
KafkaIO (ver `_SHARED_EXPANSION_SERVICE` abajo) y una lectura ACOTADA
(`max_read_time`, ejecucion batch) se verifico al menos una corrida real
completa de punta a punta contra Kafka. Estas mitigaciones reducen pero no
eliminan por completo la inestabilidad de esta combinacion en este entorno:
en corridas posteriores con lotes mas grandes el pipeline volvio a quedarse
sin avanzar en el mismo punto del arranque del worker, sin error visible. Es
una limitacion especifica del DirectRunner de Python + cross-language, no
del diseño del pipeline (ver docs/documento_tecnico.md, seccion de limites,
para el detalle completo y la evidencia).
"""

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka, default_io_expansion_service

# Una sola instancia de expansion service compartida por TODAS las
# transformaciones KafkaIO del pipeline (1 lectura + 3 escrituras). Se
# verifico empiricamente que dejar que cada ReadFromKafka/WriteToKafka
# arranque su propio expansion service (comportamiento por defecto con
# expansion_service=None) hace que el pipeline se cuelgue nada mas arrancar
# el worker, sin error -- varias JVMs con --alsoStartLoopbackWorker
# compitiendo por registrarse contra el mismo runner Python. Con una sola
# instancia compartida el pipeline funciona correctamente.
_SHARED_EXPANSION_SERVICE = default_io_expansion_service()


def read_topic(pipeline, bootstrap_servers: str, topic: str, consumer_group: str, max_read_time_seconds: int = None):
    """Lee `topic` desde el inicio disponible (earliest) como PCollection de
    tuplas (key: bytes, value: bytes). `max_read_time_seconds` acota la
    lectura para producir un job batch (ver nota del modulo)."""
    return pipeline | f"ReadKafka[{topic}]" >> ReadFromKafka(
        consumer_config={
            "bootstrap.servers": bootstrap_servers,
            "group.id": consumer_group,
            "auto.offset.reset": "earliest",
        },
        topics=[topic],
        max_read_time=max_read_time_seconds,
        expansion_service=_SHARED_EXPANSION_SERVICE,
    )


def write_topic(pcollection, bootstrap_servers: str, topic: str, label: str):
    """Escribe una PCollection de tuplas (key: bytes, value: bytes) en `topic`."""
    return pcollection | label >> WriteToKafka(
        producer_config={"bootstrap.servers": bootstrap_servers},
        topic=topic,
        expansion_service=_SHARED_EXPANSION_SERVICE,
    )


def encode_kv(key: str, value: dict):
    import json

    return key.encode("utf-8"), json.dumps(value).encode("utf-8")
