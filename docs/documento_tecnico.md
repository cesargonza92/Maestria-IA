# Documento técnico — Detección de patrones sospechosos en streaming

**Proyecto integrador — Streaming de datos y sus aplicaciones (Maestría en IA, FPUNA)**

## 1. Problema, usuarios y métricas

En un sistema de pagos es común que una misma tarjeta genere varias operaciones en pocos segundos; la mayoría
son válidas, pero ciertas combinaciones (frecuencia inusual, monto acumulado elevado, operaciones desde
distintos países o en muchos comercios en poco tiempo) ameritan una revisión rápida. El problema no es solo
calcular una regla: los eventos pueden llegar repetidos, desordenados o con retraso, y el resultado debe seguir
siendo consistente.

Este proyecto implementa un pipeline que recibe transacciones **sintéticas** por Kafka, valida su contrato,
procesa los eventos con Apache Beam y publica alertas consultables por un analista de riesgo. **No se usan
datos bancarios reales** y las reglas son un prototipo académico, no un motor antifraude de producción.

- **Usuario del resultado**: un analista de riesgo (o el propio equipo docente) que consulta `fraud.alerts`
  para ver qué tarjetas dispararon qué regla, en qué ventana de 60s, y con qué evidencia (conteo, monto,
  países, comercios).
- **Decisión que habilita**: priorizar la revisión manual de una tarjeta antes de que se acumulen más
  operaciones sospechosas.
- **Métricas expuestas** (contadores de Beam, ver logs del contenedor `beam_pipeline`): eventos válidos,
  inválidos, duplicados descartados, eventos descartados por la política de tardanza, y alertas emitidas.

## 2. Arquitectura

Ver [`arquitectura.md`](arquitectura.md) para el diagrama completo y la descripción de cada componente. En
resumen: `Productor → transactions.raw (Kafka) → Beam (validar → gate de tardanza → dedup → ventana →
agregación → reglas) → transactions.processed / fraud.alerts / invalid.events (Kafka) → Consumidor`.

Runner: **DirectRunner**, ejecutado dentro de un contenedor Docker (`beam_pipeline`). Flink no se implementó en
esta entrega (queda como extensión opcional, ver sección 6); el foco se puso en que el núcleo obligatorio
funcione y esté probado, en línea con la consigna del curso.

## 3. Contrato de eventos, tópicos, claves y particiones

**Contrato** (`contracts/transaction_schema.json`, versión `1.0`, JSON): `schema_version`, `event_id` (UUID,
usado para deduplicación), `key` (tarjeta tokenizada, también clave de Kafka), `event_time` (UTC, ISO 8601),
`customer_id`, `merchant_id`, `amount` (entero, unidad mínima de la moneda), `currency` (solo `PYG` en esta
versión), `country` (ISO 3166-1 alpha-2), `channel` (`POS`/`WEB`/`ATM`), `status`. La estrategia de evolución de
esquema es el propio campo `schema_version`: un evento con una versión no soportada se rechaza explícitamente
en vez de procesarse con supuestos incorrectos.

**Tópicos**:

| Tópico | Clave | Particiones | Contenido |
|---|---|---|---|
| `transactions.raw` | tarjeta tokenizada | 3 | Eventos crudos del productor. |
| `transactions.processed` | `event_id` | 3 | Eventos válidos y deduplicados. |
| `fraud.alerts` | `alert_id` (compactado) | 1 | Alertas, una entrada por `alert_id` vigente. |
| `invalid.events` | motivo de rechazo | 1 | Eventos rechazados por contrato o por la política de tardanza, con motivo. |

La clave de tarjeta en `transactions.raw` conserva el orden de las operaciones de una misma tarjeta dentro de
una partición (necesario para el dedup y la agregación por ventana) y permite paralelismo entre tarjetas
distintas. Con 3 particiones y una decena de tarjetas activas en la demo, la distribución es razonablemente
uniforme; en un volumen mayor habría que medir la distribución real de claves antes de fijar el número final de
particiones (ver límites).

**Esquema de salida** (`fraud.alerts`): `alert_id`, `key`, `window_start`, `window_end`, `alert_type`,
`transaction_count`, `amount_sum`, `countries`, `merchant_count`, `generated_at`.

## 4. Ventanas, lateness, deduplicación, idempotencia y semántica de entrega

- **Ventana**: fija, de 60 segundos, sobre `event_time` (no tiempo de procesamiento) — las reglas comparan
  actividad en intervalos cortos y fáciles de explicar en la demo.
- **Trigger / allowed lateness**: `AfterWatermark(late=AfterCount(1))`, modo `ACCUMULATING`,
  `allowed_lateness=120s`. Cada disparo tardío recalcula el resultado completo de la ventana (no un delta), lo
  que permite que el consumidor se quede con el último pane sin lógica adicional.
- **Política de datos tardíos**: además de la configuración anterior, el pipeline implementa un **gate
  explícito** (`TooLateGateFn`) que compara, por tarjeta, el `event_time` de cada evento contra el máximo
  `event_time` visto hasta el momento para esa misma tarjeta. Un evento cuyo atraso supera
  ventana+lateness (180s) se enruta a `invalid.events` con el motivo, en lugar de llegar a la ventana. Esta
  decisión de diseño se explica en el límite conocido de la sección 6.
- **Deduplicación**: por `event_id`, con un horizonte acotado de **10 minutos** (`DeduplicateByEventId`, estado
  + timer). Un reenvío del mismo `event_id` dentro de ese horizonte se descarta; pasado el horizonte, un
  `event_id` repetido se trataría como nuevo — limitación declarada explícitamente, no es deduplicación global.
- **Idempotencia de la salida**: `alert_id = tarjeta|window_start|window_end|alert_type` es estable frente a
  reintentos o paneles posteriores de la misma ventana, lo que permite upsert/materialización del último
  resultado (y compactación nativa de Kafka en `fraud.alerts`).
- **Semántica de entrega declarada**: **at-least-once** de punta a punta, con deduplicación acotada (10 min) e
  idempotencia en la materialización de salida por clave estable. **No se afirma exactly-once end-to-end.**

## 5. Pruebas

- **Unitarias** (`tests/unit/`): validación de contrato (`test_validation.py`), umbrales y agregación incremental
  de las 4 reglas (`test_rules.py`), construcción estable de `alert_id`, y el escenario de duplicado exacto
  (`test_dedup.py`, usando `TestPipeline`).
- **Ventanas y tiempo de evento** (`tests/streaming/test_windowing_teststream.py`, con `TestStream`): ventana
  on-time, un evento fuera de orden dentro de la política (se refleja en el resultado), y un evento más allá de
  la política (se descarta antes de llegar a la ventana, verificado en la salida `too_late`).
- **End-to-end** (`tests/e2e/test_end_to_end.py`): smoke test contra el entorno real (`docker compose`):
  publica eventos normales, un duplicado y uno tardío, y verifica en `fraud.alerts`/`invalid.events` que el
  recorrido completo funciona y que el duplicado no generó una alerta adicional. Se salta automáticamente si
  Kafka no está disponible en `localhost:29092`.

## 6. Límites conocidos, supuestos y posibles mejoras

- **Limitación verificada del DirectRunner de Beam Python (allowed_lateness)**: durante el desarrollo se
  comprobó empíricamente que este runner, en ejecución local, no garantiza el descarte de datos tardíos
  únicamente a través de `allowed_lateness` en `WindowInto` (un evento mucho más allá del horizonte permitido
  seguía siendo incorporado al resultado). Por eso la política de tardanza se implementa también de forma
  explícita en la aplicación (`TooLateGateFn`), en lugar de depender solo del comportamiento del runner. La
  configuración de `allowed_lateness` se mantiene en el `WindowInto` como segunda línea de defensa y porque es
  la forma correcta de declararlo de cara a un runner de producción (Flink, Dataflow).
- **Limitación verificada de KafkaIO cross-language en el DirectRunner (Python)**: durante el desarrollo se
  comprobó empíricamente que, en este entorno (Beam 2.61.0, DirectRunner, Python 3.11, Docker), dejar que cada
  transformación `ReadFromKafka`/`WriteToKafka` arranque su propio *expansion service* Java (comportamiento por
  defecto) combinado con una ejecución de streaming indefinida (`--streaming`, sin `max_read_time`) hace que el
  pipeline se cuelgue sin entregar ningún dato al lado Python, sin ningún error visible. Se aisló la causa
  probando por partes (lectura sola, lectura+escritura, DoFn con estado, ventana + `CombinePerKey`) y se
  aplicaron dos mitigaciones: **una única instancia de expansion service compartida** por todas las
  transformaciones KafkaIO del pipeline (`_SHARED_EXPANSION_SERVICE` en `beam_pipeline/io_kafka.py`) y una
  **lectura acotada** (`max_read_time`, ejecución batch de "micro-lotes" en vez de streaming indefinido; ver
  `--max-read-time-seconds` en `beam_pipeline/pipeline.py`, 90s por defecto, reiniciado automáticamente por
  `docker-compose.yml` vía `restart: unless-stopped`).
  Con estas dos mitigaciones se verificó **al menos una corrida real y completa contra Kafka**: eventos
  publicados por el productor fueron leídos, validados, deduplicados y escritos en `transactions.processed`
  de punta a punta, con el job terminando limpio ("Micro-lote completo."). Sin embargo, en corridas
  posteriores contra lotes más grandes el mismo pipeline volvió a quedarse sin avanzar en el mismo punto del
  arranque del worker (después de "Creating state cache", antes de procesar el primer elemento), sin errores.
  Es decir: las mitigaciones redujeron pero **no eliminaron por completo** la inestabilidad de esta
  combinación específica (KafkaIO cross-language + múltiples DoFn con estado/ventanas + DirectRunner de
  Python) en este entorno — el comportamiento no fue 100% consistente entre corridas. Esta es la razón por la
  que el pipeline se declara y prueba exhaustivamente por capas (validación, reglas, dedup, ventanas/lateness)
  con `TestPipeline`/`TestStream` de forma aislada de Kafka (sección 5), que es donde se sostiene la garantía
  de corrección, y por qué el smoke test end-to-end (`tests/e2e/`) se documenta como best-effort dependiente
  del entorno en vez de una garantía dura. Un runner de producción (Flink, Dataflow) no tiene esta limitación,
  que es específica del DirectRunner de Beam para Python combinado con transformaciones cross-language.
- Los datos son sintéticos; la lógica no reemplaza un sistema real de prevención de fraude.
- La primera versión procesa únicamente `PYG`; no incluye conversión de monedas.
- La deduplicación cubre un horizonte acotado de 10 minutos, no una garantía global.
- El entorno local (Docker Compose, DirectRunner, un solo broker) no representa disponibilidad, seguridad ni
  capacidad de producción.
- El primer arranque del contenedor `beam_pipeline` requiere acceso a internet: Beam descarga el *expansion
  service* Java de `KafkaIO` (cross-language) desde Maven Central la primera vez. Se cachea en un volumen
  Docker (`beam_expansion_cache`) para no repetir la descarga en arranques posteriores.
- La calidad de las alertas depende de umbrales fijados para la demostración (sección de reglas), no de un
  análisis estadístico de datos reales.
- **Mejoras posibles** (fuera del alcance de esta entrega): ejecución con Flink como runner, dashboard de
  métricas, análisis de distribución/skew de claves con mayor volumen, reglas configurables, y triggers
  tempranos si el caso de uso lo justificara.
