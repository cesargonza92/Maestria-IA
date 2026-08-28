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

Runner: **Flink**, vía el `PortableRunner` de Beam. El contenedor `beam_pipeline` construye el grafo del
pipeline (incluida la expansión de KafkaIO) y lo somete al job server oficial de Beam para Flink
(`beam_flink_job_server`), que lo despacha a un cluster de Flink (`jobmanager` + `taskmanager`) levantado por
`docker-compose.yml`. El `taskmanager` ejecuta los workers del SDK harness en contenedores efímeros
(`environment_type=DOCKER`). También se soporta **DirectRunner** (`--runner=direct`) como modo liviano para
pruebas rápidas sin levantar el cluster completo. **Estado verificado de esta arquitectura**: streaming real de
punta a punta contra Flink, con datos fluyendo continuamente por las cuatro etapas hasta `fraud.alerts` (ver
sección 6 para el recorrido completo de diagnóstico: requiere red host y fue validada en
Windows + WSL2 + Docker Desktop; y corrige un bug de coder en el cruce Python↔Java que
también se documenta ahí).

## 3. Contrato de eventos, tópicos, claves y particiones

**Contrato** (`contracts/transaction_schema.json`, versión `1.0`, JSON): `schema_version`, `event_id` (UUID,
usado para deduplicación), `key` (tarjeta tokenizada, también clave de Kafka), `event_time` (UTC, ISO 8601),
`customer_id`, `merchant_id`, `amount` (entero, unidad mínima de la moneda, con tope superior de sanidad —
`1.000.000.000` — para rechazar datos corruptos o maliciosos, no una cifra bancaria real), `currency` (solo
`PYG` en esta versión), `country` (ISO 3166-1 alpha-2), `channel` (`POS`/`WEB`/`ATM`), `status`. La estrategia
de evolución de esquema es el propio campo `schema_version`: un evento con una versión no soportada se rechaza
explícitamente en vez de procesarse con supuestos incorrectos.

**Tópicos**:

| Tópico | Clave | Particiones | Contenido |
|---|---|---|---|
| `transactions.raw` | tarjeta tokenizada | 3 | Eventos crudos del productor. |
| `transactions.processed` | `event_id` | 3 | Eventos válidos y deduplicados. |
| `fraud.alerts` | `alert_id` (compactado) | 1 | Alertas, una entrada por `alert_id` vigente. |
| `invalid.events` | motivo de rechazo | 1 | Eventos rechazados por contrato o por la política de tardanza, con motivo. |

La clave de tarjeta en `transactions.raw` conserva el orden de las operaciones de una misma tarjeta dentro de
una partición (necesario para el dedup y la agregación por ventana) y permite paralelismo entre tarjetas
distintas: 3 particiones permiten hasta 3 workers de lectura concurrentes, tope razonable para la escala de
esta demo (una decena de tarjetas activas a la vez) sin fragmentar tanto el tráfico de una tarjeta individual
como para perder el beneficio de agrupar su actividad.

**Distribución real medida** (`scripts/analyze_partition_skew.py`, corrida de referencia sobre un lote de
demostración de 130 eventos / 11 tarjetas distintas, particionador por defecto de `kafka-python`):

| Partición | Mensajes | % del total | Tarjetas distintas |
|---|---|---|---|
| 0 | 49 | 37.7% | 4 |
| 1 | 30 | 23.1% | 3 |
| 2 | 51 | 39.2% | 4 |

Distribución razonablemente uniforme (23-39%) para una muestra chica, sin sesgo severo hacia ninguna
partición. El script también verificó, sobre esta misma corrida, que **cada tarjeta cayó siempre en la misma
partición** (particionamiento estable por clave) y que **no hubo violaciones de orden** dentro de ninguna
partición — las dos propiedades de las que depende el dedup y la agregación por ventana. Esta medición es de
escala de demo (decenas de tarjetas), no un estudio de skew a escala de producción (miles de tarjetas con
actividad muy desigual, donde unas pocas tarjetas "calientes" podrían dominar una partición) — ver límites.

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
  decisión de diseño se explica en el límite conocido de la sección 6. El máximo visto por tarjeta está acotado
  de dos formas: (1) un solo evento no puede empujarlo más de `max_future_skew_seconds` (300s por defecto) de
  una sola vez, para que un `event_time` anómalo (error de reloj del cliente, dato malicioso) no corrompa el
  gate de forma permanente para toda actividad legítima posterior de esa tarjeta; (2) el estado expira via
  timer tras `ttl_seconds` (1 hora por defecto) de inactividad de la tarjeta, para que no crezca sin límite
  durante toda la vida del job de streaming — mismo patrón que la deduplicación de abajo.
- **Deduplicación**: por `event_id`, con un horizonte acotado de **10 minutos** (`DeduplicateByEventId`, estado
  + timer). Un reenvío del mismo `event_id` dentro de ese horizonte se descarta; pasado el horizonte, un
  `event_id` repetido se trataría como nuevo — limitación declarada explícitamente, no es deduplicación global.
- **Idempotencia de la salida**: `alert_id = tarjeta|window_start|window_end|alert_type` es estable frente a
  reintentos o paneles posteriores de la misma ventana, lo que permite upsert/materialización del último
  resultado (y compactación nativa de Kafka en `fraud.alerts`).
- **Semántica de entrega declarada**: **at-least-once** de punta a punta, con deduplicación acotada (10 min) e
  idempotencia en la materialización de salida por clave estable. **No se afirma exactly-once end-to-end.**

### Análisis de fallos y reintentos

No es solo una declaración: durante el desarrollo se observaron en vivo, y se documentan aquí, cuatro
escenarios reales de falla, con qué garantiza el diseño ante cada uno y qué no.

1. **Falla de una tarea dentro de un job en curso** (ej. un worker se cae por un problema transitorio). Con
   checkpointing periódico habilitado (`--checkpointing_interval=10000` en `beam_pipeline/pipeline.py`), Flink
   reinicia automáticamente la tarea afectada desde el último checkpoint completado, sin reenviar el job
   entero ni perder el estado acumulado hasta ese punto (dedup, ventanas en curso, gate de tardanza).
   **Verificado empíricamente**: durante el desarrollo se observó al menos una recuperación real desde
   checkpoint (`"restored":{"id":8,...}` en la API de checkpoints de Flink, `localhost:8081/jobs/<id>/checkpoints`)
   tras una falla transitoria, con el job continuando sin necesidad de un nuevo envío.

2. **Falla del TaskManager o de todo el cluster de Flink** — a diferencia del escenario anterior, esto sí tira
   el job completo. `beam_pipeline` (con `restart: unless-stopped`) somete un job **completamente nuevo** al
   reconectar, que **no hereda el estado del anterior**: el horizonte de deduplicación, cualquier ventana en
   curso y el máximo `event_time` visto por tarjeta en el gate de tardanza arrancan de cero. Esto se observó en
   vivo repetidas veces durante el ajuste de memoria del TaskManager (sección 6): el pipeline siempre se
   recuperó y volvió a procesar tráfico nuevo correctamente, pero cualquier ventana en curso en el momento de
   la caída se perdió. Mitigación posible fuera del alcance de esta entrega: reanudar desde un *savepoint*
   explícito en vez de someter un job sin estado previo.

3. **Reintento de escritura a Kafka (sink)**: la semántica declarada es at-least-once, no exactly-once — un
   reintento tras una falla transitoria del broker puede producir un mensaje duplicado en el tópico de salida.
   El diseño lo tolera por construcción en dos de los tres sinks: `transactions.processed` (clave `event_id`)
   y `fraud.alerts` (clave `alert_id`, compactado) — un duplicado de escritura es indistinguible de un pane
   reenviado y se resuelve igual, por upsert. `invalid.events` usa una clave aleatoria (no hay una clave
   natural para un evento inválido) y por lo tanto sí puede terminar con entradas duplicadas ante un reintento
   de escritura — aceptado explícitamente porque ese tópico es de diagnóstico, no de negocio.

4. **Borde exacto de un reenvío de job** (continuación del escenario 2): el punto preciso de reanudación de la
   lectura de Kafka al someter un job nuevo depende del comportamiento de *commit* de offsets de KafkaIO, que
   no se verificó con precisión mensaje a mensaje. El diseño no depende de esa precisión: el dedup por
   `event_id` (horizonte de 10 min) absorbe una relectura de mensajes ya procesados, y la naturaleza idempotente
   de la salida absorbe una reescritura. **Límite declarado explícitamente**: no se garantiza cero-duplicados
   ni cero-pérdida en el borde de un reenvío de job; se garantiza que el sistema converge a un resultado
   consistente a pesar de ellos.

## 5. Pruebas

- **Unitarias** (`tests/unit/`): validación de contrato incluido el tope de sanidad de `amount`
  (`test_validation.py`), umbrales y agregación incremental de las 4 reglas (`test_rules.py`), construcción
  estable de `alert_id`, y el escenario de duplicado exacto (`test_dedup.py`, usando `TestPipeline`).
- **Ventanas y tiempo de evento** (`tests/streaming/test_windowing_teststream.py`, con `TestStream`): ventana
  on-time, un evento fuera de orden dentro de la política (se refleja en el resultado), un evento más allá de
  la política (se descarta antes de llegar a la ventana, verificado en la salida `too_late`), el tope de avance
  del gate ante un `event_time` anómalo (verificado inspeccionando el atraso reportado en el evento
  descartado), y la expiración del estado del gate tras el TTL de inactividad de una tarjeta.
- **End-to-end** (`tests/e2e/test_end_to_end.py`): contra el entorno real (`docker compose`), dos pruebas. La
  primera publica eventos normales, un duplicado y uno tardío, y verifica en `fraud.alerts`/`invalid.events`
  que el recorrido completo funciona y que el duplicado no generó una alerta adicional (espera el cierre de
  ventana, ~60-90s). La segunda es una regresión dirigida y rápida que no depende del cierre de ventana:
  publica un evento válido y uno inválido y confirma que ambos caminos de escritura (`transactions.processed`,
  `invalid.events`) completan sin la excepción de coder que rompió el cruce Python↔Java (sección 6) — existe
  específicamente porque ese bug no tenía ninguna otra cobertura de pruebas. Ambas se saltan automáticamente si
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
  del entorno en vez de una garantía dura.

- **Migración a Flink, resuelta de punta a punta**: dado lo anterior, se migró el runner por defecto de
  DirectRunner a **Flink** (`--runner=flink`, `PortableRunner` de Beam sobre un cluster real
  `jobmanager`+`taskmanager`, con el job server oficial `beam_flink_job_server`; ver `docker-compose.yml` y
  `beam_pipeline/pipeline.py`). **Estado final verificado**: streaming real y continuo contra Kafka, con datos
  fluyendo sin interrupción por las cuatro etapas hasta `fraud.alerts` (corrida de referencia: 130 eventos
  publicados, 117 procesados, 46 alertas distintas materializadas con las 4 reglas disparando, job estable sin
  caídas). Llegar a ese estado requirió resolver, en orden, los siguientes problemas reales:

  1. **Anuncio de host del job server**: por defecto anuncia `localhost` como su propio host, lo que rompe la
     conexión entre contenedores — corregido con `--job-host`.
  2. **Permisos del socket de Docker**: el usuario sin privilegios de la imagen oficial de Flink no puede usar
     el socket de Docker montado, aun arrancando el contenedor como root, porque el propio entrypoint de Flink
     baja privilegios internamente — corregido con un wrapper de entrypoint que ajusta permisos antes de esa
     caída (`flink/taskmanager-entrypoint.sh`).
  3. **CLI de Docker faltante**: la imagen oficial de Flink no trae el CLI de `docker`, necesario para que el
     TaskManager lance los workers del SDK harness como contenedores — corregido copiándolo desde la imagen
     oficial de Docker (`flink/taskmanager.Dockerfile`).
  4. **Alineación de red host entre Flink, Beam y los SDK Harness**: el TaskManager lanza los workers del SDK
     harness como contenedores hermanos con `--network=host`, por lo que los componentes críticos de Beam/Flink
     se configuraron también con `network_mode: host` y endpoints `localhost`. En el entorno de entrega se
     verificó explícitamente esta configuración con **Windows + WSL2 + Docker Desktop**: Flink respondió en
     `127.0.0.1:8081`, el Beam Job Server en `127.0.0.1:8099`, los SDK Harness establecieron sus canales y Kafka
     quedó accesible para Beam/Flink mediante `localhost:29092`.

  5. **Almacenamiento de artefactos compartido**: el job server y el TaskManager son contenedores separados,
     cada uno con su propio `/tmp/beam-artifact-staging` local — sin compartirlo, el TaskManager no encuentra
     los artefactos (paquete Python, dependencias) que el job server dejó ahí al recibir el job
     (`FileNotFoundException`). Corregido con un volumen nombrado montado en ambos servicios.
  6. **Fuga de Metaspace y de memoria off-heap por reenvíos repetidos**: con el TaskManager corriendo por un
     tiempo prolongado y recibiendo múltiples reenvíos del mismo job (cada reintento carga el código del
     pipeline en un *classloader* nuevo, que no se libera del todo), la JVM del TaskManager eventualmente
     agotaba primero el Metaspace y después la memoria off-heap de Netty/gRPC (`OutOfMemoryError`), tumbando el
     TaskManager entero. Corregido dándole más margen explícito a ambos
     (`taskmanager.memory.jvm-metaspace.size`, `taskmanager.memory.framework/task.off-heap.size` en
     `docker-compose.yml`) — mitigación práctica, no una solución estructural al *classloader leak* en sí.
  7. **Inestabilidad del entorno host durante el desarrollo (fuera del pipeline en sí, pero real)**: durante el
     diagnóstico en vivo, el entorno se caía repetidamente por dos causas ajenas a Beam/Flink: WSL2 apagando la
     instancia de la distro por inactividad entre comandos (mitigado manteniendo una sesión de WSL adjunta de
     forma persistente) y Windows suspendiendo el equipo por inactividad de teclado/mouse — con temporizadores
     de suspensión **separados e independientes** para alimentación por cable (AC) y por batería (DC), ambos
     debieron desactivarse (`powercfg /change standby-timeout-ac 0` y `...-dc 0`) para sostener una corrida de
     varios minutos sin interrupción.

  Con los siete puntos anteriores resueltos, el job llegaba a `RUNNING` con las 6 tareas corriendo de forma
  estable — pero **sin emitir ningún dato**: el operador de lectura de Kafka quedaba con ~92% de uso de CPU
  sostenido y cero registros de salida indefinidamente, sin ningún error. La causa raíz, encontrada recién ahí,
  no era de infraestructura sino del propio pipeline:

  - **Bug de coder en el cruce Python↔Java**: los pasos `Encode*` (justo antes de cada `WriteToKafka`, que
    convierten el evento a una tupla `(bytes, bytes)`) no declaraban explícitamente su tipo de salida. Sin esa
    pista, Beam infiere un coder de Python (pickle) para la tupla en lugar del coder portable de bytes que el
    lado Java de `KafkaIO.WriteToKafka` necesita para decodificar. En **DirectRunner** esto se manifestaba como
    un `CoderException` explícito y legible (`UnknownCoderWrapper was used to perform an actual decoding in
    the Java SDK`); en **Flink**, el mismo problema de fondo se manifestaba de forma silenciosa — el operador
    quedaba "vivo" pero nunca lograba emitir el primer elemento. Corregido declarando `with_output_types` en
    los tres pasos `Encode*` (`beam_pipeline/pipeline.py`). Este bug no tenía ninguna cobertura de pruebas
    (ver la nueva prueba dirigida en la sección 5); con el fix aplicado, ambos runners completan el recorrido
    real de punta a punta.

- Los datos son sintéticos; la lógica no reemplaza un sistema real de prevención de fraude.
- La primera versión procesa únicamente `PYG`; no incluye conversión de monedas.
- La deduplicación cubre un horizonte acotado de 10 minutos, no una garantía global.
- El entorno local (Docker Compose, un solo broker, un TaskManager con 2 slots) no representa disponibilidad,
  seguridad ni capacidad de producción.
- El primer arranque del contenedor `beam_pipeline` requiere acceso a internet: Beam descarga el *expansion
  service* Java de `KafkaIO` (cross-language) desde Maven Central la primera vez. Se cachea en un volumen
  Docker (`beam_expansion_cache`) para no repetir la descarga en arranques posteriores. Lo mismo aplica a las
  imágenes de Flink y del job server de Beam en el primer `docker compose up`.
- La calidad de las alertas depende de umbrales fijados para la demostración (sección de reglas), no de un
  análisis estadístico de datos reales.
- El *classloader leak* del TaskManager ante reenvíos repetidos del job (ver sección 6, punto 6) se mitiga con
  más memoria, no se resuelve de forma estructural: un TaskManager que reciba decenas de reenvíos en su vida
  útil eventualmente volvería a agotar el margen dado. En un despliegue real, el TaskManager se reciclaría
  entre reintentos en vez de acumularlos indefinidamente.
- Todas las corridas del pipeline (Flink o DirectRunner) usan por defecto el mismo `--consumer-group`
  (`beam-pipeline`); dos corridas simultáneas contra el mismo cluster de Kafka se reparten las particiones
  entre sí sin aviso, en vez de leer cada una el flujo completo. No afecta a una corrida a la vez (el caso de
  uso normal), pero hay que pasar un `--consumer-group` distinto si se quiere correr más de un runner al mismo
  tiempo contra el mismo Kafka.
- **Mejoras posibles** (fuera del alcance de esta entrega): reciclado del TaskManager entre reintentos en vez
  de mitigar la fuga con más memoria, dashboard de métricas, análisis de distribución/skew de claves con mayor
  volumen, reglas configurables, y triggers tempranos si el caso de uso lo justificara.
