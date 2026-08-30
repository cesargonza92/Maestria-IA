# Proyecto Integrador --- Data Streaming (Kafka + Apache Beam + Apache Flink)

**Maestría en Inteligencia Artificial --- FPUNA**\
Materia: *Streaming de datos y sus aplicaciones* · Docente: Rodrigo
Parra, M.Sc.\
Integrantes: **Graciela Lezcano** · **César Gonzalez**

## Descripción

Detección de patrones sospechosos en transacciones de pago
**sintéticas** (frecuencia, monto acumulado, multi-país y
multi-comercio), mediante un pipeline de streaming end-to-end:

**productor → Kafka → Apache Beam → Apache Flink → Kafka → consumidor**

Apache Beam implementa la lógica de procesamiento y Apache Flink actúa
como runner distribuido mediante `PortableRunner`.

La documentación técnica completa se encuentra en
[`docs/documento_tecnico.md`](docs/documento_tecnico.md) y el diagrama
de arquitectura en [`docs/arquitectura.md`](docs/arquitectura.md).

## Prerrequisitos

-   Docker y Docker Compose v2.
-   Windows + WSL2 + Docker Desktop para el entorno validado.
-   Soporte de `network_mode: host` para Beam/Flink.
-   Acceso a Internet durante el primer arranque.
-   Python 3.11 para ejecutar las pruebas localmente.
-   `pip install -r requirements.txt`.

> Entorno validado: Python 3.11.9, Apache Beam 2.61.0, Apache Flink 1.17
> y Apache Kafka 3.7.0.

## Estructura del repositorio

``` text
contracts/        Contrato de eventos (JSON Schema)
producer/         Productor sintético
beam_pipeline/    Pipeline Apache Beam
flink/            Configuración e imagen del TaskManager de Flink
consumer/         Consumidor de demostración
tests/unit/       Pruebas unitarias
tests/streaming/  Pruebas de ventanas y tiempo de evento
tests/e2e/        Pruebas end-to-end
data/             Escenarios de ejemplo
docs/             Documento técnico y arquitectura
scripts/          Scripts de inicio, demo y detención
```

## Arquitectura de ejecución

El pipeline utiliza Apache Beam con `PortableRunner` sobre Apache Flink.
Los componentes Beam/Flink y los SDK Harness de Python y Java creados
dinámicamente utilizan red host.

Kafka mantiene dos listeners:

  -----------------------------------------------------------------------
  Cliente                             Bootstrap server
  ----------------------------------- -----------------------------------
  `producer`, `consumer` y            `kafka:9092`
  `kafka-init` dentro de Docker       
  Compose                             

  Beam/Flink y SDK Harness con red    `localhost:29092`
  host                                
  -----------------------------------------------------------------------

Endpoints principales:

  Componente                      Endpoint
  ------------------------------- -------------------
  Kafka desde Beam/Flink          `localhost:29092`
  Kafka desde servicios Compose   `kafka:9092`
  Flink JobManager / REST         `localhost:8081`
  Beam Flink Job Server           `localhost:8099`

### Variables de entorno

Para el pipeline Beam:

``` text
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
FLINK_JOB_ENDPOINT=localhost:8099
KAFKA_GROUP_ID=beam-pipeline
```

Para productor y consumidor:

``` text
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```

Los valores definidos mediante variables de entorno tienen prioridad
sobre los valores por defecto de los scripts Python.

## Tópicos Kafka

  Tópico                       Particiones Uso
  -------------------------- ------------- ----------------------------------
  `transactions.raw`                     3 Eventos de entrada
  `transactions.processed`               3 Transacciones válidas procesadas
  `fraud.alerts`                         3 Alertas generadas por las reglas
  `invalid.events`                       3 Eventos inválidos o rechazados

El factor de replicación utilizado localmente es **1** y `fraud.alerts`
utiliza `cleanup.policy=compact`.

## Inicio del entorno

``` powershell
docker compose up -d --build
```

Verificar los contenedores:

``` powershell
docker compose ps
```

Verificar los tópicos:

``` powershell
docker compose logs kafka-init --no-log-prefix
```

Verificar Flink:

``` powershell
docker exec flink-jobmanager curl -s http://localhost:8081/overview
docker exec flink-jobmanager curl -s http://localhost:8081/jobs/overview
```

En una ejecución correcta el job debe encontrarse en estado `RUNNING`,
sin tareas fallidas.

Logs del pipeline:

``` powershell
docker compose logs -f beam_pipeline
```

## Demostración

La demostración principal puede ejecutarse mediante:

``` powershell
scripts\demo.ps1
```

También puede realizarse manualmente.

### 1. Verificar el job de Flink

``` powershell
docker exec flink-jobmanager curl -s http://localhost:8081/jobs/overview
```

### 2. Generar eventos

``` powershell
docker compose --profile tools run --rm producer `
  --bootstrap-servers=kafka:9092 `
  --profiles=normal,duplicates,late,alerting `
  --count=10 `
  --seed=7
```

### 3. Verificar entrada

``` powershell
docker exec kafka `
  /opt/kafka/bin/kafka-get-offsets.sh `
  --bootstrap-server kafka:9092 `
  --topic transactions.raw
```

### 4. Mostrar eventos procesados

``` powershell
docker exec kafka `
  /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic transactions.processed `
  --from-beginning `
  --max-messages 5 `
  --timeout-ms 10000
```

### 5. Mostrar alertas

``` powershell
docker exec kafka `
  /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic fraud.alerts `
  --from-beginning `
  --max-messages 5 `
  --timeout-ms 10000
```

### 6. Mostrar eventos inválidos

``` powershell
docker exec kafka `
  /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic invalid.events `
  --from-beginning `
  --max-messages 5 `
  --timeout-ms 10000
```

## Pruebas

### Unitarias y comportamiento temporal

``` powershell
python -m pytest tests/unit tests/streaming -v
```

### End-to-end

Con la infraestructura levantada:

``` powershell
pytest .\tests\e2e\test_end_to_end.py -v -s
```

Resultado validado para la versión final:

``` text
tests/e2e/test_end_to_end.py::test_normal_duplicate_late_and_invalid_events_flow_end_to_end PASSED
tests/e2e/test_end_to_end.py::test_writes_to_all_three_output_topics_succeed_without_a_coder_exception PASSED

2 passed in 92.10s
```

La primera prueba verifica eventos normales, duplicados, tardíos e
inválidos. La segunda verifica la escritura hacia los tres tópicos de
salida y actúa como regresión frente a errores de codificación en la
frontera Beam Python/KafkaIO.

## Validación funcional final

Se publicó un lote de **30 transacciones sintéticas** con el perfil
`normal`.

Entrada:

``` text
transactions.raw:0:0
transactions.raw:1:17
transactions.raw:2:13
```

Total: **30 eventos**.

Salida procesada:

``` text
transactions.processed:0:13
transactions.processed:1:11
transactions.processed:2:6
```

Total: **30 eventos procesados**.

Alertas:

``` text
fraud.alerts:0:4
fraud.alerts:1:4
fraud.alerts:2:3
```

Total: **11 alertas**.

Eventos inválidos para esta corrida:

``` text
invalid.events:0:0
invalid.events:1:0
invalid.events:2:0
```

El JobManager informó:

``` text
state = RUNNING
running = 6
total = 6
initializing = 0
failed = 0
```

El recorrido validado fue:

``` text
Producer
   |
   v
transactions.raw
   |
   v
Apache Beam
   |
   v
Apache Flink
   |
   +--> transactions.processed
   +--> fraud.alerts
   +--> invalid.events
```

## Estabilización del TaskManager

Durante la integración se identificaron problemas de memoria
directa/off-heap en el TaskManager:

``` text
java.lang.OutOfMemoryError: Direct buffer memory
```

La configuración final reserva explícitamente memoria:

``` yaml
taskmanager.memory.process.size: 4096m
taskmanager.memory.framework.off-heap.size: 512m
taskmanager.memory.task.off-heap.size: 512m

classloader.check-leaked-classloader: false

restart-strategy.type: fixed-delay
restart-strategy.fixed-delay.attempts: 5
restart-strategy.fixed-delay.delay: 10 s
```

Esta configuración permitió estabilizar la integración Flink + Beam +
gRPC/Netty.

Para revisar el estado:

``` powershell
docker compose ps
docker exec flink-jobmanager curl -s http://localhost:8081/overview
docker exec flink-jobmanager curl -s http://localhost:8081/jobs/overview
```

Ante una caída del TaskManager:

``` powershell
docker compose logs --tail 200 taskmanager
```

## DirectRunner

Flink es el runner principal. DirectRunner se mantiene como alternativa
para pruebas locales:

``` powershell
docker compose run --rm beam_pipeline `
  --bootstrap-servers=kafka:9092 `
  --runner=direct `
  --max-read-time-seconds=90 `
  --consumer-group=demo-direct
```

Si Flink y DirectRunner se ejecutan simultáneamente deben utilizar
grupos de consumidores diferentes.

## Detener el entorno

``` powershell
docker compose down
```

o:

``` powershell
scripts\stop.ps1
```

Para eliminar también los volúmenes:

``` powershell
docker compose down -v
```

## Notas de reproducibilidad

-   El productor permite utilizar un `seed` fijo para repetir
    escenarios.
-   `kafka-init` crea automáticamente los cuatro tópicos.
-   Los cuatro tópicos utilizan 3 particiones y factor de replicación 1.
-   Flink es el runner principal del proyecto.
-   La solución fue validada end-to-end sobre Windows + WSL2 + Docker
    Desktop.
-   El pipeline utiliza claves estables para permitir materialización
    idempotente de alertas.

## Integrantes y contribuciones

  ----------------------------------------------------------------------------------------
  Integrante                                           Contribución principal
  ---------------------------------------------------- -----------------------------------
  Graciela Lezcano                                     Diseño e implementación del
  ([@graclez](https://github.com/graclez))             contrato de eventos, productor
                                                       sintético, lógica del pipeline de
                                                       Apache Beam (validación, tardanza,
                                                       deduplicación, ventanas y reglas),
                                                       consumidor de demostración y
                                                       pruebas funcionales.

  César Gonzalez                                       Integración y validación de la
  ([@cesargonza92](https://github.com/cesargonza92))   arquitectura end-to-end Kafka +
                                                       Apache Beam + Flink; configuración
                                                       y estabilización del entorno
                                                       Docker/WSL2; diagnóstico de
                                                       conectividad entre Job Server,
                                                       JobManager, TaskManager y SDK
                                                       Harness; ajuste y ejecución de
                                                       pruebas E2E; validación del flujo
                                                       de eventos y tópicos Kafka;
                                                       documentación técnica, pruebas de
                                                       reproducibilidad y preparación de
                                                       la versión final entregable.
  ----------------------------------------------------------------------------------------
