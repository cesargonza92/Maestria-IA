# Proyecto Integrador — Data Streaming (Kafka + Apache Beam)

**Maestría en Inteligencia Artificial — FPUNA**
Materia: *Streaming de datos y sus aplicaciones* · Docente: Rodrigo Parra, M.Sc.
Integrantes: **Graciela Lezcano** · **César Gonzalez**


Detección de patrones sospechosos en transacciones de pago **sintéticas** (frecuencia, monto acumulado,
multi-país, multi-comercio), con un pipeline end-to-end: **productor → Kafka → Apache Beam → Kafka →
consumidor**. La propuesta completa está en [`docs/documento_tecnico.md`](docs/documento_tecnico.md) (caso de
uso, contrato de eventos, tópicos, ventanas, deduplicación, semántica de entrega y límites conocidos) y el
diagrama en [`docs/arquitectura.md`](docs/arquitectura.md).

## Prerrequisitos

- Docker y Docker Compose v2 (`docker compose version`). La configuración final usa `network_mode: host`
  para los componentes Beam/Flink. En el entorno validado del proyecto, **Windows + WSL2 + Docker Desktop**
  soportó correctamente esta configuración; se verificó la conectividad host antes de ejecutar el pipeline.
- Acceso a internet en el primer arranque: Beam descarga el *expansion service* de KafkaIO desde Maven Central
  (se cachea en un volumen), y Docker descarga las imágenes de Flink y del job server de Beam (~1.5GB).
- Para correr los tests localmente sin Docker: Python 3.11 y `pip install -r requirements.txt`.

## Estructura del repositorio

```
contracts/        Contrato de eventos (JSON Schema)
producer/          Productor sintético (perfiles: normal, duplicates, late, alerting)
beam_pipeline/      Pipeline de Apache Beam (validación, dedup, ventanas, reglas, KafkaIO)
flink/              Imagen del TaskManager de Flink (runtime para el runner de streaming)
consumer/           Consumidor de demostración
tests/unit/         Pruebas unitarias (validación, reglas, dedup)
tests/streaming/    Pruebas de ventanas/tiempo de evento con TestStream
tests/e2e/          Smoke test end-to-end contra el entorno real
data/               Escenario de ejemplo (no sensible)
docs/               Documento técnico y diagrama de arquitectura
scripts/            Comandos de inicio, demo, detención y análisis de distribución de claves
```

## Inicio

```bash
docker compose up -d --build
```

Esto levanta Kafka, crea los tópicos, levanta el cluster de Flink (`jobmanager` + `taskmanager`) y el job
server de Beam, y somete el pipeline (`beam_pipeline`). El primer arranque tarda varios minutos (descarga las
imágenes de Flink y del job server, ~1.5GB en total). O usando el script equivalente:

```bash
scripts/start.sh
```

En Windows/PowerShell:

```powershell
scripts\start.ps1
```

Verificar que los tópicos se crearon:

```bash
docker compose logs kafka-init --no-log-prefix
```

Verificar primero el estado del cluster de Flink mediante la API REST del JobManager:

```bash
docker exec flink-jobmanager curl -s http://localhost:8081/overview
```

La respuesta debe mostrar al menos un TaskManager registrado y slots disponibles o asignados. El estado de los jobs puede consultarse con:

```bash
docker exec flink-jobmanager curl -s http://localhost:8081/jobs/overview
```

El dashboard web en **http://localhost:8081** puede utilizarse cuando el endpoint REST esté accesible desde el host. La validación del cluster no depende exclusivamente del acceso al dashboard. Los logs del pipeline también pueden consultarse con:

```bash
docker compose logs -f beam_pipeline
```

## Pruebas

Unitarias y de ventanas/tiempo de evento (no requieren Docker ni Kafka):

```bash
pip install -r requirements.txt
pytest tests/unit tests/streaming -v
```

Smoke test end-to-end (requiere el entorno levantado, ver "Inicio"):

```bash
pip install -r requirements.txt
python -m pytest tests/e2e -v
```
## Validación final

La versión final del proyecto fue validada ejecutando el flujo completo de streaming sobre Apache Flink, Apache Beam y Kafka.

### Entorno validado

- **Sistema operativo:** Windows con WSL2.
- **Contenedores:** Docker Desktop + Docker Compose v2.
- **Python:** 3.11.9.
- **Apache Beam:** 2.61.0.
- **Apache Flink:** 1.17.
- **Apache Kafka:** 3.7.0.
- **Runner principal:** Flink mediante Apache Beam `PortableRunner`.
- **Entorno de los SDK Harness:** `DOCKER`.

La arquitectura final utiliza `network_mode: host` para los componentes de ejecución de Beam/Flink. En el entorno utilizado para la entrega se verificó correctamente la comunicación entre:

- `beam_pipeline`
- `beam_flink_job_server`
- Flink `jobmanager`
- Flink `taskmanager`
- SDK Harness de Python
- SDK Harness de Java
- Kafka

Los principales endpoints utilizados durante la validación fueron:

| Componente | Endpoint |
|---|---|
| Kafka desde Beam/Flink | `localhost:29092` |
| Flink JobManager / REST | `localhost:8081` (interno; acceso desde host dependiente del entorno) |
| Beam Flink Job Server | `localhost:8099` |

### Pruebas ejecutadas

Se validaron las pruebas unitarias, las pruebas de comportamiento temporal mediante `TestStream` y las pruebas end-to-end contra la infraestructura real.

```powershell
python -m pytest tests/unit tests/streaming -v
python -m pytest tests/e2e -v
```

## Demostración

Publica un lote con los 4 perfiles (normal, duplicados, tardíos/desordenados y uno que dispara las 4 reglas
deliberadamente) y muestra el resultado materializado:

```bash
scripts/demo.sh
```

```powershell
scripts\demo.ps1
```

Esto ejecuta, en orden:

1. `producer` (perfil `normal,duplicates,late,alerting`, seed fijo — reproducible).
2. Una espera para que el pipeline (streaming real sobre Flink) procese y publique los resultados.
3. `consumer`, que imprime cada alerta nueva/actualizada y un resumen final (alertas distintas materializadas
   por `alert_id`, eventos inválidos/descartados recibidos).

Para correr manualmente con otros parámetros:

```bash
docker compose --profile tools run --rm producer --bootstrap-servers=kafka:9092 --profiles=late --count=20 --seed=7
docker compose --profile tools run --rm consumer --bootstrap-servers=kafka:9092 --timeout-ms=20000
```

Para verificar cómo se distribuyeron las tarjetas producidas entre las particiones de `transactions.raw`
(mensajes, tarjetas distintas y orden por partición — ver `docs/documento_tecnico.md`, sección 3):

```bash
pip install -r requirements.txt
python scripts/analyze_partition_skew.py --bootstrap-servers=localhost:29092
```

Para correr el pipeline con DirectRunner en vez de Flink (más simple, sin cluster; funciona en cualquier
entorno, incluido Docker Desktop):

```bash
docker compose run --rm beam_pipeline --bootstrap-servers=kafka:9092 --runner=direct --max-read-time-seconds=90 --consumer-group=demo-direct
```

> Si vas a tener corriendo Flink y una corrida manual de DirectRunner al mismo tiempo contra el mismo Kafka,
> usá un `--consumer-group` distinto en cada una (como en el ejemplo) — comparten el mismo valor por defecto
> (`beam-pipeline`), y dos corridas con el mismo grupo se reparten las particiones entre sí en vez de leer cada
> una el flujo completo.

Logs del pipeline en vivo (eventos válidos/inválidos, duplicados descartados, eventos fuera de política,
alertas emitidas):

```bash
docker compose logs -f beam_pipeline
```

### Corrida con Flink en Windows

La configuración final fue validada en **Windows + WSL2 + Docker Desktop**. `jobmanager`, `taskmanager`,
`beam_flink_job_server` y `beam_pipeline` usan `network_mode: host`; los SDK Harness efímeros lanzados por el
TaskManager también usan red host. Esto permite que los endpoints internos de Beam/Flink se resuelvan mediante
`localhost`.

Con el entorno levantado, la API REST de Flink se verifica desde el JobManager con
`docker exec flink-jobmanager curl -s http://localhost:8081/overview`. El acceso directo al dashboard desde
Windows puede depender de la exposición de red de Docker Desktop. El Beam Flink Job Server utiliza
`localhost:8099` y Kafka queda accesible para Beam/Flink en `localhost:29092`.

> Esta validación corresponde al entorno usado para la entrega. En otro host o versión de Docker conviene
> verificar explícitamente el soporte de `--network host`.

## Estabilización del TaskManager

Durante ejecuciones prolongadas se observó una finalización del TaskManager con código de salida `239`, sin indicios de terminación por falta de memoria (`OOMKilled=false`). Los logs mostraron accesos tardíos a un classloader ya cerrado en la interacción Flink + Beam/gRPC/Log4j.

La configuración actual incorpora:

```yaml
classloader.check-leaked-classloader: false
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 5
restart-strategy.fixed-delay.delay: 10 s
```

La política limita los redeploys indefinidos y evita que la comprobación de classloaders de Flink termine el TaskManager por accesos tardíos de componentes de terceros. Esta configuración se considera un ajuste de estabilización del entorno local; las ejecuciones prolongadas deben seguir verificándose operacionalmente.

Para comprobar el estado:

```powershell
docker compose ps
docker exec flink-jobmanager curl -s http://localhost:8081/overview
docker exec flink-jobmanager curl -s http://localhost:8081/jobs/overview
```

Si `taskmanagers` es `0`, se debe revisar `docker compose logs taskmanager` antes de continuar con la demostración.

## Detener el entorno

```bash
docker compose down
```

```powershell
scripts\stop.ps1
```

(El volumen `beam_expansion_cache` se conserva entre corridas para no volver a descargar el expansion service
de Java. Para limpiarlo también: `docker compose down -v`.)

## Notas de reproducibilidad

- El productor es determinista: mismo `--seed` + mismo `--profiles` generan siempre la misma secuencia de
  eventos (ver `producer/profiles.py`), para poder repetir un escenario en la demo o en el smoke test.
- `kafka-init` crea los 4 tópicos automáticamente al levantar el entorno; no hay pasos manuales.
- Los cuatro tópicos (`transactions.raw`, `transactions.processed`, `fraud.alerts` e `invalid.events`) se crean con **3 particiones** y factor de replicación **1**; `fraud.alerts` utiliza `cleanup.policy=compact`.
- Runner de Beam: **Flink** por defecto (streaming real, `PortableRunner`, verificado de punta a punta), con
  **DirectRunner** disponible como alternativa liviana (`--runner=direct`). La configuración Flink usa red host
  y fue validada en Windows + WSL2 + Docker Desktop; ver "Corrida con Flink en Windows" y el documento técnico.

## Integrantes y contribuciones

| Integrante | Contribución principal |
|---|---|
| Graciela Lezcano ([@graclez](https://github.com/graclez)) | Diseño e implementación completa: contrato de eventos, productor sintético, pipeline de Beam (validación, gate de tardanza, deduplicación, ventanas, reglas), consumidor de demostración, infraestructura Docker, pruebas (unitarias, `TestStream`, e2e) y documentación. |
| César Gonzalez ([@cesargonza92](https://github.com/cesargonza92)) | Integración y validación de la arquitectura end-to-end Kafka + Apache Beam + Flink; configuración y estabilización del entorno Docker/WSL2; diagnóstico de conectividad entre Job Server, JobManager, TaskManager y SDK Harness; ajuste y ejecución de pruebas E2E; validación del flujo de eventos y tópicos Kafka; documentación técnica, pruebas de reproducibilidad y preparación de la versión final entregable. |