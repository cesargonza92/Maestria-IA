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

- Docker y Docker Compose v2 (`docker compose version`), con soporte real de red tipo host: Linux nativo, o
  **WSL2 con Docker Engine nativo instalado dentro de la distro** (no Docker Desktop — ver "Corrida con Flink
  en Windows" más abajo). Docker Desktop para Windows/Mac no implementa host networking real y el runner de
  Flink no completa el recorrido en vivo sobre él; `--runner=direct` sí funciona igual en cualquier entorno.
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

Verificar el cluster de Flink: abrir **http://localhost:8081** — debería verse 1 TaskManager registrado. El
estado del job sometido se ve ahí mismo, o con:

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
pytest tests/e2e -v
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

Docker Desktop no soporta host networking real, que el runner de Flink necesita (ver Prerrequisitos). Para
reproducir la corrida de Flink en Windows: instalar una distro WSL2 dedicada con Docker Engine nativo (**no**
Docker Desktop) —

```powershell
wsl --install -d Ubuntu-24.04
```

— y dentro de esa distro instalar Docker Engine siguiendo la [guía oficial para Ubuntu](https://docs.docker.com/engine/install/ubuntu/),
clonar/montar este repo (accesible en `/mnt/c/...`) y correr los comandos de este README desde ahí. `--runner=direct`
no tiene esta limitación y corre igual sobre Docker Desktop.

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
- Runner de Beam: **Flink** por defecto (streaming real, `PortableRunner`, verificado de punta a punta), con
  **DirectRunner** disponible como alternativa liviana (`--runner=direct`). Flink requiere Docker con soporte
  real de red tipo host — ver "Prerrequisitos" y "Corrida con Flink en Windows" arriba, y la sección de
  límites del documento técnico para el diagnóstico completo del camino hasta el estado actual.

## Integrantes y contribuciones

| Integrante | Contribución principal |
|---|---|
| Graciela Lezcano ([@graclez](https://github.com/graclez)) | Diseño e implementación completa: contrato de eventos, productor sintético, pipeline de Beam (validación, gate de tardanza, deduplicación, ventanas, reglas), consumidor de demostración, infraestructura Docker, pruebas (unitarias, `TestStream`, e2e) y documentación. |
| César Gonzalez ([@cesargonza92](https://github.com/cesargonza92)) | *(completar!!)* |
