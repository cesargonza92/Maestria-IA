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

- Docker y Docker Compose v2 (`docker compose version`).
- Acceso a internet en el primer arranque (Beam descarga el *expansion service* de KafkaIO desde Maven
  Central; queda cacheado en un volumen para arranques posteriores).
- Para correr los tests localmente sin Docker: Python 3.11 y `pip install -r requirements.txt`.

## Estructura del repositorio

```
contracts/        Contrato de eventos (JSON Schema)
producer/          Productor sintético (perfiles: normal, duplicates, late, alerting)
beam_pipeline/      Pipeline de Apache Beam (validación, dedup, ventanas, reglas, KafkaIO)
consumer/           Consumidor de demostración
tests/unit/         Pruebas unitarias (validación, reglas, dedup)
tests/streaming/    Pruebas de ventanas/tiempo de evento con TestStream
tests/e2e/          Smoke test end-to-end contra el entorno real
data/               Escenario de ejemplo (no sensible)
docs/               Documento técnico y diagrama de arquitectura
scripts/            Comandos de inicio, demo y detención
```

## Inicio

```bash
docker compose up -d --build kafka kafka-init beam_pipeline
```

O usando el script equivalente (crea los 4 tópicos y deja el pipeline corriendo en segundo plano):

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
2. Una espera de ~3 minutos para que un micro-lote del pipeline (ver "Notas de reproducibilidad") lea, procese
   y publique los resultados.
3. `consumer`, que imprime cada alerta nueva/actualizada y un resumen final (alertas distintas materializadas
   por `alert_id`, eventos inválidos/descartados recibidos).

Para correr manualmente con otros parámetros:

```bash
docker compose --profile tools run --rm producer --bootstrap-servers=kafka:9092 --profiles=late --count=20 --seed=7
docker compose --profile tools run --rm consumer --bootstrap-servers=kafka:9092 --timeout-ms=20000
```

Logs del pipeline en vivo (eventos válidos/inválidos, duplicados descartados, eventos fuera de política,
alertas emitidas):

```bash
docker compose logs -f beam_pipeline
```

**Nota**: se verificó empíricamente que la combinación KafkaIO cross-language + DirectRunner de Python puede,
en algunas corridas, quedarse sin avanzar justo después de "Creating state cache" (sin ningún error) — una
limitación conocida de este runner con transformaciones cross-language, documentada en
`docs/documento_tecnico.md`. Si `docker compose logs beam_pipeline` no muestra nueva actividad varios minutos
después de publicar eventos, reiniciá el micro-lote: `docker compose restart beam_pipeline`.

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
- Runner de Beam: **DirectRunner**. Flink no se implementó en esta entrega (ver límites en el documento
  técnico).

## Integrantes y contribuciones

| Integrante | Contribución principal |
|---|---|
| Graciela Lezcano ([@graclez](https://github.com/graclez)) | Diseño e implementación completa: contrato de eventos, productor sintético, pipeline de Beam (validación, gate de tardanza, deduplicación, ventanas, reglas), consumidor de demostración, infraestructura Docker, pruebas (unitarias, `TestStream`, e2e) y documentación. |
| César Gonzalez ([@cesargonza92](https://github.com/cesargonza92)) | *(completar a medida que avance su aporte)* |

Entrega grupal para la materia *Streaming de datos y sus aplicaciones* (Maestría en IA, FPUNA).
