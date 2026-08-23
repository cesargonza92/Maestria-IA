#!/usr/bin/env bash
set -euo pipefail

PROFILES="${1:-normal,duplicates,late,alerting}"
COUNT="${2:-30}"
SEED="${3:-42}"

echo "Publicando eventos (perfiles: $PROFILES)..."
docker compose --profile tools run --rm producer \
    --bootstrap-servers=kafka:9092 --profiles="$PROFILES" --count="$COUNT" --seed="$SEED"

# beam_pipeline corre como micro-lotes acotados (ver docs/documento_tecnico.md,
# seccion de limites): cada arranque de un micro-lote tarda 1-2 minutos en
# establecer el canal cross-language antes de poder leer, y luego lee durante
# max_read_time (90s por defecto). En el peor caso, los eventos recien
# publicados llegan justo despues de que el micro-lote en curso ya cerro su
# lectura, y hay que esperar al siguiente ciclo completo.
echo
echo "Esperando a que un micro-lote del pipeline procese los eventos (~3 min, ver limites en docs/documento_tecnico.md)..."
sleep 180

echo "Consumiendo fraud.alerts e invalid.events (20s de inactividad para cerrar)..."
docker compose --profile tools run --rm consumer \
    --bootstrap-servers=kafka:9092 --timeout-ms=20000
