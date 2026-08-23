#!/usr/bin/env bash
set -euo pipefail

echo "Levantando Kafka, creando topicos y arrancando el pipeline de Beam..."
docker compose up -d --build kafka kafka-init beam_pipeline
docker compose logs kafka-init --no-log-prefix

echo
echo "Entorno listo. beam_pipeline corre en segundo plano (ver: docker compose logs -f beam_pipeline)."
echo "Para producir eventos y ver alertas: scripts/demo.sh"
