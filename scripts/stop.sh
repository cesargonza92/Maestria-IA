#!/usr/bin/env bash
set -euo pipefail

echo "Deteniendo y eliminando los contenedores (se conserva el volumen de cache de Beam)..."
docker compose down
