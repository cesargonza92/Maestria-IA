#!/bin/sh
# El entrypoint original de Flink baja privilegios al usuario "flink" antes
# de arrancar el TaskManager, aunque el contenedor arranque como root (ver
# "user: root" en docker-compose.yml) -- por eso el proceso que realmente
# lanza los contenedores del SDK harness (environment_type=DOCKER) no
# hereda el acceso a /var/run/docker.sock. Este wrapper corre ANTES de esa
# caida de privilegios, mientras el proceso todavia es root, y abre el
# socket para que el usuario sin privilegios tambien pueda usarlo.
set -e
if [ -S /var/run/docker.sock ]; then
    chmod 666 /var/run/docker.sock || true
fi
exec /docker-entrypoint.sh "$@"
