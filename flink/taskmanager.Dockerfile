# Imagen del TaskManager de Flink + el CLI de Docker.
#
# El runner portable de Beam, con environment_type=DOCKER, hace que el
# TaskManager ejecute el binario `docker` (via el socket montado) para
# levantar un contenedor efimero por worker del SDK harness (tanto para los
# DoFn de Python como para el entorno Java nativo de KafkaIO). La imagen
# oficial de Flink no trae ese binario -- se copia el CLI estatico desde la
# imagen oficial de Docker (no se necesita el daemon, solo el CLI).
FROM docker:26-cli AS dockercli

FROM flink:1.17-scala_2.12-java11
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
COPY flink/taskmanager-entrypoint.sh /taskmanager-entrypoint.sh
RUN chmod +x /taskmanager-entrypoint.sh
ENTRYPOINT ["/taskmanager-entrypoint.sh"]
