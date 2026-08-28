# Imagen del TaskManager de Flink + Docker CLI.
#
# Beam PortableRunner con environment_type=DOCKER necesita que el
# TaskManager pueda invocar Docker mediante /var/run/docker.sock.

FROM docker:26-cli AS dockercli

FROM flink:1.17-scala_2.12-java11

USER root

COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker

COPY flink/taskmanager-entrypoint.sh /taskmanager-entrypoint.sh

# Windows puede guardar .sh como CRLF.
# Se normaliza a LF antes de utilizarlo como ENTRYPOINT.
RUN sed -i 's/\r$//' /taskmanager-entrypoint.sh \
    && chmod +x /taskmanager-entrypoint.sh

ENTRYPOINT ["/taskmanager-entrypoint.sh"]