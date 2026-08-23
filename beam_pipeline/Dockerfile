FROM python:3.11-slim

# default-jdk-headless: requerido por Beam para levantar el expansion
# service Java de KafkaIO (cross-language). No se usa Java para nada mas.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY beam_pipeline/requirements.txt ./beam_pipeline/requirements.txt
RUN pip install --no-cache-dir -r beam_pipeline/requirements.txt

COPY contracts/ ./contracts/
COPY beam_pipeline/ ./beam_pipeline/

WORKDIR /app/beam_pipeline

ENTRYPOINT ["python", "pipeline.py"]
