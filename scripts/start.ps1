Write-Host "Levantando Kafka, creando topicos y arrancando el pipeline de Beam..."
docker compose up -d --build kafka kafka-init beam_pipeline
docker compose logs kafka-init --no-log-prefix
Write-Host ""
Write-Host "Entorno listo. beam_pipeline corre en segundo plano (ver: docker compose logs -f beam_pipeline)."
Write-Host "Para producir eventos y ver alertas: scripts\demo.ps1"
