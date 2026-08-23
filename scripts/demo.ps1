param(
    [string]$Profiles = "normal,duplicates,late,alerting",
    [int]$Count = 30,
    [int]$Seed = 42
)

Write-Host "Publicando eventos (perfiles: $Profiles)..."
docker compose --profile tools run --rm producer `
    --bootstrap-servers=kafka:9092 --profiles=$Profiles --count=$Count --seed=$Seed

# beam_pipeline corre como micro-lotes acotados (ver docs/documento_tecnico.md,
# seccion de limites): cada arranque de un micro-lote tarda 1-2 minutos en
# establecer el canal cross-language antes de poder leer, y luego lee durante
# max_read_time (90s por defecto). En el peor caso, los eventos recien
# publicados llegan justo despues de que el micro-lote en curso ya cerro su
# lectura, y hay que esperar al siguiente ciclo completo.
Write-Host ""
Write-Host "Esperando a que un micro-lote del pipeline procese los eventos (~3 min, ver limites en docs/documento_tecnico.md)..."
Start-Sleep -Seconds 180

Write-Host "Consumiendo fraud.alerts e invalid.events (20s de inactividad para cerrar)..."
docker compose --profile tools run --rm consumer `
    --bootstrap-servers=kafka:9092 --timeout-ms=20000
