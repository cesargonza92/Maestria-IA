"""Consumidor de demostracion.

Consume `fraud.alerts` e `invalid.events`, materializa en memoria el ultimo
valor visto por `alert_id` (simulando del lado del cliente lo que la
compactacion de Kafka hace del lado del broker) e imprime un resumen. Sirve
para verificar visualmente que un evento duplicado no produce una alerta
adicional y que un evento tardio dentro del horizonte permitido actualiza la
alerta existente en lugar de crear una nueva.
"""

import argparse
import json
import logging

from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s consumer %(levelname)s %(message)s")
log = logging.getLogger("consumer")


def run(args: argparse.Namespace) -> None:
    consumer = KafkaConsumer(
        args.alerts_topic,
        args.invalid_topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=args.group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=args.timeout_ms,
    )

    latest_alerts = {}
    invalid_reasons = []

    for message in consumer:
        if message.topic == args.alerts_topic:
            alert = message.value
            is_update = alert["alert_id"] in latest_alerts
            latest_alerts[alert["alert_id"]] = alert
            log.info(
                "%s alert_id=%s tipo=%s tarjeta=%s ventana=[%s,%s) monto_acum=%s paises=%s comercios=%s",
                "ACTUALIZA" if is_update else "NUEVA",
                alert["alert_id"], alert["alert_type"], alert["key"],
                alert["window_start"], alert["window_end"],
                alert["amount_sum"], alert["countries"], alert["merchant_count"],
            )
        elif message.topic == args.invalid_topic:
            invalid_reasons.append(message.value.get("reason", "sin_motivo"))
            log.info("INVALIDO/DESCARTADO motivo=%s", message.value.get("reason"))

    log.info("================ Resumen de la demostracion ================")
    log.info("Alertas distintas materializadas (por alert_id): %d", len(latest_alerts))
    for alert_id, alert in latest_alerts.items():
        log.info("  - %s -> conteo=%s monto=%s", alert_id, alert["transaction_count"], alert["amount_sum"])
    log.info("Eventos invalidos/descartados: %d", len(invalid_reasons))
    log.info("==============================================================")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--alerts-topic", default="fraud.alerts")
    parser.add_argument("--invalid-topic", default="invalid.events")
    parser.add_argument("--group-id", default="demo-consumer")
    parser.add_argument("--timeout-ms", type=int, default=20000, help="Inactividad maxima antes de cerrar (demo)")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
