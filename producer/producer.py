"""Productor sintetico de transacciones.

Publica eventos JSON en el topico `transactions.raw`, usando `key` (tarjeta
tokenizada) como clave de particionamiento Kafka. Es determinista: el mismo
--seed y --profiles generan siempre la misma secuencia, para poder repetir
un escenario en la demo o en el smoke test end-to-end.

Uso:
    python producer.py --profiles normal,duplicates,late,alerting --count 40 --seed 7
"""

import os
import argparse
import json
import logging
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from profiles import PROFILES

logging.basicConfig(level=logging.INFO, format="%(asctime)s producer %(levelname)s %(message)s")
log = logging.getLogger("producer")


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=50,
    )


def run(args: argparse.Namespace) -> None:
    producer = build_producer(args.bootstrap_servers)
    profile_names = [p.strip() for p in args.profiles.split(",") if p.strip()]
    unknown = [p for p in profile_names if p not in PROFILES]
    if unknown:
        raise SystemExit(f"Perfiles desconocidos: {unknown}. Disponibles: {list(PROFILES)}")

    rng = random.Random(args.seed)
    start = datetime.now(timezone.utc)

    sent = 0
    for name in profile_names:
        log.info("Iniciando perfil '%s' (%d eventos)", name, args.count)
        generator = PROFILES[name](rng, args.count, start)
        for delay, event in generator:
            if args.speed > 0:
                time.sleep(delay / args.speed)
            producer.send(args.topic, key=event["key"], value=event)
            sent += 1
            log.info(
                "perfil=%s event_id=%s key=%s event_time=%s amount=%s",
                name, event["event_id"], event["key"], event["event_time"], event["amount"],
            )
        producer.flush()
        log.info("Perfil '%s' completo.", name)

    log.info("Total de eventos publicados: %d", sent)
    producer.flush()
    producer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    #parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    parser.add_argument("--topic", default="transactions.raw")
    parser.add_argument("--profiles", default="normal", help="Lista separada por comas: normal,duplicates,late,alerting")
    parser.add_argument("--count", type=int, default=30, help="Eventos por perfil")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--speed", type=float, default=4.0, help="Factor de aceleracion del ritmo de envio (1=tiempo real)")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
