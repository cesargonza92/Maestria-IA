"""Analiza la distribucion de claves (tarjetas) entre las particiones de
`transactions.raw`: cuantos mensajes y cuantas tarjetas distintas cayeron en
cada particion, y si el orden por tarjeta se conserva dentro de su
particion. Sirve como evidencia real para la justificacion de particiones
del documento tecnico (seccion 3), en vez de una declaracion sin medir.

Uso (requiere el entorno levantado y datos ya producidos en transactions.raw,
ver README):
    python scripts/analyze_partition_skew.py --bootstrap-servers=localhost:29092
"""

import argparse
from collections import Counter, defaultdict

from kafka import KafkaConsumer, TopicPartition


def analyze(bootstrap_servers: str, topic: str) -> None:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda v: v,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        raise SystemExit(f"El topico '{topic}' no existe o no tiene particiones.")

    topic_partitions = [TopicPartition(topic, p) for p in sorted(partitions)]
    consumer.assign(topic_partitions)
    consumer.seek_to_beginning()
    end_offsets = consumer.end_offsets(topic_partitions)

    messages_per_partition = Counter()
    keys_per_partition = defaultdict(set)
    order_violations = defaultdict(int)
    last_offset_seen = defaultdict(lambda: -1)

    for message in consumer:
        tp = TopicPartition(message.topic, message.partition)
        messages_per_partition[message.partition] += 1
        if message.key:
            keys_per_partition[message.partition].add(message.key)
        if message.offset <= last_offset_seen[tp]:
            order_violations[message.partition] += 1
        last_offset_seen[tp] = message.offset
        if all(last_offset_seen[tp] >= end_offsets[tp] - 1 for tp in topic_partitions if end_offsets[tp] > 0):
            break

    consumer.close()

    total_messages = sum(messages_per_partition.values())
    total_distinct_keys = len(set().union(*keys_per_partition.values())) if keys_per_partition else 0

    print(f"Topico: {topic}  ({len(topic_partitions)} particiones)")
    print(f"Total de mensajes: {total_messages}  |  Tarjetas distintas: {total_distinct_keys}")
    print()
    print(f"{'Particion':<10}{'Mensajes':<12}{'% del total':<14}{'Tarjetas':<12}{'Viol. de orden':<15}")
    for p in sorted(partitions):
        pct = (messages_per_partition[p] / total_messages * 100) if total_messages else 0
        print(
            f"{p:<10}{messages_per_partition[p]:<12}{pct:<14.1f}"
            f"{len(keys_per_partition[p]):<12}{order_violations[p]:<15}"
        )

    # Una tarjeta que aparezca en mas de una particion indicaria que el
    # particionamiento por clave no es estable (bug de configuracion, no
    # esperado con el particionador por defecto de Kafka).
    key_to_partitions = defaultdict(set)
    for p, keys in keys_per_partition.items():
        for k in keys:
            key_to_partitions[k].add(p)
    unstable_keys = {k: ps for k, ps in key_to_partitions.items() if len(ps) > 1}
    print()
    if unstable_keys:
        print(f"ALERTA: {len(unstable_keys)} tarjeta(s) aparecieron en mas de una particion: {unstable_keys}")
    else:
        print("OK: cada tarjeta cayo siempre en la misma particion (particionamiento estable por clave).")
    if any(order_violations.values()):
        print(f"ALERTA: se detectaron violaciones de orden dentro de una particion: {dict(order_violations)}")
    else:
        print("OK: no se detectaron violaciones de orden dentro de ninguna particion.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--topic", default="transactions.raw")
    return parser.parse_args()


if __name__ == "__main__":
    analyze(**vars(parse_args()))
