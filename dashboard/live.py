"""Consumer LIVE: segundo consumer no MESMO tópico, com group.id próprio.

- auto.offset.reset=latest + group aleatório -> sempre lê o "agora", ignora histórico.
- Não grava nada, não commita. É o speed layer do dashboard.
"""

import json
import uuid

from confluent_kafka import Consumer

from config import KAFKA_BOOTSTRAP, TOPIC


def make_live_consumer() -> Consumer:
    c = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"dashboard-live-{uuid.uuid4()}",  # efêmero: só o agora
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([TOPIC])
    return c


def poll_trades(consumer: Consumer, max_msgs: int = 500, timeout: float = 0.3) -> list[dict]:
    out: list[dict] = []
    for _ in range(max_msgs):
        msg = consumer.poll(timeout)
        if msg is None:
            break
        if msg.error():
            print(f"[live] erro no consumer: {msg.error()}")
            continue
        out.append(json.loads(msg.value()))
    return out
