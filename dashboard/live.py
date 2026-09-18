"""Consumer LIVE: segundo consumer no MESMO tópico, com group.id próprio.

- auto.offset.reset=latest + group aleatório -> sempre lê o "agora", ignora histórico.
- Não grava nada, não commita. É o speed layer do dashboard.

Modo demo (DASHBOARD_MODE=demo, ver dashboard/data.py) não usa Kafka: em vez disso
faz replay em loop de um snapshot real de trades (demo_data/bronze_sample.parquet),
só pra dar uma demonstração clicável sem precisar de Kafka/producer no ar.
"""

import json
import uuid
from pathlib import Path
from typing import Any

import polars as pl

from config import KAFKA_BOOTSTRAP, TOPIC

DEMO_TRADES_PATH = Path(__file__).resolve().parent / "demo_data" / "bronze_sample.parquet"


def make_live_consumer() -> Any:
    from confluent_kafka import Consumer  # import local: só a fase local precisa disso

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


def poll_trades(consumer: Any, max_msgs: int = 500, timeout: float = 0.3) -> list[dict]:
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


def load_demo_trades() -> pl.DataFrame:
    return pl.read_parquet(DEMO_TRADES_PATH)


def poll_trades_replay(df: pl.DataFrame, cursor: int, chunk: int = 180) -> tuple[list[dict], int]:
    """Devolve o próximo bloco do snapshot a partir de `cursor`, dando loop no fim."""
    n = df.height
    end = cursor + chunk
    if end <= n:
        rows = df[cursor:end].to_dicts()
    else:
        rows = df[cursor:n].to_dicts() + df[0 : end - n].to_dicts()
    return rows, end % n
