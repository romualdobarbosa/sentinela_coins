"""Producer: abre o WebSocket de trades da Binance e publica no tópico Kafka.

- Assina vários pares num combined stream só (1 conexão).
- Chaveia a mensagem por símbolo -> particionamento por símbolo no Kafka.
- Loop de reconexão: a Binance derruba a conexão a cada 24h (e pode cair antes).
"""

import asyncio
import json

import websockets
from confluent_kafka import Producer

from config import BINANCE_WS_BASE, KAFKA_BOOTSTRAP, SYMBOLS, TOPIC


def build_url() -> str:
    streams = "/".join(f"{s}@trade" for s in SYMBOLS)
    return f"{BINANCE_WS_BASE}?streams={streams}"


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[producer] falha na entrega: {err} (key={msg.key()})")


def make_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "linger.ms": 50,  # micro-batch pra throughput
            "compression.type": "lz4",
            "acks": "all",  # espera as réplicas (temos replication-factor 3)
        }
    )


async def run() -> None:
    producer = make_producer()
    url = build_url()
    print(f"[producer] assinando: {SYMBOLS}")

    while True:  # reconexão eterna
        try:
            # ping_interval/timeout: a lib responde os ping frames do servidor sozinha
            async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
                print(f"[producer] conectado -> {url}")
                async for raw in ws:
                    msg = json.loads(raw)
                    data = msg.get("data", msg)  # combined stream embrulha em "data"
                    symbol = data["s"]
                    producer.produce(
                        TOPIC,
                        key=symbol,
                        value=json.dumps(data).encode(),
                        callback=delivery_report,
                    )
                    producer.poll(0)  # dispara callbacks de entrega
        except Exception as e:  # noqa: BLE001 (skeleton: reconecta em qualquer erro)
            print(f"[producer] conexão caiu: {e} -> reconectando em 5s")
            producer.flush(5)
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n[producer] encerrado")
