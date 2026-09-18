"""Consumer BATCH: lê o tópico e grava a camada BRONZE (parquet cru, particionado).

- group.id próprio -> offset independente do consumer live.
- Buffer em memória, faz flush por quantidade OU tempo.
- Commit manual só depois de gravar (at-least-once: não perde dado se cair).
- Particiona por symbol/dt -> casa com a leitura da camada silver.
"""

import json
import time
from pathlib import Path

import polars as pl
from confluent_kafka import Consumer

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    BATCH_MAX_MESSAGES,
    BATCH_MAX_SECONDS,
    BRONZE_PATH,
    KAFKA_BOOTSTRAP,
    TOPIC,
)


def make_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "bronze-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def flush(buffer: list[dict]) -> None:
    if not buffer:
        return
    df = pl.DataFrame(buffer).with_columns(
        pl.col("p").cast(pl.Float64).alias("price"),
        pl.col("q").cast(pl.Float64).alias("qty"),
        pl.from_epoch(pl.col("T"), time_unit="ms").alias("trade_time"),
    )
    df = df.with_columns(pl.col("trade_time").dt.date().alias("dt"))

    is_s3 = BRONZE_PATH.startswith("s3://")
    for (symbol, dt), part in df.group_by(["s", "dt"]):
        rel = f"symbol={symbol}/dt={dt}/{int(time.time() * 1000)}.parquet"
        if is_s3:
            # import local: s3fs só é preciso na fase AWS, não trava o resto se faltar.
            import s3fs

            fs = s3fs.S3FileSystem(
                key=AWS_ACCESS_KEY_ID,
                secret=AWS_SECRET_ACCESS_KEY,
                client_kwargs={"region_name": AWS_REGION},
            )
            with fs.open(f"{BRONZE_PATH.rstrip('/')}/{rel}", "wb") as f:
                part.write_parquet(f)
        else:
            out_dir = Path(BRONZE_PATH) / f"symbol={symbol}" / f"dt={dt}"
            out_dir.mkdir(parents=True, exist_ok=True)
            part.write_parquet(out_dir / f"{int(time.time() * 1000)}.parquet")

    print(f"[bronze] gravou {len(buffer)} trades")


def run() -> None:
    consumer = make_consumer()
    consumer.subscribe([TOPIC])
    buffer: list[dict] = []
    last_flush = time.time()
    print("[bronze] consumindo... (ctrl+c pra parar)")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    print(f"[bronze] erro no consumer: {msg.error()}")
                else:
                    buffer.append(json.loads(msg.value()))

            due = (
                len(buffer) >= BATCH_MAX_MESSAGES or (time.time() - last_flush) >= BATCH_MAX_SECONDS
            )
            if due and buffer:
                flush(buffer)
                consumer.commit(asynchronous=False)  # só commita depois de gravar
                buffer.clear()
                last_flush = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        flush(buffer)
        consumer.commit(asynchronous=False)
        consumer.close()
        print("\n[bronze] encerrado")


if __name__ == "__main__":
    run()
