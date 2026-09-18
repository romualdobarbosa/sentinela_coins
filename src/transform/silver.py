"""SILVER: lê o bronze cru e monta candles OHLCV de 1 minuto por símbolo.

DuckDB lê os parquet do bronze direto (glob recursivo). Tipagem + agregação aqui.
Na fase AWS, troca BRONZE_PATH por s3://... no .env -- build() detecta o prefixo e
carrega httpfs + credenciais automaticamente, resto da query não muda.
"""

from pathlib import Path

import duckdb

from config import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY, BRONZE_PATH, SILVER_PATH


def build() -> None:
    Path(SILVER_PATH).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    if BRONZE_PATH.startswith("s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(
            "CREATE OR REPLACE SECRET aws_s3 (TYPE S3, KEY_ID ?, SECRET ?, REGION ?)",
            [AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION],
        )

    con.execute(
        f"""
        COPY (
            -- lê price/qty/trade_time já tipados pelo consumer_batch (Polars), em vez de
            -- recalcular de p/q/T crus: o parquet do bronze tem colunas "t" (trade id) e
            -- "T" (trade time) que só diferem em maiúscula, e o DuckDB resolve nomes de
            -- coluna sem diferenciar caixa -- SELECT T colidiria silenciosamente com "t".
            WITH trades AS (
                SELECT
                    s AS symbol,
                    price,
                    qty,
                    trade_time
                FROM read_parquet('{BRONZE_PATH}/**/*.parquet')
            ),
            real_candles AS (
                SELECT
                    symbol,
                    time_bucket(INTERVAL '1 minute', trade_time) AS minute,
                    arg_min(price, trade_time)  AS open,
                    max(price)                  AS high,
                    min(price)                  AS low,
                    arg_max(price, trade_time)  AS close,
                    sum(qty)                    AS volume,
                    sum(price * qty)            AS quote_volume,
                    count(*)                    AS trades
                FROM trades
                GROUP BY symbol, minute
            ),
            -- minutos sem nenhum trade não geram linha em real_candles: preenchemos
            -- esses buracos com um candle "flat" (preço repete, volume/trades zeram),
            -- igual a Binance faz no próprio stream de kline.
            bounds AS (
                SELECT symbol, min(minute) AS min_minute, max(minute) AS max_minute
                FROM real_candles
                GROUP BY symbol
            ),
            minute_grid AS (
                SELECT
                    symbol,
                    unnest(generate_series(min_minute, max_minute, INTERVAL '1 minute')) AS minute
                FROM bounds
            ),
            joined AS (
                SELECT
                    g.symbol,
                    g.minute,
                    r.close IS NULL              AS is_filled,
                    r.open, r.high, r.low, r.close,
                    COALESCE(r.volume, 0)        AS volume,
                    COALESCE(r.quote_volume, 0)  AS quote_volume,
                    COALESCE(r.trades, 0)        AS trades
                FROM minute_grid g
                LEFT JOIN real_candles r USING (symbol, minute)
            ),
            filled AS (
                SELECT
                    symbol, minute, is_filled, open, high, low, close, volume, quote_volume, trades,
                    LAST_VALUE(close IGNORE NULLS) OVER (
                        PARTITION BY symbol ORDER BY minute
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS last_close
                FROM joined
            )
            SELECT
                symbol,
                minute,
                CASE WHEN is_filled THEN last_close ELSE open  END AS open,
                CASE WHEN is_filled THEN last_close ELSE high  END AS high,
                CASE WHEN is_filled THEN last_close ELSE low   END AS low,
                CASE WHEN is_filled THEN last_close ELSE close END AS close,
                volume,
                quote_volume,
                trades
            FROM filled
            ORDER BY symbol, minute
        ) TO '{SILVER_PATH}/candles_1m.parquet' (FORMAT PARQUET);
        """
    )
    print(f"[silver] {SILVER_PATH}/candles_1m.parquet gerado")


if __name__ == "__main__":
    build()
