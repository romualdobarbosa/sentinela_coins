"""GOLD: métricas de negócio por símbolo/dia, prontas pro dashboard.

VWAP, volume, variação % do dia, amplitude. É o que o Streamlit consome (só SELECT).
"""

from pathlib import Path

import duckdb

from config import GOLD_PATH, SILVER_PATH


def build() -> None:
    Path(GOLD_PATH).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT
                symbol,
                CAST(minute AS DATE)                        AS dt,
                count(*)                                    AS n_candles,
                sum(volume)                                 AS volume,
                sum(quote_volume)                           AS quote_volume,
                sum(quote_volume) / NULLIF(sum(volume), 0)  AS vwap,
                (max(high) - min(low)) / NULLIF(min(low), 0) * 100 AS range_pct,
                arg_min(open, minute)                       AS day_open,
                arg_max(close, minute)                      AS day_close,
                (arg_max(close, minute) - arg_min(open, minute))
                    / NULLIF(arg_min(open, minute), 0) * 100 AS change_pct
            FROM read_parquet('{SILVER_PATH}/candles_1m.parquet')
            GROUP BY symbol, dt
            ORDER BY dt, symbol
        ) TO '{GOLD_PATH}/daily_metrics.parquet' (FORMAT PARQUET);
        """
    )
    print(f"[gold] {GOLD_PATH}/daily_metrics.parquet gerado")


if __name__ == "__main__":
    build()
