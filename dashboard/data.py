"""Carrega GOLD/SILVER pro dashboard.

Modo local (padrão): lê os paths reais de config.py, gerados pelo `make pipeline`.
Modo demo (DASHBOARD_MODE=demo): lê o snapshot committed em demo_data/, pra rodar
sem Kafka/pipeline no ar (ex: Streamlit Community Cloud).
"""

import os
from pathlib import Path

import polars as pl

from config import GOLD_PATH, SILVER_PATH

TIMEFRAMES = ["1m", "5m", "15m", "1h"]

DASHBOARD_MODE = os.getenv("DASHBOARD_MODE", "local")
DEMO_DIR = Path(__file__).resolve().parent / "demo_data"


def load_gold() -> pl.DataFrame | None:
    path = (
        DEMO_DIR / "gold" / "daily_metrics.parquet"
        if DASHBOARD_MODE == "demo"
        else Path(GOLD_PATH) / "daily_metrics.parquet"
    )
    return pl.read_parquet(path) if path.exists() else None


def load_silver() -> pl.DataFrame | None:
    path = (
        DEMO_DIR / "silver" / "candles_1m.parquet"
        if DASHBOARD_MODE == "demo"
        else Path(SILVER_PATH) / "candles_1m.parquet"
    )
    return pl.read_parquet(path) if path.exists() else None


def resample_candles(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
    df = df.sort("minute")
    if timeframe == "1m":
        return df
    return df.group_by_dynamic("minute", every=timeframe).agg(
        open=pl.col("open").first(),
        high=pl.col("high").max(),
        low=pl.col("low").min(),
        close=pl.col("close").last(),
        volume=pl.col("volume").sum(),
        quote_volume=pl.col("quote_volume").sum(),
        trades=pl.col("trades").sum(),
    )
