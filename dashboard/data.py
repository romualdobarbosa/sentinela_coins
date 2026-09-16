"""Carrega GOLD/SILVER reais pro dashboard. Sem mock — lê os paths de config.py."""

from pathlib import Path

import polars as pl

from config import GOLD_PATH, SILVER_PATH

TIMEFRAMES = ["1m", "5m", "15m", "1h"]


def load_gold() -> pl.DataFrame | None:
    path = Path(GOLD_PATH) / "daily_metrics.parquet"
    return pl.read_parquet(path) if path.exists() else None


def load_silver() -> pl.DataFrame | None:
    path = Path(SILVER_PATH) / "candles_1m.parquet"
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
