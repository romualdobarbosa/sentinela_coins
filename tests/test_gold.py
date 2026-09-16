from datetime import datetime

import gold
import polars as pl
import pytest


def test_daily_metrics_math(tmp_path, monkeypatch):
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    silver_dir.mkdir()

    pl.DataFrame(
        {
            "symbol": ["BTCUSDT"] * 3,
            "minute": [
                datetime(2024, 1, 1, 10, 0),
                datetime(2024, 1, 1, 10, 1),
                datetime(2024, 1, 1, 10, 2),
            ],
            "open": [100.0, 105.0, 108.0],
            "high": [106.0, 107.0, 110.0],
            "low": [99.0, 104.0, 107.0],
            "close": [105.0, 106.0, 109.0],
            "volume": [1.0, 2.0, 1.0],
            "quote_volume": [100.0, 210.0, 109.0],
            "trades": [3, 4, 2],
        }
    ).write_parquet(silver_dir / "candles_1m.parquet")

    monkeypatch.setattr(gold, "SILVER_PATH", str(silver_dir))
    monkeypatch.setattr(gold, "GOLD_PATH", str(gold_dir))

    gold.build()

    out = pl.read_parquet(gold_dir / "daily_metrics.parquet")
    assert out.height == 1
    row = out.row(0, named=True)

    assert row["symbol"] == "BTCUSDT"
    assert row["n_candles"] == 3
    assert row["volume"] == pytest.approx(4.0)
    assert row["quote_volume"] == pytest.approx(419.0)
    assert row["vwap"] == pytest.approx(419.0 / 4.0)
    assert row["day_open"] == pytest.approx(100.0)
    assert row["day_close"] == pytest.approx(109.0)
    assert row["change_pct"] == pytest.approx((109.0 - 100.0) / 100.0 * 100)
    assert row["range_pct"] == pytest.approx((110.0 - 99.0) / 99.0 * 100)
