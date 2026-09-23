from datetime import datetime, timedelta

import polars as pl
from components import MIN_GAP_MINUTES, _drop_ingestion_gaps

T0 = datetime(2026, 9, 16, 4, 0)


def _candles(trades: list[int]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "minute": [T0 + timedelta(minutes=i) for i in range(len(trades))],
            "trades": trades,
        }
    )


def test_gap_longo_vira_rangebreak_e_candles_flat_saem():
    df = _candles([5, 5] + [0] * (MIN_GAP_MINUTES + 5) + [7, 7])
    out, breaks = _drop_ingestion_gaps(df)
    assert out["trades"].to_list() == [5, 5, 7, 7]
    assert len(breaks) == 1
    start, end = breaks[0]["bounds"]
    assert start == T0 + timedelta(minutes=2)
    assert end == T0 + timedelta(minutes=2 + MIN_GAP_MINUTES + 5)


def test_gap_curto_descarta_candle_mas_nao_gera_rangebreak():
    out, breaks = _drop_ingestion_gaps(_candles([5, 0, 0, 5]))
    assert out["trades"].to_list() == [5, 5]
    assert breaks == []


def test_sem_gap_nao_mexe_em_nada():
    df = _candles([3, 4, 5])
    out, breaks = _drop_ingestion_gaps(df)
    assert out.equals(df)
    assert breaks == []
