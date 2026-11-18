from datetime import date, datetime, timedelta

import polars as pl
import pytest

import quality

T0 = datetime(2024, 1, 1, 10, 0)


def _candle(minute: int, o=100.0, h=101.0, low=99.0, c=100.5, trades=10, symbol="BTCUSDT"):
    return {
        "symbol": symbol,
        "minute": T0 + timedelta(minutes=minute),
        "open": o,
        "high": h,
        "low": low,
        "close": c,
        "volume": 1.0 if trades else 0.0,
        "quote_volume": 100.0 if trades else 0.0,
        "trades": trades,
    }


def _gold(n_candles=1440, vwap=100.0, dt=date(2024, 1, 1)):
    return {"symbol": "BTCUSDT", "dt": dt, "n_candles": n_candles, "vwap": vwap}


def _write(tmp_path, silver_rows, gold_rows):
    (tmp_path / "silver").mkdir()
    (tmp_path / "gold").mkdir()
    pl.DataFrame(silver_rows).write_parquet(tmp_path / "silver" / "candles_1m.parquet")
    pl.DataFrame(gold_rows).write_parquet(tmp_path / "gold" / "daily_metrics.parquet")
    return str(tmp_path / "silver"), str(tmp_path / "gold")


def _violations(results):
    return {r.name: r.violations for r in results if r.violations}


def test_clean_data_passes_every_check(tmp_path):
    silver, gold = _write(tmp_path, [_candle(i) for i in range(3)], [_gold()])
    assert _violations(quality.run_checks(silver, gold)) == {}


@pytest.mark.parametrize(
    ("bad_candle", "rule"),
    [
        (_candle(1, low=-1.0), "preco_positivo"),
        (_candle(1, h=98.0, low=99.0, o=98.5, c=98.5), "low_menor_que_high"),
        (_candle(1, o=105.0), "open_dentro_do_range"),
        (_candle(1, c=90.0), "close_dentro_do_range"),
    ],
)
def test_each_silver_rule_catches_its_violation(tmp_path, bad_candle, rule):
    silver, gold = _write(tmp_path, [_candle(0), bad_candle], [_gold()])
    assert rule in _violations(quality.run_checks(silver, gold))


def test_duplicate_keys_are_caught_in_silver_and_gold(tmp_path):
    silver, gold = _write(tmp_path, [_candle(0), _candle(0)], [_gold(), _gold()])
    assert _violations(quality.run_checks(silver, gold)) == {
        "chave_unica(symbol, minute)": 1,
        "chave_unica(symbol, dt)": 1,
    }


def test_gold_rules(tmp_path):
    silver, gold = _write(
        tmp_path, [_candle(0)], [_gold(n_candles=1441), _gold(vwap=0.0, dt=date(2024, 1, 2))]
    )
    assert _violations(quality.run_checks(silver, gold)) == {
        "vwap_positivo": 1,
        "no_maximo_1440_candles_por_dia": 1,
    }


def test_find_gaps_reports_only_runs_at_or_above_threshold(tmp_path):
    # minuto 0 real, 1-2 vazios (curto demais), 3 real, 4-9 vazios (6 min = gap), 10 real
    rows = [_candle(0), _candle(3), _candle(10)]
    rows += [_candle(m, 100.5, 100.5, 100.5, 100.5, trades=0) for m in (1, 2, 4, 5, 6, 7, 8, 9)]
    silver, _ = _write(tmp_path, rows, [_gold()])

    gaps = quality.find_gaps(silver, min_minutes=5)

    assert len(gaps) == 1
    assert (gaps[0].start, gaps[0].end, gaps[0].minutes) == (
        T0 + timedelta(minutes=4),
        T0 + timedelta(minutes=9),
        6,
    )


def test_find_gaps_separates_symbols(tmp_path):
    rows = [_candle(0, symbol="BTCUSDT"), _candle(0, symbol="ETHUSDT")]
    rows += [_candle(m, trades=0, symbol="ETHUSDT") for m in range(1, 6)]
    silver, _ = _write(tmp_path, rows, [_gold()])

    gaps = quality.find_gaps(silver, min_minutes=5)

    assert [(g.symbol, g.minutes) for g in gaps] == [("ETHUSDT", 5)]
