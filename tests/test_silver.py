from datetime import UTC, datetime, timedelta

import polars as pl
import silver

import consumer_batch
import promote_bronze


def _write_bronze_trades(
    bronze_dir, symbol: str, prices_and_times: list[tuple[float, int]], monkeypatch
) -> None:
    # gera o bronze passando pelo fluxo real raw -> promote (em vez de escrever o parquet
    # à mão), pra garantir o mesmo schema de colunas (raw + price/qty/trade_time tipados)
    # que o pipeline real produz -- é essa costura entre bronze e silver que escondia a
    # colisão de nomes "t"/"T" que o DuckDB resolve sem diferenciar caixa.
    raw_dir = bronze_dir.parent / "raw"
    monkeypatch.setattr(consumer_batch, "RAW_PATH", str(raw_dir))
    monkeypatch.setattr(promote_bronze, "RAW_PATH", str(raw_dir))
    monkeypatch.setattr(promote_bronze, "BRONZE_PATH", str(bronze_dir))
    buffer = [
        {"s": symbol, "t": i, "p": str(price), "q": "1.0", "T": t}
        for i, (price, t) in enumerate(prices_and_times)
    ]
    consumer_batch.flush(buffer)
    promote_bronze.promote()


def test_forward_fill_produces_flat_candles_for_minutes_without_trades(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

    t0 = int(datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC).timestamp() * 1000)
    t1 = t0 + 3 * 60_000  # próximo trade real 3 minutos depois -> buraco de 2 minutos

    _write_bronze_trades(bronze_dir, "BTCUSDT", [(100.0, t0), (110.0, t1)], monkeypatch)

    monkeypatch.setattr(silver, "BRONZE_PATH", str(bronze_dir))
    monkeypatch.setattr(silver, "SILVER_PATH", str(silver_dir))

    silver.build()

    out = pl.read_parquet(silver_dir / "candles_1m.parquet").sort("minute")

    # sem buracos: 10:00, 10:01, 10:02, 10:03
    assert out.height == 4
    minutes = out["minute"].to_list()
    assert all(minutes[i + 1] - minutes[i] == timedelta(minutes=1) for i in range(len(minutes) - 1))

    filled = out.filter(pl.col("trades") == 0)
    assert filled.height == 2
    assert (filled["volume"] == 0).all()
    assert (filled["quote_volume"] == 0).all()
    assert (filled["open"] == filled["close"]).all()
    assert (filled["high"] == filled["low"]).all()
    # preço repetido é o último close real conhecido (candle de 10:00, close=100.0)
    assert filled["close"].to_list() == [100.0, 100.0]

    real = out.filter(pl.col("trades") > 0).sort("minute")
    assert real.height == 2
    assert real["close"].to_list() == [100.0, 110.0]


def test_no_forward_fill_needed_when_trades_are_contiguous(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

    t0 = int(datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC).timestamp() * 1000)
    t1 = t0 + 60_000

    _write_bronze_trades(bronze_dir, "ETHUSDT", [(3400.0, t0), (3410.0, t1)], monkeypatch)

    monkeypatch.setattr(silver, "BRONZE_PATH", str(bronze_dir))
    monkeypatch.setattr(silver, "SILVER_PATH", str(silver_dir))

    silver.build()

    out = pl.read_parquet(silver_dir / "candles_1m.parquet")
    assert out.height == 2
    assert (out["trades"] > 0).all()


def test_open_and_close_break_same_millisecond_ties_by_trade_id(tmp_path, monkeypatch):
    # Varredura de book: vários trades no mesmo milissegundo com preços diferentes. Com
    # arg_min(price, trade_time) o empate era resolvido pela ordem física das linhas, e o
    # open mudava conforme o layout do bronze (aconteceu em BNBUSDT, 22/09 02:37).
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

    t0 = int(datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC).timestamp() * 1000)
    # ids 0..3, todos no mesmo ms: o open certo é o do id 0, o close o do id 3
    _write_bronze_trades(
        bronze_dir, "BNBUSDT", [(791.46, t0), (791.8, t0), (792.0, t0), (792.18, t0)], monkeypatch
    )
    # reescreve o bronze com as linhas em ordem inversa, pra ordem física não ajudar
    (path,) = bronze_dir.rglob("*.parquet")
    pl.read_parquet(path).reverse().write_parquet(path)

    monkeypatch.setattr(silver, "BRONZE_PATH", str(bronze_dir))
    monkeypatch.setattr(silver, "SILVER_PATH", str(silver_dir))

    silver.build()

    out = pl.read_parquet(silver_dir / "candles_1m.parquet")
    assert out.select("open", "close").row(0) == (791.46, 792.18)
