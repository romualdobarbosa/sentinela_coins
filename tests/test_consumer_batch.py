import polars as pl

import consumer_batch


def test_flush_partitions_by_symbol_and_date(tmp_path, monkeypatch):
    monkeypatch.setattr(consumer_batch, "BRONZE_PATH", str(tmp_path))

    buffer = [
        {"s": "BTCUSDT", "p": "65000.0", "q": "0.01", "T": 1700000000000},
        {"s": "BTCUSDT", "p": "65010.0", "q": "0.02", "T": 1700000005000},
        {"s": "ETHUSDT", "p": "3400.0", "q": "0.5", "T": 1700000000000},
    ]

    consumer_batch.flush(buffer)

    btc_files = list(tmp_path.glob("symbol=BTCUSDT/dt=*/*.parquet"))
    eth_files = list(tmp_path.glob("symbol=ETHUSDT/dt=*/*.parquet"))
    assert len(btc_files) == 1
    assert len(eth_files) == 1

    btc_df = pl.read_parquet(btc_files[0])
    assert btc_df.height == 2
    assert set(btc_df["price"].to_list()) == {65000.0, 65010.0}

    eth_df = pl.read_parquet(eth_files[0])
    assert eth_df.height == 1
    assert eth_df["qty"].to_list() == [0.5]


def test_flush_noop_on_empty_buffer(tmp_path, monkeypatch):
    monkeypatch.setattr(consumer_batch, "BRONZE_PATH", str(tmp_path))

    consumer_batch.flush([])

    assert list(tmp_path.iterdir()) == []
