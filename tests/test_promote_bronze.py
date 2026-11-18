import polars as pl

import consumer_batch
import promote_bronze


def test_promote_dedups_trade_replayed_after_crash(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    monkeypatch.setattr(consumer_batch, "RAW_PATH", str(raw_dir))
    monkeypatch.setattr(promote_bronze, "RAW_PATH", str(raw_dir))
    monkeypatch.setattr(promote_bronze, "BRONZE_PATH", str(bronze_dir))

    trade = {"s": "BTCUSDT", "t": 42, "p": "65000.0", "q": "0.01", "T": 1700000000000}
    # simula o crash entre flush e commit: o mesmo trade_id cai em dois arquivos raw.
    consumer_batch.flush([trade])
    consumer_batch.flush([trade])

    promote_bronze.promote()

    bronze_files = list(bronze_dir.glob("symbol=BTCUSDT/dt=*/data.parquet"))
    assert len(bronze_files) == 1
    df = pl.read_parquet(bronze_files[0])
    assert df.height == 1
    assert df["t"].to_list() == [42]


def test_promote_is_idempotent_on_rerun(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    monkeypatch.setattr(consumer_batch, "RAW_PATH", str(raw_dir))
    monkeypatch.setattr(promote_bronze, "RAW_PATH", str(raw_dir))
    monkeypatch.setattr(promote_bronze, "BRONZE_PATH", str(bronze_dir))

    consumer_batch.flush([{"s": "ETHUSDT", "t": 1, "p": "3400.0", "q": "0.5", "T": 1700000000000}])

    promote_bronze.promote()
    promote_bronze.promote()  # rerun sem raw novo -> sobrescreve com o mesmo resultado

    bronze_files = list(bronze_dir.glob("symbol=ETHUSDT/dt=*/data.parquet"))
    assert len(bronze_files) == 1
    assert pl.read_parquet(bronze_files[0]).height == 1


def test_promote_noop_on_empty_raw(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    raw_dir.mkdir()
    monkeypatch.setattr(promote_bronze, "RAW_PATH", str(raw_dir))
    monkeypatch.setattr(promote_bronze, "BRONZE_PATH", str(bronze_dir))

    promote_bronze.promote()

    assert not bronze_dir.exists()


def test_promote_writes_partition_sorted_by_trade_id(tmp_path, monkeypatch):
    # unique() do Polars não preserva ordem: sem ordenar, o parquet saía com as linhas
    # embaralhadas, comprimia 3x pior (t, E e trade_time deixam de ser quase sequenciais)
    # e duas promoções do mesmo raw davam arquivos com ordem diferente.
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    monkeypatch.setattr(consumer_batch, "RAW_PATH", str(raw_dir))
    monkeypatch.setattr(promote_bronze, "RAW_PATH", str(raw_dir))
    monkeypatch.setattr(promote_bronze, "BRONZE_PATH", str(bronze_dir))

    t0 = 1700000000000
    trades = [
        {"s": "BTCUSDT", "t": i, "p": "65000.0", "q": "0.01", "T": t0 + i} for i in range(5000)
    ]
    consumer_batch.flush(trades[2500:])
    consumer_batch.flush(trades[:2500])
    consumer_batch.flush(trades[1000:1100])  # replay no meio

    promote_bronze.promote()

    df = pl.read_parquet(next(bronze_dir.glob("symbol=BTCUSDT/dt=*/data.parquet")))
    assert df["t"].to_list() == list(range(5000))
