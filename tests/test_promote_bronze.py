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
