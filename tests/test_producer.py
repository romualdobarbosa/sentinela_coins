import producer


def test_build_url_combines_all_symbols_in_one_stream(monkeypatch):
    monkeypatch.setattr(producer, "SYMBOLS", ["btcusdt", "ethusdt", "solusdt"])
    monkeypatch.setattr(producer, "BINANCE_WS_BASE", "wss://example.test/stream")

    url = producer.build_url()

    assert url == "wss://example.test/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade"


def test_build_url_single_symbol(monkeypatch):
    monkeypatch.setattr(producer, "SYMBOLS", ["btcusdt"])
    monkeypatch.setattr(producer, "BINANCE_WS_BASE", "wss://example.test/stream")

    url = producer.build_url()

    assert url == "wss://example.test/stream?streams=btcusdt@trade"
