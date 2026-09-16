"""Dashboard Streamlit.

- Aba "Visão Geral": KPI cards por símbolo (GOLD), escolhe o símbolo em foco.
- Aba "Análise": lê GOLD + SILVER (batch). Candlestick real, timeframe ajustável.
- Aba "Ao vivo": consumer live lendo o tópico direto. Speed layer.

Rode com: make dashboard   (ou: PYTHONPATH=src streamlit run dashboard/app.py)
"""

import sys
from pathlib import Path

# deixa o `import config` / `import live` / `import data` funcionarem
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))
sys.path.append(str(Path(__file__).resolve().parent))

import components  # noqa: E402
import polars as pl  # noqa: E402
import streamlit as st  # noqa: E402
from live import make_live_consumer, poll_trades  # noqa: E402
from theme import format_price, inject_css  # noqa: E402

import data  # noqa: E402

st.set_page_config(page_title="Crypto Streaming Lakehouse", layout="wide")
inject_css()
st.title("Crypto Streaming Lakehouse")

tab_overview, tab_analysis, tab_live = st.tabs(["Visão Geral", "Análise", "Ao vivo"])

# ---------- Visão geral ----------
with tab_overview:
    gold_df = data.load_gold()
    if gold_df is None or gold_df.is_empty():
        st.info("Camada gold ainda não existe. Rode: make pipeline")
    else:
        latest = gold_df.sort("dt").group_by("symbol").last().sort("symbol")
        symbols = latest["symbol"].to_list()

        cols = st.columns(len(symbols))
        for col, row in zip(cols, latest.iter_rows(named=True), strict=True):
            components.kpi_card(
                col, row["symbol"], row["day_close"], row["change_pct"], row["volume"]
            )

        default = st.session_state.get("selected_symbol", symbols[0])
        st.session_state["selected_symbol"] = st.selectbox(
            "Símbolo em foco (usado na aba Análise)",
            symbols,
            index=symbols.index(default) if default in symbols else 0,
        )

# ---------- Batch / analítico ----------
with tab_analysis:
    silver_df = data.load_silver()
    gold_df = data.load_gold()

    if silver_df is None or silver_df.is_empty():
        st.info("Camada silver ainda não existe. Rode: make pipeline")
    else:
        symbols = sorted(silver_df["symbol"].unique().to_list())
        default = st.session_state.get("selected_symbol", symbols[0])
        symbol = st.selectbox(
            "Símbolo", symbols, index=symbols.index(default) if default in symbols else 0
        )
        timeframe = st.select_slider("Timeframe", options=data.TIMEFRAMES, value="1m")

        candles = data.resample_candles(silver_df.filter(pl.col("symbol") == symbol), timeframe)

        if gold_df is not None:
            symbol_gold = gold_df.filter(pl.col("symbol") == symbol).sort("dt")
            if not symbol_gold.is_empty():
                last = symbol_gold.row(-1, named=True)
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("VWAP", format_price(last["vwap"]))
                k2.metric("Amplitude", f"{last['range_pct']:.2f}%")
                k3.metric(
                    "Abertura → Fechamento",
                    f"{format_price(last['day_open'])} → {format_price(last['day_close'])}",
                )
                k4.metric("Variação do dia", f"{last['change_pct']:+.2f}%")

        st.plotly_chart(components.price_chart(candles, symbol), width="stretch")

        st.subheader("Histórico diário (gold)")
        if gold_df is not None:
            st.dataframe(
                gold_df.filter(pl.col("symbol") == symbol).sort("dt").to_pandas(), width="stretch"
            )

# ---------- Live / speed layer ----------
with tab_live:
    from streamlit_autorefresh import st_autorefresh

    st_autorefresh(interval=2000, key="live_refresh")

    if "live_consumer" not in st.session_state:
        st.session_state.live_consumer = make_live_consumer()
        st.session_state.live_buffer = []

    trades = poll_trades(st.session_state.live_consumer)
    if trades:
        rows = [
            {"symbol": t["s"], "price": float(t["p"]), "qty": float(t["q"]), "T": t["T"]}
            for t in trades
        ]
        st.session_state.live_buffer = (st.session_state.live_buffer + rows)[-2000:]

    if st.session_state.live_buffer:
        live = pl.DataFrame(st.session_state.live_buffer)
        symbols = sorted(live["symbol"].unique().to_list())
        cols = st.columns(len(symbols))
        for col, symbol in zip(cols, symbols, strict=True):
            prices = live.filter(pl.col("symbol") == symbol).sort("T")["price"].to_list()
            with col:
                st.metric(symbol, format_price(prices[-1]))
                st.plotly_chart(
                    components.sparkline(prices[-100:]),
                    width="stretch",
                    config={"displayModeBar": False},
                )
        st.caption(f"{len(st.session_state.live_buffer)} trades na janela")
    else:
        st.info("Sem trades ainda — producer + Kafka estão de pé?")
