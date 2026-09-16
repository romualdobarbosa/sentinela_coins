"""Dashboard Streamlit.

- Aba "Análise": lê a camada GOLD (batch). Só SELECT, rápido.
- Aba "Ao vivo": consumer live lendo o tópico direto. Speed layer.

Rode com: make dashboard   (ou: PYTHONPATH=src streamlit run dashboard/app.py)
"""

import sys
from pathlib import Path

# deixa o `import config` / `import live` funcionarem
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))
sys.path.append(str(Path(__file__).resolve().parent))

import polars as pl  # noqa: E402
import streamlit as st  # noqa: E402
from live import make_live_consumer, poll_trades  # noqa: E402

from config import GOLD_PATH  # noqa: E402

st.set_page_config(page_title="Crypto Streaming Lakehouse", layout="wide")
st.title("Crypto Streaming Lakehouse")

tab_hist, tab_live = st.tabs(["Análise (Gold)", "Ao vivo"])

# ---------- Batch / analítico ----------
with tab_hist:
    gold_file = Path(GOLD_PATH) / "daily_metrics.parquet"
    if gold_file.exists():
        df = pl.read_parquet(gold_file)
        st.subheader("Métricas diárias por símbolo")
        st.dataframe(df.to_pandas(), width="stretch")

        latest = df.sort("dt").group_by("symbol").last().sort("symbol")
        st.subheader("Variação % — último dia disponível")
        st.bar_chart(
            latest.select(["symbol", "change_pct"]).to_pandas(), x="symbol", y="change_pct"
        )
    else:
        st.info("Camada gold ainda não existe. Rode: make silver && make gold")

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
        last = live.group_by("symbol").last().sort("symbol")
        cols = st.columns(len(last))
        for col, row in zip(cols, last.iter_rows(named=True), strict=True):
            col.metric(row["symbol"], f"{row['price']:.4f}")
        st.caption(f"{len(st.session_state.live_buffer)} trades na janela")
    else:
        st.info("Sem trades ainda — producer + Kafka estão de pé?")
